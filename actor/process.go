package actor

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/DataDog/gostackparse"
)

// Envelope 封装消息和发送者 PID
// Msg: 实际消息内容
// Sender: 发送者 PID
type Envelope struct {
	Msg    any
	Sender *PID
}

// Processer is an interface the abstracts the way a process behaves.
// Processer 抽象了进程的行为接口
// Start: 启动进程
// PID: 获取进程 PID
// Send: 发送消息
// Invoke: 批量处理消息
// Shutdown: 关闭进程
type Processer interface {
	Start()
	PID() *PID
	Send(*PID, any, *PID)
	Invoke([]Envelope)
	Shutdown()
}

// process 代表一个 actor 进程实例
// inbox: 消息收件箱
// context: actor 上下文
// pid: 进程唯一标识
// restarts: 重启次数
// mbuffer: 崩溃时未处理的消息缓存
type process struct {
	Opts

	inbox    Inboxer
	context  *Context
	pid      *PID
	restarts int32
	mbuffer  []Envelope
}

// newProcess 创建一个新的 process 实例
func newProcess(e *Engine, opts Opts) *process {
	pid := NewPID(e.address, opts.Kind+pidSeparator+opts.ID)
	ctx := newContext(opts.Context, e, pid)
	p := &process{
		pid:     pid,
		inbox:   NewInbox(opts.InboxSize),
		Opts:    opts,
		context: ctx,
		mbuffer: nil,
	}
	return p
}

// applyMiddleware 应用中间件链到消息接收函数
func applyMiddleware(rcv ReceiveFunc, middleware ...MiddlewareFunc) ReceiveFunc {
	for i := len(middleware) - 1; i >= 0; i-- {
		rcv = middleware[i](rcv)
	}
	return rcv
}

// Invoke 批量处理消息队列，支持崩溃恢复与优雅关闭
func (p *process) Invoke(msgs []Envelope) {
	var (
		// numbers of msgs that need to be processed.
		nmsg = len(msgs)
		// numbers of msgs that are processed.
		nproc = 0
		// FIXME: We could use nrpoc here, but for some reason placing nproc++ on the
		// bottom of the function it freezes some tests. Hence, I created a new counter
		// for bookkeeping.
		processed = 0
	)
	defer func() {
		// If we recovered, we buffer up all the messages that we could not process
		// so we can retry them on the next restart.
		if v := recover(); v != nil {
			p.context.message = Stopped{}
			p.context.receiver.Receive(p.context)

			// 崩溃时将未处理的消息缓存，重启后重试
			p.mbuffer = make([]Envelope, nmsg-nproc)
			for i := 0; i < nmsg-nproc; i++ {
				p.mbuffer[i] = msgs[i+nproc]
			}
			p.tryRestart(v)
		}
	}()

	for i := 0; i < len(msgs); i++ {
		nproc++
		msg := msgs[i]
		if pill, ok := msg.Msg.(poisonPill); ok {
			// If we need to gracefuly stop, we process all the messages
			// from the inbox, otherwise we ignore and cleanup.
			if pill.graceful {
				// 优雅关闭时，处理剩余所有消息
				msgsToProcess := msgs[processed:]
				for _, m := range msgsToProcess {
					p.invokeMsg(m)
				}
			}
			p.cleanup(pill.cancel)
			return
		}
		p.invokeMsg(msg)
		processed++
	}
}

// invokeMsg 处理单条消息，支持中间件
func (p *process) invokeMsg(msg Envelope) {
	// suppress poison pill messages here. they're private to the actor engine.
	if _, ok := msg.Msg.(poisonPill); ok {
		return
	}
	p.context.message = msg.Msg
	p.context.sender = msg.Sender
	recv := p.context.receiver
	if len(p.Opts.Middleware) > 0 {
		applyMiddleware(recv.Receive, p.Opts.Middleware...)(p.context)
	} else {
		recv.Receive(p.context)
	}
}

// Start 启动进程，初始化 receiver 并处理初始化/启动事件
func (p *process) Start() {
	recv := p.Producer()
	p.context.receiver = recv
	defer func() {
		if v := recover(); v != nil {
			p.context.message = Stopped{}
			p.context.receiver.Receive(p.context)
			p.tryRestart(v)
		}
	}()
	p.context.message = Initialized{}
	applyMiddleware(recv.Receive, p.Opts.Middleware...)(p.context)
	p.context.engine.BroadcastEvent(ActorInitializedEvent{PID: p.pid, Timestamp: time.Now()})

	p.context.message = Started{}
	applyMiddleware(recv.Receive, p.Opts.Middleware...)(p.context)
	p.context.engine.BroadcastEvent(ActorStartedEvent{PID: p.pid, Timestamp: time.Now()})
	// If we have messages in our buffer, invoke them.
	if len(p.mbuffer) > 0 {
		p.Invoke(p.mbuffer)
		p.mbuffer = nil
	}

	p.inbox.Start(p)
}

// tryRestart 处理进程崩溃后的重启逻辑，支持最大重启次数与延迟
func (p *process) tryRestart(v any) {
	// InternalError does not take the maximum restarts into account.
	// For now, InternalError is getting triggered when we are dialing
	// a remote node. By doing this, we can keep dialing until it comes
	// back up. NOTE: not sure if that is the best option. What if that
	// node never comes back up again?
	if msg, ok := v.(*InternalError); ok {
		slog.Error(msg.From, "err", msg.Err)
		time.Sleep(p.Opts.RestartDelay)
		p.Start()
		return
	}
	stackTrace := cleanTrace(debug.Stack())
	// If we reach the max restarts, we shutdown the inbox and clean
	// everything up.
	if p.restarts == p.MaxRestarts {
		p.context.engine.BroadcastEvent(ActorMaxRestartsExceededEvent{
			PID:       p.pid,
			Timestamp: time.Now(),
		})
		p.cleanup(nil)
		return
	}

	p.restarts++
	// Restart the process after its restartDelay
	p.context.engine.BroadcastEvent(ActorRestartedEvent{
		PID:        p.pid,
		Timestamp:  time.Now(),
		Stacktrace: stackTrace,
		Reason:     v,
		Restarts:   p.restarts,
	})
	time.Sleep(p.Opts.RestartDelay)
	p.Start()
}

// cleanup 清理进程资源，递归关闭所有子进程，移除注册表
func (p *process) cleanup(cancel context.CancelFunc) {
	defer cancel()

	if p.context.parentCtx != nil {
		p.context.parentCtx.children.Delete(p.pid.ID)
	}

	if p.context.children.Len() > 0 {
		children := p.context.Children()
		for _, pid := range children {
			<-p.context.engine.Poison(pid).Done()
		}
	}

	p.inbox.Stop()
	p.context.engine.Registry.Remove(p.pid)
	p.context.message = Stopped{}
	applyMiddleware(p.context.receiver.Receive, p.Opts.Middleware...)(p.context)

	p.context.engine.BroadcastEvent(ActorStoppedEvent{PID: p.pid, Timestamp: time.Now()})
}

// PID 返回进程的 PID
func (p *process) PID() *PID { return p.pid }

// Send 向进程收件箱发送消息
func (p *process) Send(_ *PID, msg any, sender *PID) {
	p.inbox.Send(Envelope{Msg: msg, Sender: sender})
}

// Shutdown 关闭进程，释放资源
func (p *process) Shutdown() {
	p.cleanup(nil)
}

// cleanTrace 清理 goroutine 堆栈信息，便于日志分析
func cleanTrace(stack []byte) []byte {
	goros, err := gostackparse.Parse(bytes.NewReader(stack))
	if err != nil {
		slog.Error("failed to parse stacktrace", "err", err)
		return stack
	}
	if len(goros) != 1 {
		slog.Error("expected only one goroutine", "goroutines", len(goros))
		return stack
	}
	// skip the first frames:
	goros[0].Stack = goros[0].Stack[4:]
	buf := bytes.NewBuffer(nil)
	_, _ = fmt.Fprintf(buf, "goroutine %d [%s]\n", goros[0].ID, goros[0].State)
	for _, frame := range goros[0].Stack {
		_, _ = fmt.Fprintf(buf, "%s\n", frame.Func)
		_, _ = fmt.Fprint(buf, "\t", frame.File, ":", frame.Line, "\n")
	}
	return buf.Bytes()
}
