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
// 这是 Actor 模型中消息传递的基本单元
// Msg: 实际消息内容，可以是任意类型
// Sender: 发送者的 PID，用于回复消息或追踪消息来源
type Envelope struct {
	Msg    any
	Sender *PID
}

// Processer 定义了进程的核心行为接口
// 这是 Actor 模型中进程的抽象，定义了进程的生命周期和消息处理能力
type Processer interface {
	Start()               // 启动进程，开始处理消息
	PID() *PID            // 获取进程的唯一标识符
	Send(*PID, any, *PID) // 发送消息到指定进程
	Invoke([]Envelope)    // 批量处理消息队列
	Shutdown()            // 关闭进程，释放资源
}

// process 是 Actor 进程的具体实现
// 包含了进程的完整状态和行为
type process struct {
	Opts // 嵌入进程配置选项

	inbox    Inboxer    // 消息收件箱，负责消息的接收和缓冲
	context  *Context   // Actor 上下文，包含进程运行时信息
	pid      *PID       // 进程唯一标识符
	restarts int32      // 当前重启次数，用于重启策略控制
	mbuffer  []Envelope // 消息缓冲区，存储崩溃时未处理的消息
}

// newProcess 创建一个新的 process 实例
// e: Actor 引擎实例
// opts: 进程配置选项
// 返回: 初始化完成的进程实例
func newProcess(e *Engine, opts Opts) *process {
	// 构造进程 PID：地址 + 类型 + ID
	pid := NewPID(e.address, opts.Kind+pidSeparator+opts.ID)
	// 创建进程上下文，包含引擎引用和 PID
	ctx := newContext(opts.Context, e, pid)
	p := &process{
		pid:     pid,
		inbox:   NewInbox(opts.InboxSize), // 创建指定大小的收件箱
		Opts:    opts,
		context: ctx,
		mbuffer: nil, // 初始时无缓冲消息
	}
	return p
}

// applyMiddleware 应用中间件链到消息接收函数
// 中间件按照洋葱模型执行，最后添加的中间件最先执行
// rcv: 原始的消息接收函数
// middleware: 中间件函数列表
// 返回: 包装后的消息接收函数
func applyMiddleware(rcv ReceiveFunc, middleware ...MiddlewareFunc) ReceiveFunc {
	// 从后往前应用中间件，形成调用链
	for i := len(middleware) - 1; i >= 0; i-- {
		rcv = middleware[i](rcv)
	}
	return rcv
}

// Invoke 批量处理消息队列
// 这是 Actor 消息处理的核心方法，支持崩溃恢复和优雅关闭
// msgs: 待处理的消息列表
func (p *process) Invoke(msgs []Envelope) {
	var (
		nmsg      = len(msgs) // 总消息数量
		nproc     = 0         // 已处理消息数量（用于崩溃恢复）
		processed = 0         // 成功处理的消息数量
	)

	// 使用 defer + recover 实现崩溃恢复机制
	defer func() {
		if v := recover(); v != nil {
			// 发送停止消息给 receiver
			p.context.message = Stopped{}
			p.context.receiver.Receive(p.context)

			// 将未处理的消息缓存起来，重启后重新处理
			// 这确保了消息不会因为崩溃而丢失
			p.mbuffer = make([]Envelope, nmsg-nproc)
			for i := 0; i < nmsg-nproc; i++ {
				p.mbuffer[i] = msgs[i+nproc]
			}
			p.tryRestart(v) // 尝试重启进程
		}
	}()

	// 逐个处理消息
	for i := 0; i < len(msgs); i++ {
		nproc++ // 更新处理计数器
		msg := msgs[i]

		// 检查是否为毒丸消息（关闭信号）
		if pill, ok := msg.Msg.(poisonPill); ok {
			if pill.graceful {
				// 优雅关闭：处理完剩余的所有消息再关闭
				msgsToProcess := msgs[processed:]
				for _, m := range msgsToProcess {
					p.invokeMsg(m)
				}
			}
			// 执行清理并退出
			p.cleanup(pill.cancel)
			return
		}

		p.invokeMsg(msg) // 处理普通消息
		processed++      // 更新成功处理计数器
	}
}

// TODO:ready to read record

// invokeMsg 处理单条消息
// 设置上下文并通过中间件链调用 receiver
// msg: 要处理的消息封装
func (p *process) invokeMsg(msg Envelope) {
	// 过滤毒丸消息，这些是引擎内部使用的私有消息
	if _, ok := msg.Msg.(poisonPill); ok {
		return
	}

	// 设置当前消息上下文
	p.context.message = msg.Msg
	p.context.sender = msg.Sender
	recv := p.context.receiver

	// 应用中间件链处理消息
	if len(p.Opts.Middleware) > 0 {
		applyMiddleware(recv.Receive, p.Opts.Middleware...)(p.context)
	} else {
		recv.Receive(p.context)
	}
}

// Start 启动进程
// 这是 Actor 生命周期的开始，包括初始化、启动事件和消息处理
func (p *process) Start() {
	// 通过 Producer 创建 receiver 实例
	recv := p.Producer()
	p.context.receiver = recv

	// 崩溃恢复机制
	defer func() {
		if v := recover(); v != nil {
			p.context.message = Stopped{}
			p.context.receiver.Receive(p.context)
			p.tryRestart(v)
		}
	}()

	// 发送初始化事件
	p.context.message = Initialized{}
	applyMiddleware(recv.Receive, p.Opts.Middleware...)(p.context)
	p.context.engine.BroadcastEvent(ActorInitializedEvent{PID: p.pid, Timestamp: time.Now()})

	// 发送启动事件
	p.context.message = Started{}
	applyMiddleware(recv.Receive, p.Opts.Middleware...)(p.context)
	p.context.engine.BroadcastEvent(ActorStartedEvent{PID: p.pid, Timestamp: time.Now()})

	// 如果有缓冲的消息（来自之前的崩溃），先处理它们
	if len(p.mbuffer) > 0 {
		p.Invoke(p.mbuffer)
		p.mbuffer = nil // 清空缓冲区
	}

	// 启动收件箱，开始接收和处理新消息
	p.inbox.Start(p)
}

// tryRestart 处理进程崩溃后的重启逻辑
// 实现了指数退避和最大重启次数限制的重启策略
// v: 崩溃时的 panic 值
func (p *process) tryRestart(v any) {
	// InternalError 是特殊情况，不计入重启次数限制
	// 主要用于远程节点连接失败等场景，需要持续重试
	if msg, ok := v.(*InternalError); ok {
		slog.Error(msg.From, "err", msg.Err)
		time.Sleep(p.Opts.RestartDelay) // 延迟后重启
		p.Start()
		return
	}

	// 获取并清理堆栈信息，用于调试
	stackTrace := cleanTrace(debug.Stack())

	// TODO:ready to read record

	// 检查是否达到最大重启次数
	if p.restarts == p.MaxRestarts {
		// 广播最大重启次数超限事件
		p.context.engine.BroadcastEvent(ActorMaxRestartsExceededEvent{
			PID:       p.pid,
			Timestamp: time.Now(),
		})
		p.cleanup(nil) // 彻底清理，不再重启
		return
	}

	p.restarts++ // 增加重启计数器

	// 广播重启事件，包含详细的崩溃信息
	p.context.engine.BroadcastEvent(ActorRestartedEvent{
		PID:        p.pid,
		Timestamp:  time.Now(),
		Stacktrace: stackTrace,
		Reason:     v,
		Restarts:   p.restarts,
	})

	// 延迟后重启，实现退避策略
	time.Sleep(p.Opts.RestartDelay)
	p.Start()
}

// TODO:ready to read record

// cleanup 清理进程资源
// 实现了完整的资源清理流程，包括子进程管理和注册表清理
// cancel: 上下文取消函数，用于通知相关 goroutine 停止
func (p *process) cleanup(cancel context.CancelFunc) {
	defer cancel() // 确保取消上下文

	// 从父进程的子进程列表中移除自己
	if p.context.parentCtx != nil {
		p.context.parentCtx.children.Delete(p.pid.ID)
	}

	// 递归关闭所有子进程
	// 这确保了进程树的正确清理，避免孤儿进程
	if p.context.children.Len() > 0 {
		children := p.context.Children()
		for _, pid := range children {
			// 等待每个子进程完全关闭
			<-p.context.engine.Poison(pid).Done()
		}
	}

	// 停止收件箱，不再接收新消息
	p.inbox.Stop()
	// 从引擎注册表中移除进程
	p.context.engine.Registry.Remove(p.pid)

	// 发送停止消息给 receiver，允许其执行清理逻辑
	p.context.message = Stopped{}
	applyMiddleware(p.context.receiver.Receive, p.Opts.Middleware...)(p.context)

	// 广播进程停止事件
	p.context.engine.BroadcastEvent(ActorStoppedEvent{PID: p.pid, Timestamp: time.Now()})
}

// PID 返回进程的 PID
func (p *process) PID() *PID { return p.pid }

// Send 向进程收件箱发送消息
// 这是外部与进程通信的入口点
// _: 目标 PID（在这个实现中未使用，因为消息直接发送到当前进程）
// msg: 要发送的消息内容
// sender: 发送者的 PID
func (p *process) Send(_ *PID, msg any, sender *PID) {
	p.inbox.Send(Envelope{Msg: msg, Sender: sender})
}

// Shutdown 关闭进程
// 这是进程的优雅关闭入口，释放所有资源
func (p *process) Shutdown() {
	p.cleanup(nil)
}

// cleanTrace 清理 goroutine 堆栈信息
// 移除不必要的栈帧，使错误日志更加清晰易读
// stack: 原始堆栈信息
// 返回: 清理后的堆栈信息
func cleanTrace(stack []byte) []byte {
	// 解析 goroutine 堆栈
	goros, err := gostackparse.Parse(bytes.NewReader(stack))
	if err != nil {
		slog.Error("failed to parse stacktrace", "err", err)
		return stack // 解析失败时返回原始堆栈
	}
	if len(goros) != 1 {
		slog.Error("expected only one goroutine", "goroutines", len(goros))
		return stack // 异常情况，返回原始堆栈
	}

	// 跳过前 4 个栈帧（通常是 runtime 相关的内部调用）
	goros[0].Stack = goros[0].Stack[4:]

	// 重新格式化堆栈信息
	buf := bytes.NewBuffer(nil)
	_, _ = fmt.Fprintf(buf, "goroutine %d [%s]\n", goros[0].ID, goros[0].State)
	for _, frame := range goros[0].Stack {
		_, _ = fmt.Fprintf(buf, "%s\n", frame.Func)
		_, _ = fmt.Fprint(buf, "\t", frame.File, ":", frame.Line, "\n")
	}
	return buf.Bytes()
}
