package actor

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// Remoter 接口用于抽象与 Engine 绑定的远程通信能力
// Address: 返回远程地址
// Send: 发送消息
// Start/Stop: 启动和停止远程服务
type Remoter interface {
	Address() string
	Send(*PID, any, *PID)
	Start(*Engine) error
	Stop() *sync.WaitGroup
}

// Producer 是用于生成 Receiver 的工厂方法类型
type Producer func() Receiver

// Receiver 接口定义了消息接收与处理能力
// Receive: 处理消息的入口
type Receiver interface {
	Receive(*Context)
}

// Engine 代表 actor 系统的引擎，负责进程管理、消息分发等
// Registry: 进程注册表
// address: 当前引擎地址
// remote: 远程通信实现
// eventStream: 事件流 PID
type Engine struct {
	Registry    *Registry
	address     string
	remote      Remoter
	eventStream *PID
}

// EngineConfig 用于配置 Engine 的参数
// remote: 远程通信实现
type EngineConfig struct {
	remote Remoter
}

// NewEngineConfig 返回默认 EngineConfig
func NewEngineConfig() EngineConfig {
	return EngineConfig{}
}

// WithRemote 配置远程通信能力
func (config EngineConfig) WithRemote(remote Remoter) EngineConfig {
	config.remote = remote
	return config
}

// NewEngine 根据 EngineConfig 创建新的 Engine 实例
func NewEngine(config EngineConfig) (*Engine, error) {
	e := &Engine{}
	e.Registry = newRegistry(e) // 初始化注册表，支持自定义 deadletter
	e.address = LocalLookupAddr
	if config.remote != nil {
		e.remote = config.remote
		e.address = config.remote.Address()
		err := config.remote.Start(e)
		if err != nil {
			return nil, fmt.Errorf("failed to start remote: %w", err)
		}
	}
	// 启动事件流进程
	e.eventStream = e.Spawn(newEventStream(), "eventstream")
	return e, nil
}

// Spawn 启动一个新的 actor 进程，使用 Producer 生成 Receiver
// kind: 进程类型标识
// opts: 可选配置项
func (e *Engine) Spawn(p Producer, kind string, opts ...OptFunc) *PID {
	options := DefaultOpts(p)
	options.Kind = kind
	for _, opt := range opts {
		opt(&options)
	}
	// 若未指定 ID，则自动生成
	if len(options.ID) == 0 {
		id := strconv.Itoa(rand.Intn(math.MaxInt))
		options.ID = id
	}
	proc := newProcess(e, options)
	return e.SpawnProc(proc)
}

// SpawnFunc 启动一个无状态函数型 actor
func (e *Engine) SpawnFunc(f func(*Context), kind string, opts ...OptFunc) *PID {
	return e.Spawn(newFuncReceiver(f), kind, opts...)
}

// SpawnProc 启动自定义 Processer 实例
// 适用于自定义进程场景
func (e *Engine) SpawnProc(p Processer) *PID {
	e.Registry.add(p)
	return p.PID()
}

// Address 返回当前引擎的地址
// 若无远程配置则为 local，否则为远程监听地址
func (e *Engine) Address() string {
	return e.address
}

// Request 以"请求-响应"方式发送消息，返回 Response 对象
// 调用 Response.Result() 会阻塞直到超时或收到响应
func (e *Engine) Request(pid *PID, msg any, timeout time.Duration) *Response {
	resp := NewResponse(e, timeout)
	e.Registry.add(resp)

	e.SendWithSender(pid, msg, resp.PID())

	return resp
}

// SendWithSender 发送消息并指定 sender
// 接收方可通过 Context.Sender() 获取 sender
func (e *Engine) SendWithSender(pid *PID, msg any, sender *PID) {
	e.send(pid, msg, sender)
}

// Send 发送消息，若目标进程不存在则转发到 DeadLetter
func (e *Engine) Send(pid *PID, msg any) {
	e.send(pid, msg, nil)
}

// BroadcastEvent 广播事件到 eventstream，通知所有订阅者
func (e *Engine) BroadcastEvent(msg any) {
	if e.eventStream != nil {
		e.send(e.eventStream, msg, nil)
	}
}

// send 是底层消息发送实现，自动区分本地/远程/丢失场景
func (e *Engine) send(pid *PID, msg any, sender *PID) {
	// 若目标 PID 为空，直接丢弃
	if pid == nil {
		return
	}
	if e.isLocalMessage(pid) {
		e.SendLocal(pid, msg, sender)
		return
	}
	if e.remote == nil {
		// 无远程时，广播 remote 缺失事件
		e.BroadcastEvent(EngineRemoteMissingEvent{Target: pid, Sender: sender, Message: msg})
		return
	}
	e.remote.Send(pid, msg, sender)
}

// SendRepeater 用于周期性向指定 PID 发送消息
// 可通过 Stop() 停止定时发送
type SendRepeater struct {
	engine   *Engine
	self     *PID
	target   *PID
	msg      any
	interval time.Duration
	cancelch chan struct{}
}

// start 启动定时发送 goroutine
func (sr SendRepeater) start() {
	ticker := time.NewTicker(sr.interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				sr.engine.SendWithSender(sr.target, sr.msg, sr.self)
			case <-sr.cancelch:
				ticker.Stop()
				return
			}
		}
	}()
}

// Stop 停止定时发送
func (sr SendRepeater) Stop() {
	close(sr.cancelch)
}

// SendRepeat 每隔 interval 向指定 PID 发送消息
// 返回 SendRepeater，可调用 Stop 停止
func (e *Engine) SendRepeat(pid *PID, msg any, interval time.Duration) SendRepeater {
	clonedPID := *pid.CloneVT()
	sr := SendRepeater{
		engine:   e,
		self:     nil,
		target:   &clonedPID,
		interval: interval,
		msg:      msg,
		cancelch: make(chan struct{}, 1),
	}
	sr.start()
	return sr
}

// Stop 立即终止指定 PID 关联进程，返回 context 可用于等待停止完成
func (e *Engine) Stop(pid *PID) context.Context {
	return e.sendPoisonPill(context.Background(), false, pid)
}

// Poison 优雅终止指定 PID 关联进程，处理完消息后退出
// 返回 context 可用于等待停止完成
func (e *Engine) Poison(pid *PID) context.Context {
	return e.sendPoisonPill(context.Background(), true, pid)
}

// PoisonCtx 同 Poison，但可自定义 context
func (e *Engine) PoisonCtx(ctx context.Context, pid *PID) context.Context {
	return e.sendPoisonPill(ctx, true, pid)
}

// sendPoisonPill 发送 poison pill 消息，支持优雅/非优雅关闭
func (e *Engine) sendPoisonPill(ctx context.Context, graceful bool, pid *PID) context.Context {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	pill := poisonPill{
		cancel:   cancel,
		graceful: graceful,
	}
	// 若目标进程不存在，广播 DeadLetter 事件
	if e.Registry.get(pid) == nil {
		e.BroadcastEvent(DeadLetterEvent{
			Target:  pid,
			Message: pill,
			Sender:  nil,
		})
		cancel()
		return ctx
	}
	e.SendLocal(pid, pill, nil)
	return ctx
}

// SendLocal 仅向本地进程发送消息，若目标不存在则广播 DeadLetter
func (e *Engine) SendLocal(pid *PID, msg any, sender *PID) {
	proc := e.Registry.get(pid)
	if proc == nil {
		// 广播 deadLetter 消息
		e.BroadcastEvent(DeadLetterEvent{
			Target:  pid,
			Message: msg,
			Sender:  sender,
		})
		return
	}
	proc.Send(pid, msg, sender)
}

// Subscribe 订阅 eventstream 事件流
func (e *Engine) Subscribe(pid *PID) {
	e.Send(e.eventStream, eventSub{pid: pid})
}

// Unsubscribe 取消订阅 eventstream 事件流
func (e *Engine) Unsubscribe(pid *PID) {
	e.Send(e.eventStream, eventUnsub{pid: pid})
}

// isLocalMessage 判断消息目标是否为本地进程
func (e *Engine) isLocalMessage(pid *PID) bool {
	if pid == nil {
		return false
	}
	return e.address == pid.Address
}

// funcReceiver 用于将函数适配为 Receiver
type funcReceiver struct {
	f func(*Context)
}

// newFuncReceiver 将函数包装为 Producer
func newFuncReceiver(f func(*Context)) Producer {
	return func() Receiver {
		return &funcReceiver{
			f: f,
		}
	}
}

// Receive 调用底层函数处理消息
func (r *funcReceiver) Receive(c *Context) {
	r.f(c)
}
