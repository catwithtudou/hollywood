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

// Remoter 接口定义了远程通信的抽象能力
// 用于支持分布式 Actor 系统，实现跨节点的消息传递
type Remoter interface {
	Address() string       // 返回远程节点的网络地址
	Send(*PID, any, *PID)  // 发送消息到远程节点
	Start(*Engine) error   // 启动远程服务，绑定到指定引擎
	Stop() *sync.WaitGroup // 停止远程服务，返回等待组用于同步
}

// Producer 是用于生成 Receiver 的工厂方法类型
// 采用工厂模式，支持延迟创建和依赖注入
type Producer func() Receiver

// Receiver 接口定义了消息接收与处理的核心能力
// 这是 Actor 模型中 Actor 的行为抽象
type Receiver interface {
	Receive(*Context) // 处理接收到的消息，通过 Context 获取消息内容和元数据
}

// Engine 是 Actor 系统的核心引擎
// 负责进程生命周期管理、消息路由、事件广播等核心功能
// 支持本地和远程 Actor 的统一管理
type Engine struct {
	Registry    *Registry // 进程注册表，管理所有本地 Actor 进程
	address     string    // 当前引擎的网络地址标识
	remote      Remoter   // 远程通信实现，支持分布式部署
	eventStream *PID      // 事件流进程的 PID，用于系统事件广播
}

// EngineConfig 封装了 Engine 的配置参数
// 采用配置对象模式，支持灵活的引擎定制
type EngineConfig struct {
	remote Remoter // 可选的远程通信实现
}

// NewEngineConfig 创建默认的引擎配置
// 返回: 包含默认设置的配置对象
func NewEngineConfig() EngineConfig {
	return EngineConfig{}
}

// WithRemote 为引擎配置远程通信能力
// 支持方法链式调用，提供流畅的配置体验
// remote: 远程通信实现
// 返回: 更新后的配置对象
func (config EngineConfig) WithRemote(remote Remoter) EngineConfig {
	config.remote = remote
	return config
}

// NewEngine 根据配置创建新的 Engine 实例
// 初始化注册表、设置地址、启动远程服务和事件流
// config: 引擎配置参数
// 返回: (引擎实例, 错误信息)
func NewEngine(config EngineConfig) (*Engine, error) {
	e := &Engine{}
	// 初始化进程注册表，传入引擎引用用于 deadletter 处理
	e.Registry = newRegistry(e)
	// 默认使用本地地址
	e.address = LocalLookupAddr

	// 如果配置了远程通信，则启动远程服务
	if config.remote != nil {
		e.remote = config.remote
		e.address = config.remote.Address() // 使用远程地址作为引擎地址
		err := config.remote.Start(e)
		if err != nil {
			return nil, fmt.Errorf("failed to start remote: %w", err)
		}
	}

	// 启动系统事件流进程，用于事件广播和订阅
	e.eventStream = e.Spawn(newEventStream(), "eventstream")
	return e, nil
}

// Spawn 启动一个新的 Actor 进程
// 这是创建 Actor 的主要入口，支持灵活的配置选项
// p: Producer 工厂方法，用于创建 Receiver 实例
// kind: 进程类型标识，用于分类和调试
// opts: 可选配置函数，支持自定义进程行为
// 返回: 新创建进程的 PID
func (e *Engine) Spawn(p Producer, kind string, opts ...OptFunc) *PID {
	// 应用默认配置
	options := DefaultOpts(p)
	options.Kind = kind

	// 应用用户自定义配置
	for _, opt := range opts {
		opt(&options)
	}

	// 如果未指定 ID，则自动生成随机 ID
	if len(options.ID) == 0 {
		id := strconv.Itoa(rand.Intn(math.MaxInt))
		options.ID = id
	}

	// 创建进程实例并启动
	proc := newProcess(e, options)
	return e.SpawnProc(proc)
}

// SpawnFunc 启动一个基于函数的无状态 Actor
// 适用于简单的消息处理场景，无需复杂的状态管理
// f: 消息处理函数
// kind: 进程类型标识
// opts: 可选配置函数
// 返回: 新创建进程的 PID
func (e *Engine) SpawnFunc(f func(*Context), kind string, opts ...OptFunc) *PID {
	return e.Spawn(newFuncReceiver(f), kind, opts...)
}

// SpawnProc 启动自定义的 Processer 实例
// 适用于需要完全自定义进程行为的高级场景
// p: 自定义的进程实现
// 返回: 进程的 PID
func (e *Engine) SpawnProc(p Processer) *PID {
	// 将进程注册到引擎的注册表中
	e.Registry.add(p)
	return p.PID()
}

// Address 返回当前引擎的网络地址
// 本地引擎返回 "local"，远程引擎返回实际的网络地址
func (e *Engine) Address() string {
	return e.address
}

// Request 实现请求-响应模式的消息发送
// 创建临时响应进程，发送消息后等待回复
// pid: 目标进程 PID
// msg: 要发送的消息
// timeout: 响应超时时间
// 返回: Response 对象，可调用 Result() 等待响应
func (e *Engine) Request(pid *PID, msg any, timeout time.Duration) *Response {
	// 创建临时响应进程
	resp := NewResponse(e, timeout)
	e.Registry.add(resp)

	// 发送消息，指定响应进程为 sender
	e.SendWithSender(pid, msg, resp.PID())

	return resp
}

// SendWithSender 发送消息并指定发送者
// 接收方可通过 Context.Sender() 获取发送者信息，用于回复消息
// pid: 目标进程 PID
// msg: 要发送的消息
// sender: 发送者 PID
func (e *Engine) SendWithSender(pid *PID, msg any, sender *PID) {
	e.send(pid, msg, sender)
}

// Send 发送消息到指定进程
// 这是最常用的消息发送方法，发送者为 nil
// pid: 目标进程 PID
// msg: 要发送的消息
func (e *Engine) Send(pid *PID, msg any) {
	e.send(pid, msg, nil)
}

// BroadcastEvent 广播系统事件到事件流
// 所有订阅了事件流的进程都会收到该事件
// 用于系统监控、日志记录、调试等场景
// msg: 要广播的事件消息
func (e *Engine) BroadcastEvent(msg any) {
	if e.eventStream != nil {
		e.send(e.eventStream, msg, nil)
	}
}

// send 是底层的消息发送实现
// 自动判断目标是本地进程还是远程进程，并选择合适的发送方式
// pid: 目标进程 PID
// msg: 要发送的消息
// sender: 发送者 PID
func (e *Engine) send(pid *PID, msg any, sender *PID) {
	// 空 PID 检查，直接丢弃
	if pid == nil {
		return
	}

	// 判断是否为本地消息
	if e.isLocalMessage(pid) {
		e.SendLocal(pid, msg, sender)
		return
	}

	// 远程消息处理
	if e.remote == nil {
		// 没有配置远程通信，广播远程缺失事件
		e.BroadcastEvent(EngineRemoteMissingEvent{Target: pid, Sender: sender, Message: msg})
		return
	}

	// 通过远程通信发送消息
	e.remote.Send(pid, msg, sender)
}

// SendRepeater 实现定时重复发送消息的功能
// 用于心跳检测、定时任务等场景
type SendRepeater struct {
	engine   *Engine       // 引擎引用
	self     *PID          // 发送者 PID
	target   *PID          // 目标进程 PID
	msg      any           // 要发送的消息
	interval time.Duration // 发送间隔
	cancelch chan struct{} // 取消通道
}

// start 启动定时发送的后台 goroutine
// 使用 ticker 实现精确的时间间隔控制
func (sr SendRepeater) start() {
	ticker := time.NewTicker(sr.interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				// 定时发送消息
				sr.engine.SendWithSender(sr.target, sr.msg, sr.self)
			case <-sr.cancelch:
				// 收到取消信号，停止定时器并退出
				ticker.Stop()
				return
			}
		}
	}()
}

// Stop 停止定时发送
// 通过关闭取消通道通知后台 goroutine 退出
func (sr SendRepeater) Stop() {
	close(sr.cancelch)
}

// SendRepeat 创建定时重复发送器
// 每隔指定间隔向目标进程发送消息
// pid: 目标进程 PID
// msg: 要发送的消息
// interval: 发送间隔
// 返回: SendRepeater 实例，可调用 Stop() 停止发送
func (e *Engine) SendRepeat(pid *PID, msg any, interval time.Duration) SendRepeater {
	// 克隆 PID 避免并发修改问题
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

// Stop 立即终止指定进程
// 不等待进程处理完当前消息，强制停止
// pid: 要停止的进程 PID
// 返回: context，可用于等待停止完成
func (e *Engine) Stop(pid *PID) context.Context {
	return e.sendPoisonPill(context.Background(), false, pid)
}

// Poison 优雅地终止指定进程
// 等待进程处理完收件箱中的所有消息后再停止
// pid: 要停止的进程 PID
// 返回: context，可用于等待停止完成
func (e *Engine) Poison(pid *PID) context.Context {
	return e.sendPoisonPill(context.Background(), true, pid)
}

// PoisonCtx 优雅地终止指定进程，支持自定义 context
// 允许调用者控制停止操作的上下文和超时
// ctx: 父 context
// pid: 要停止的进程 PID
// 返回: 子 context，可用于等待停止完成
func (e *Engine) PoisonCtx(ctx context.Context, pid *PID) context.Context {
	return e.sendPoisonPill(ctx, true, pid)
}

// sendPoisonPill 发送毒丸消息实现进程终止
// 毒丸是 Actor 模型中用于优雅关闭进程的特殊消息
// ctx: 父 context
// graceful: 是否优雅关闭（处理完所有消息）
// pid: 目标进程 PID
// 返回: 子 context，当进程停止时会被取消
func (e *Engine) sendPoisonPill(ctx context.Context, graceful bool, pid *PID) context.Context {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)

	// 创建毒丸消息
	pill := poisonPill{
		cancel:   cancel,   // 进程停止时调用此函数取消 context
		graceful: graceful, // 是否优雅关闭
	}

	// 检查目标进程是否存在
	if e.Registry.get(pid) == nil {
		// 进程不存在，广播 DeadLetter 事件
		e.BroadcastEvent(DeadLetterEvent{
			Target:  pid,
			Message: pill,
			Sender:  nil,
		})
		cancel() // 立即取消 context
		return ctx
	}

	// 发送毒丸消息到目标进程
	e.SendLocal(pid, pill, nil)
	return ctx
}

// SendLocal 仅向本地进程发送消息
// 如果目标进程不存在，则广播 DeadLetter 事件
// pid: 目标进程 PID
// msg: 要发送的消息
// sender: 发送者 PID
func (e *Engine) SendLocal(pid *PID, msg any, sender *PID) {
	// 从注册表查找目标进程
	proc := e.Registry.get(pid)
	if proc == nil {
		// 进程不存在，广播 DeadLetter 事件
		// DeadLetter 用于处理发送到不存在进程的消息
		e.BroadcastEvent(DeadLetterEvent{
			Target:  pid,
			Message: msg,
			Sender:  sender,
		})
		return
	}

	// 将消息发送到目标进程的收件箱
	proc.Send(pid, msg, sender)
}

// Subscribe 订阅系统事件流
// 订阅后，指定进程会收到所有系统广播的事件
// pid: 要订阅事件的进程 PID
func (e *Engine) Subscribe(pid *PID) {
	e.Send(e.eventStream, eventSub{pid: pid})
}

// Unsubscribe 取消订阅系统事件流
// 取消订阅后，指定进程不再收到系统事件
// pid: 要取消订阅的进程 PID
func (e *Engine) Unsubscribe(pid *PID) {
	e.Send(e.eventStream, eventUnsub{pid: pid})
}

// isLocalMessage 判断消息目标是否为本地进程
// 通过比较地址判断是否需要本地处理还是远程转发
// pid: 目标进程 PID
// 返回: true 表示本地进程，false 表示远程进程
func (e *Engine) isLocalMessage(pid *PID) bool {
	if pid == nil {
		return false
	}
	return e.address == pid.Address
}

// funcReceiver 将普通函数适配为 Receiver 接口
// 用于支持函数式的 Actor 编程风格
type funcReceiver struct {
	f func(*Context) // 底层的消息处理函数
}

// newFuncReceiver 创建函数式 Receiver 的 Producer
// 采用闭包模式，将函数包装为 Producer 工厂方法
// f: 消息处理函数
// 返回: Producer 工厂方法
func newFuncReceiver(f func(*Context)) Producer {
	return func() Receiver {
		return &funcReceiver{
			f: f,
		}
	}
}

// Receive 实现 Receiver 接口
// 直接调用底层函数处理消息
// c: 消息上下文，包含消息内容和元数据
func (r *funcReceiver) Receive(c *Context) {
	r.f(c)
}
