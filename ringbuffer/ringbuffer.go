package ringbuffer

import (
	"sync"
	"sync/atomic"
)

// buffer 是环形缓冲区的底层结构，存储元素及头尾指针
// 采用经典的环形缓冲区设计：使用固定大小数组和头尾指针实现
// items: 存储元素的切片，作为环形数组使用
// head: 头指针，指向下一个出队元素的前一个位置（空位）
// tail: 尾指针，指向最后一个入队元素的位置
// mod: 缓冲区容量，用于取模运算实现环形特性
type buffer[T any] struct {
	items           []T
	head, tail, mod int64
}

// RingBuffer 是线程安全的泛型环形缓冲区
// 支持动态扩容，当缓冲区满时自动扩容为原来的两倍
// 使用互斥锁保证并发安全，原子操作优化长度读取性能
type RingBuffer[T any] struct {
	len     int64      // 当前元素数量，使用原子操作保证并发读取安全
	content *buffer[T] // 实际存储内容的 buffer
	mu      sync.Mutex // 互斥锁，保护 buffer 结构的并发访问
}

// New 创建一个指定容量的环形缓冲区
// size: 初始容量，必须大于 0
// 返回: 初始化完成的环形缓冲区实例
func New[T any](size int64) *RingBuffer[T] {
	return &RingBuffer[T]{
		content: &buffer[T]{
			items: make([]T, size),
			head:  0,    // 初始头指针指向位置 0
			tail:  0,    // 初始尾指针指向位置 0
			mod:   size, // 容量用于取模运算
		},
		len: 0, // 初始长度为 0
	}
}

// Push 向缓冲区尾部插入一个元素
// 采用先移动指针再检查是否需要扩容的策略
// 若缓冲区已满，则自动扩容为原来的两倍，保证 O(1) 摊还复杂度
// item: 要插入的元素
func (rb *RingBuffer[T]) Push(item T) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// 先移动尾指针到下一个位置
	rb.content.tail = (rb.content.tail + 1) % rb.content.mod

	// 检查是否发生环形缓冲区满的情况（尾指针追上头指针）
	if rb.content.tail == rb.content.head {
		// 缓冲区已满，需要扩容
		size := rb.content.mod * 2
		newBuff := make([]T, size)

		// 将原有数据按顺序复制到新缓冲区的前半部分
		// 从 tail+1 开始复制，保持元素的逻辑顺序
		for i := int64(0); i < rb.content.mod; i++ {
			idx := (rb.content.tail + i) % rb.content.mod
			newBuff[i] = rb.content.items[idx]
		}

		// 创建新的 buffer，重置头尾指针
		content := &buffer[T]{
			items: newBuff,
			head:  0,              // 头指针重置为 0
			tail:  rb.content.mod, // 尾指针指向已复制数据的末尾
			mod:   size,           // 新的容量
		}
		rb.content = content
	}

	// 原子递增长度计数器
	atomic.AddInt64(&rb.len, 1)
	// 将元素写入当前尾指针位置
	rb.content.items[rb.content.tail] = item
}

// Len 返回当前缓冲区元素数量
// 使用原子操作读取，保证并发安全且无需加锁
func (rb *RingBuffer[T]) Len() int64 {
	return atomic.LoadInt64(&rb.len)
}

// Pop 从缓冲区头部弹出一个元素
// 采用 FIFO（先进先出）策略
// 返回: (元素值, 是否成功)，若缓冲区为空则返回 (零值, false)
func (rb *RingBuffer[T]) Pop() (T, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// 检查缓冲区是否为空
	if rb.len == 0 {
		var t T // 返回类型 T 的零值
		return t, false
	}

	// 移动头指针到下一个有效元素位置
	rb.content.head = (rb.content.head + 1) % rb.content.mod
	item := rb.content.items[rb.content.head]

	// 清空已弹出的位置，帮助 GC 回收引用类型
	var t T
	rb.content.items[rb.content.head] = t

	// 原子递减长度计数器
	atomic.AddInt64(&rb.len, -1)
	return item, true
}

// PopN 批量弹出 n 个元素，返回弹出的切片
// 提供批量操作以提高性能，减少锁竞争
// n: 要弹出的元素数量
// 返回: (元素切片, 是否成功)，若缓冲区为空则返回 (nil, false)
func (rb *RingBuffer[T]) PopN(n int64) ([]T, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// 检查缓冲区是否为空
	if rb.len == 0 {
		return nil, false
	}

	content := rb.content

	// 边界检查：若请求数量超过当前长度，则只弹出全部元素
	if n >= rb.len {
		n = rb.len
	}

	// 原子递减长度计数器
	atomic.AddInt64(&rb.len, -n)

	// 预分配结果切片，避免动态扩容
	items := make([]T, n)

	// 批量复制元素，处理环形边界情况
	for i := int64(0); i < n; i++ {
		pos := (content.head + 1 + i) % content.mod
		items[i] = content.items[pos]

		// 清空已弹出位置，帮助 GC 回收引用类型
		var t T
		content.items[pos] = t
	}

	// 批量移动头指针
	content.head = (content.head + n) % content.mod

	return items, true
}
