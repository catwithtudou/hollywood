package ringbuffer

import (
	"sync"
	"sync/atomic"
)

// buffer 是环形缓冲区的底层结构，存储元素及头尾指针
// items: 存储元素的切片
// head: 头指针，指向下一个出队元素
// tail: 尾指针，指向下一个入队位置
// mod: 缓冲区容量（用于取模实现环形）
type buffer[T any] struct {
	items           []T
	head, tail, mod int64
}

// RingBuffer 是线程安全的泛型环形缓冲区
// len: 当前元素数量
// content: 实际存储内容的 buffer
// mu: 互斥锁，保证并发安全
type RingBuffer[T any] struct {
	len     int64
	content *buffer[T]
	mu      sync.Mutex
}

// New 创建一个指定容量的环形缓冲区
func New[T any](size int64) *RingBuffer[T] {
	return &RingBuffer[T]{
		content: &buffer[T]{
			items: make([]T, size),
			head:  0,
			tail:  0,
			mod:   size,
		},
		len: 0,
	}
}

// Push 向缓冲区尾部插入一个元素
// 若缓冲区已满，则自动扩容为原来的两倍
func (rb *RingBuffer[T]) Push(item T) {
	rb.mu.Lock()
	// 计算新尾指针位置
	rb.content.tail = (rb.content.tail + 1) % rb.content.mod
	// 如果尾指针追上头指针，说明缓冲区已满，需要扩容
	if rb.content.tail == rb.content.head {
		size := rb.content.mod * 2
		newBuff := make([]T, size)
		// 将原有数据顺序复制到新缓冲区
		for i := int64(0); i < rb.content.mod; i++ {
			idx := (rb.content.tail + i) % rb.content.mod
			newBuff[i] = rb.content.items[idx]
		}
		content := &buffer[T]{
			items: newBuff,
			head:  0,
			tail:  rb.content.mod,
			mod:   size,
		}
		rb.content = content
	}
	atomic.AddInt64(&rb.len, 1)
	// 元素写入尾部
	rb.content.items[rb.content.tail] = item
	rb.mu.Unlock()
}

// Len 返回当前缓冲区元素数量
func (rb *RingBuffer[T]) Len() int64 {
	return atomic.LoadInt64(&rb.len)
}

// Pop 从缓冲区头部弹出一个元素
// 若缓冲区为空，返回 false
func (rb *RingBuffer[T]) Pop() (T, bool) {
	rb.mu.Lock()
	if rb.len == 0 {
		rb.mu.Unlock()
		var t T
		return t, false
	}
	// 头指针前移，获取元素
	rb.content.head = (rb.content.head + 1) % rb.content.mod
	item := rb.content.items[rb.content.head]
	var t T
	// 清空已弹出位置，便于 GC
	rb.content.items[rb.content.head] = t
	atomic.AddInt64(&rb.len, -1)
	rb.mu.Unlock()
	return item, true
}

// PopN 批量弹出 n 个元素，返回弹出的切片
// 若缓冲区为空，返回 false
func (rb *RingBuffer[T]) PopN(n int64) ([]T, bool) {
	rb.mu.Lock()
	if rb.len == 0 {
		rb.mu.Unlock()
		return nil, false
	}
	content := rb.content

	// 若 n 超过当前长度，则只弹出全部元素
	if n >= rb.len {
		n = rb.len
	}
	atomic.AddInt64(&rb.len, -n)

	items := make([]T, n)
	for i := int64(0); i < n; i++ {
		pos := (content.head + 1 + i) % content.mod
		items[i] = content.items[pos]
		var t T
		// 清空已弹出位置，便于 GC
		content.items[pos] = t
	}
	// 头指针批量前移
	content.head = (content.head + n) % content.mod

	rb.mu.Unlock()
	return items, true
}
