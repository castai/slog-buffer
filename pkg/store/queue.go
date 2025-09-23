package store

import "sync"

type Queue[T any] interface {
	Enqueue(v T)
	Dequeue() (T, bool)
	Peek() (T, bool)
	Size() int
}

type ringQueue[T any] struct {
	buf        []T
	head, tail int
	size       int
	count      int

	mu       sync.RWMutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
}

func NewRingQueue[T any](size int) Queue[T] {
	if size <= 0 {
		panic("size must be > 0")
	}
	q := &ringQueue[T]{buf: make([]T, size), size: size}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	return q
}

func (q *ringQueue[T]) Peek() (T, bool) {
	var zero T
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count == 0 {
		return zero, false
	}
	return q.buf[q.head], true
}

func (q *ringQueue[T]) Enqueue(v T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count == q.size {
		q.head = (q.head + 1) % q.size
		q.count--
	}
	q.buf[q.tail] = v
	q.tail = (q.tail + 1) % q.size
	q.count++
}

func (q *ringQueue[T]) Dequeue() (T, bool) {
	var zero T
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count == 0 {
		return zero, false
	}
	v := q.buf[q.head]
	q.buf[q.head] = zero
	q.head = (q.head + 1) % q.size
	q.count--
	return v, true
}

func (q *ringQueue[T]) Size() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.count
}
