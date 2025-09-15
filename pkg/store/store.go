// Package store provides a configurable batching memory queue for streaming
// data entries with periodic or size-based flushing to a user-provided Flusher.
//
// It is designed for use cases where entries must be collected and written out
// in batches, such as for logging, metrics, or any buffered data pipeline.
//
//go:generate go run go.uber.org/mock/mockgen -destination=mocks/mocks.go -package=mocks github.com/castai/slog-buffer/pkg/store Store,Flusher,ErrorHandler
package store

import (
	"context"

	"github.com/coder/quartz"
)

// Flusher is an interface for a batch writer. Implementations are responsible for
// processing a batch of entries atomically. If an error occurs, Flush should return it.
type Flusher[T any] interface {
	// Flush sends a batch of entries.
	// ctx controls cancellation, batching, or deadlines.
	Flush(ctx context.Context, entries []T) error
}

// Store is an interface for buffered write and lifecycle control.
// Write buffers entries and Run starts processing of submitted entries in background.
type Store[T any] interface {
	// Write queues one or more entries into the buffer.
	Write(entries ...T)
	// Run starts the background processing loop on the provided context.
	// The loop continues until the context is canceled.
	Run(ctx context.Context)
}

// memory implements Store using a channel-based buffer and periodic/batch-size-based flushing.
//
// This type is not thread-safe for direct field access.
// All mutation should be done using exported Store methods.
type memory[T any] struct {
	flusher Flusher[T]     // User-provided batch Flusher
	cfg     *Config        // Configuration
	flush   chan any       // Signal channel for external/manual flush requests
	buffer  chan T         // Buffered channel of incoming entries
	ticker  *quartz.Ticker // Time-based flush trigger
	batch   []T            // Current unflushed batch
}

// NewMemory creates a new buffered Store using the provided Flusher and configuration options.
//
// Options can control batch size, flush interval, error handling, and clock/ticker source.
// The returned Store should be started with Run().
func NewMemory[T any](flusher Flusher[T], options ...Option) Store[T] {
	cfg := defaults

	for _, o := range options {
		o(cfg)
	}

	m := &memory[T]{
		cfg:     cfg,
		flusher: flusher,
		flush:   make(chan any, 1),
		buffer:  make(chan T, cfg.Capacity),
		ticker:  cfg.clock.NewTicker(cfg.Interval),
		batch:   make([]T, 0, cfg.BatchSize),
	}

	return m
}

// Write submits entries to the buffer, batching them according to configuration.
//
// If the buffer is full, forces a flush signal and retries entry submission.
func (m *memory[T]) Write(entries ...T) {
	for _, e := range entries {
		select {
		case m.buffer <- e:
		default:
			m.force()
			m.buffer <- e
		}
	}
}

// force triggers an immediate flush by signaling on the flush channel,
// unless a flush is already pending.
func (m *memory[T]) force() {
	select {
	case m.flush <- struct{}{}:
	default:
	}
}

// Run begins the internal processing loop which dequeues entries,
// triggers flushes on interval, batch size, or external signal, and handles context cancellation.
func (m *memory[T]) Run(ctx context.Context) {
	go m.run(ctx)
}

// run contains the main loop performing batching, flushing, and handling exit conditions.
func (m *memory[T]) run(ctx context.Context) {
	for {
		ok := m.once(ctx)
		if !ok {
			return
		}
	}
}

// once performs a single processing step, handling buffer dequeues,
// timer ticks, flush signals, or context cancellation.
// Returns false when processing should terminate.
func (m *memory[T]) once(ctx context.Context) bool {
	select {
	case item := <-m.buffer:
		m.batch = append(m.batch, item)
		if len(m.batch) >= m.cfg.BatchSize {
			m.tryFlush(ctx)
		}
	case <-m.ticker.C:
		if len(m.batch) > 0 {
			m.tryFlush(ctx)
		}
	case <-m.flush:
		m.drain()
		if len(m.batch) > 0 {
			m.tryFlush(ctx)
		}
	case <-ctx.Done():
		m.drain()
		if len(m.batch) > 0 {
			m.tryFlush(ctx)
		}
		return false
	}
	return true
}

// drain dequeues all entries currently in the buffer, appending them to the batch.
// No flush is performed; this only moves entries.
func (m *memory[T]) drain() {
	drained := false
	for !drained {
		select {
		case entry := <-m.buffer:
			m.batch = append(m.batch, entry)
		default:
			drained = true
		}
	}
}

// tryFlush sends the current batch to the Flusher, handles errors via configured handler,
// and resets batch state.
func (m *memory[T]) tryFlush(ctx context.Context) {
	if err := m.flusher.Flush(ctx, m.batch); err != nil && m.cfg.err != nil {
		m.cfg.err.OnError(err)
	}
	m.batch = m.batch[:0]
}
