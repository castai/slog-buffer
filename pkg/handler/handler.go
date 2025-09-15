// Package handler provides a buffered slog.Handler implementation
// that transforms log records and writes them to a provided store.
package handler

import (
	"context"
	"log/slog"

	"github.com/castai/slog-buffer/pkg/store"
)

var defaultLevel = slog.LevelInfo

type Config struct {
	Level slog.Level
}
type (
	Option           func(c *Config)
	Transform[T any] func(r slog.Record) (T, error)
)

var defaults = &Config{
	Level: slog.LevelInfo,
}

// Buffered is a slog.Handler that buffers log records by transforming them
// and writing them to a provided store. It is useful for scenarios where
// logs need to be collected and processed in batches, for example, before
// sending them to an external service or database.
type Buffered[T any] struct {
	store     store.Store[T]
	transform Transform[T]
	cfg       *Config
	attrs     []slog.Attr
}

// WithLevel returns an Option that sets the logging level for the handler.
// This allows you to control the verbosity of the logs the handler will
// process.
func WithLevel(l slog.Level) Option {
	return func(c *Config) {
		c.Level = l
	}
}

// NewBuffered creates and returns a new Buffered handler.
//
// The transform function is used to convert an slog.Record into the
// specific type T before it is written to the store.
//
// The store is where the transformed logs will be written. It must implement
// the store.Store[T] interface.
func NewBuffered[T any](transform Transform[T], store store.Store[T], options ...Option) *Buffered[T] {
	b := &Buffered[T]{
		store:     store,
		cfg:       defaults,
		transform: transform,
		attrs:     []slog.Attr{},
	}

	for _, o := range options {
		o(b.cfg)
	}

	return b
}

// Enabled reports whether the handler will log a record with the given level.
func (b *Buffered[T]) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= b.cfg.Level
}

// Handle transforms a log record and writes it to the store.
func (b *Buffered[T]) Handle(ctx context.Context, r slog.Record) error {
	e, err := b.transform(r)
	if err != nil {
		return err
	}

	b.store.Write(e)
	return nil
}

// WithAttrs implements slog.Handler interface
func (h *Buffered[T]) WithAttrs(attrs []slog.Attr) slog.Handler {
	newHandler := *h
	newHandler.attrs = append(h.attrs, attrs...)
	return &newHandler
}

// WithGroup implements slog.Handler interface
func (h *Buffered[T]) WithGroup(name string) slog.Handler {
	// For simplicity, we're not implementing group support here
	return h
}
