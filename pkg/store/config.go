package store

import (
	"time"

	"github.com/coder/quartz"
)

type (
	Option func(cfg *Config)
)

type ErrorHandler interface {
	OnError(err error)
}

func WithInterval(i time.Duration) Option {
	return func(cfg *Config) {
		cfg.Interval = i
	}
}

func WithBatchSize(s int) Option {
	return func(cfg *Config) {
		cfg.BatchSize = s
	}
}

func WithCapacity(c int) Option {
	return func(cfg *Config) {
		cfg.Capacity = c
	}
}

func WithOnError(onError ErrorHandler) Option {
	return func(cfg *Config) {
		cfg.err = onError
	}
}

func WithClock(c quartz.Clock) Option {
	return func(cfg *Config) {
		cfg.clock = c
	}
}

type Config struct {
	Interval  time.Duration
	BatchSize int
	Capacity  int
	err       ErrorHandler
	clock     quartz.Clock
}

var defaults = &Config{
	Interval:  5 * time.Second,
	BatchSize: 200,
	Capacity:  10000,
	clock:     quartz.NewReal(),
}
