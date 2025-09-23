package store

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/coder/quartz"
	"go.uber.org/mock/gomock"

	"github.com/castai/slog-buffer/pkg/store/mocks"
)

func TestMemory_Write(t *testing.T) {
	type tuners struct {
		mockFlush   func(m *mocks.MockFlusher[*int])
		mockOnError func(m *mocks.MockErrorHandler)
	}

	tests := []struct {
		name      string
		batchSize int
		entries   int
		flushes   int
		tuners    tuners
	}{
		{
			name:      "should not flush on entries less than batch size",
			batchSize: 5,
			entries:   4,
			flushes:   0,
			tuners: tuners{
				mockFlush:   func(flusher *mocks.MockFlusher[*int]) {},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
		},
		{
			name:      "should flush exactly once when batch size is reached",
			batchSize: 5,
			entries:   5,
			flushes:   1,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[*int]) {
					flusher.EXPECT().Flush(gomock.Any(), gomock.Any()).Return(nil).Times(1)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
		},
		{
			name:      "should flush exactly once when batch size is reached",
			batchSize: 5,
			entries:   5,
			flushes:   1,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[*int]) {
					flusher.EXPECT().Flush(gomock.Any(), gomock.Any()).Return(nil).Times(1)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
		},
		{
			name:      "should handle flush error",
			batchSize: 5,
			entries:   5,
			flushes:   1,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[*int]) {
					flusher.EXPECT().Flush(gomock.Any(), gomock.Any()).Return(fmt.Errorf("error")).Times(1)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {
					m.EXPECT().OnError(gomock.Any()).Times(1)
				},
			},
		},
		{
			name:      "should flush multiple times when entries exceed batch size",
			batchSize: 5,
			entries:   12,
			flushes:   2,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[*int]) {
					flusher.EXPECT().Flush(gomock.Any(), gomock.Any()).Return(nil).Times(2)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
		},
		{
			name:      "should handle empty entries gracefully",
			batchSize: 5,
			entries:   0,
			flushes:   0,
			tuners: tuners{
				mockFlush:   func(flusher *mocks.MockFlusher[*int]) {},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
		},
		{
			name:      "should handle entries larger than batch size in a single call",
			batchSize: 5,
			entries:   6,
			flushes:   1,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[*int]) {
					flusher.EXPECT().Flush(gomock.Any(), gomock.Any()).Return(nil).Times(1)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			flusher := mocks.NewMockFlusher[*int](ctrl)
			tt.tuners.mockFlush(flusher)
			errHandler := mocks.NewMockErrorHandler(ctrl)
			tt.tuners.mockOnError(errHandler)
			c := quartz.NewMock(t)
			s := NewMemory(flusher, WithBatchSize(tt.batchSize), WithInterval(1*time.Hour), WithClock(c), WithOnError(errHandler))

			for i := 0; i < tt.entries; i++ {
				s.Write(&i)
				s.(*memory[*int]).once(t.Context())
			}
		})
	}
}

func TestMemory_once(t *testing.T) {
	type tuners struct {
		mockFlush   func(m *mocks.MockFlusher[int])
		mockOnError func(m *mocks.MockErrorHandler)
	}

	tests := []struct {
		name          string
		batchSize     int
		setup         func(m *memory[int], ctx context.Context)
		tuners        tuners
		expectedFlush int
		isOk          bool
		cancel        bool
	}{
		{
			name:      "append to batch when not full",
			batchSize: 5,
			setup: func(m *memory[int], ctx context.Context) {
				m.buffer <- 1
			},
			expectedFlush: 0,
			cancel:        false,
			isOk:          true,
		},
		{
			name:      "flush when batch size reached",
			batchSize: 1,
			setup: func(m *memory[int], ctx context.Context) {
				m.buffer <- 1
			},
			expectedFlush: 1,
			cancel:        false,
			isOk:          true,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
					flusher.EXPECT().Flush(gomock.Any(), []int{1}).Return(nil).Times(1)
				},
			},
		},
		{
			name:      "flush with error handled",
			batchSize: 1,
			setup: func(m *memory[int], ctx context.Context) {
				m.buffer <- 42
			},
			expectedFlush: 1,
			cancel:        false,
			isOk:          true,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
					flusher.EXPECT().Flush(gomock.Any(), []int{42}).Return(fmt.Errorf("boom")).Times(1)
				},
				mockOnError: func(h *mocks.MockErrorHandler) {
					h.EXPECT().OnError(gomock.Any()).Times(1)
				},
			},
		},
		{
			name:      "flush when ticker fires and batch not empty",
			batchSize: 10,
			setup: func(m *memory[int], ctx context.Context) {
				m.batch = append(m.batch, 42)
				m.cfg.clock.(*quartz.Mock).AdvanceNext()
			},
			expectedFlush: 1,
			cancel:        false,
			isOk:          true,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
					flusher.EXPECT().Flush(gomock.Any(), []int{42}).Return(nil).Times(1)
				},
			},
		},
		{
			name:      "no flush when ticker fires and batch empty",
			batchSize: 10,
			setup: func(m *memory[int], ctx context.Context) {
				m.cfg.clock.(*quartz.Mock).AdvanceNext()
			},
			expectedFlush: 0,
			cancel:        false,
			isOk:          true,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
				},
				mockOnError: func(h *mocks.MockErrorHandler) {
				},
			},
		},
		{
			name:      "force flush drains buffer",
			batchSize: 5,
			setup: func(m *memory[int], ctx context.Context) {
				m.batch = append(m.batch, 1, 2)
				m.force()
			},
			expectedFlush: 1,
			cancel:        false,
			isOk:          true,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
					flusher.EXPECT().Flush(gomock.Any(), []int{1, 2}).Return(nil).Times(1)
				},
				mockOnError: func(h *mocks.MockErrorHandler) {
				},
			},
		},
		{
			name:      "context exit flushes non-empty batch",
			batchSize: 5,
			setup: func(m *memory[int], ctx context.Context) {
				m.batch = append(m.batch, 1, 2)
			},
			expectedFlush: 1,
			cancel:        true,
			isOk:          false,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
					flusher.EXPECT().Flush(gomock.Any(), []int{1, 2}).Return(nil).Times(1)
				},
				mockOnError: func(h *mocks.MockErrorHandler) {
				},
			},
		},
		{
			name:      "context exit with empty batch",
			batchSize: 5,
			setup: func(m *memory[int], ctx context.Context) {
			},
			expectedFlush: 0,
			cancel:        true,
			isOk:          false,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
				},
				mockOnError: func(h *mocks.MockErrorHandler) {
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			flusher := mocks.NewMockFlusher[int](ctrl)
			if tt.tuners.mockFlush != nil {
				tt.tuners.mockFlush(flusher)
			}

			errHandler := mocks.NewMockErrorHandler(ctrl)
			if tt.tuners.mockOnError != nil {
				tt.tuners.mockOnError(errHandler)
			}

			ctx, cancel := context.WithCancel(context.Background())
			s := NewMemory(flusher, WithBatchSize(tt.batchSize), WithInterval(1*time.Hour), WithOnError(errHandler), WithClock(quartz.NewMock(t)))
			m := s.(*memory[int])

			tt.setup(m, ctx)

			if tt.cancel {
				cancel()
			}

			done := make(chan bool, 1)
			go func() { done <- m.once(ctx) }()

			select {
			case result := <-done:
				if result != tt.isOk {
					t.Errorf("once() returned %v, want %v", result, tt.isOk)
				}
			case <-time.After(100 * time.Millisecond):
				t.Fatalf("once() timed out")
			}

			cancel()
		})
	}
}

func TestMemory_drain(t *testing.T) {
	type fields struct {
		batchSize int
		bufferCap int
	}
	type tuners struct {
		mockFlush func(m *mocks.MockFlusher[int])
	}
	type setupFn func(m *memory[int])
	tests := []struct {
		name       string
		entries    []int
		fields     fields
		setup      setupFn
		wantBatch  []int
		wantBuffer int
		tuners     tuners
	}{
		{
			name:       "drain with empty buffer",
			entries:    []int{},
			fields:     fields{batchSize: 5, bufferCap: 5},
			setup:      nil,
			wantBatch:  []int{},
			wantBuffer: 0,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
				},
			},
		},
		{
			name:       "drain with single entry",
			entries:    []int{1},
			fields:     fields{batchSize: 5, bufferCap: 5},
			setup:      nil,
			wantBatch:  []int{1},
			wantBuffer: 0,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
				},
			},
		},
		{
			name:       "drain with multiple entries",
			entries:    []int{1, 2, 3},
			fields:     fields{batchSize: 5, bufferCap: 5},
			setup:      nil,
			wantBatch:  []int{1, 2, 3},
			wantBuffer: 0,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
				},
			},
		},
		{
			name:       "buffer full, drain all",
			entries:    []int{1, 2, 3, 4, 5},
			fields:     fields{batchSize: 5, bufferCap: 5},
			setup:      nil,
			wantBatch:  []int{},
			wantBuffer: 0,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
					flusher.EXPECT().Flush(gomock.Any(), []int{1, 2, 3, 4, 5}).Return(nil).Times(1)
				},
			},
		},
		{
			name:    "drain preserves batch if entries already present",
			entries: []int{4, 5},
			fields:  fields{batchSize: 5, bufferCap: 5},
			setup: func(m *memory[int]) {
				m.batch = []int{99, 100}
			},
			wantBatch:  []int{99, 100, 4, 5},
			wantBuffer: 0,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			flusher := mocks.NewMockFlusher[int](ctrl)
			tt.tuners.mockFlush(flusher)
			mem := NewMemory(flusher, WithBatchSize(tt.fields.batchSize), WithCapacity(tt.fields.bufferCap)).(*memory[int])

			if tt.setup != nil {
				tt.setup(mem)
			}

			for _, e := range tt.entries {
				mem.buffer <- e
			}

			mem.drain()

			if !reflect.DeepEqual(mem.batch, tt.wantBatch) {
				t.Errorf("batch got %v, want %v", mem.batch, tt.wantBatch)
			}

			if len(mem.buffer) != tt.wantBuffer {
				t.Errorf("buffer len got %v, want %v", len(mem.buffer), tt.wantBuffer)
			}
		})
	}
}

func TestMemory_tryFlush(t *testing.T) {
	type tuners struct {
		mockFlush   func(m *mocks.MockFlusher[int])
		mockOnError func(m *mocks.MockErrorHandler)
	}

	tests := []struct {
		name         string
		initialBatch []int
		initialQueue [][]int
		tuners       tuners
		wantBatch    []int
		wantQueueLen int
		expectFlush  bool
	}{
		{
			name:         "successful flush of current batch",
			initialBatch: []int{1, 2, 3},
			initialQueue: nil,
			wantBatch:    []int{},
			wantQueueLen: 0,
			expectFlush:  true,
			tuners: tuners{
				mockFlush: func(m *mocks.MockFlusher[int]) {
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{1, 2, 3})).Return(nil).Times(1)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
		},
		{
			name:         "empty batch does not flush",
			initialBatch: []int{},
			initialQueue: nil,
			wantBatch:    []int{},
			wantQueueLen: 0,
			expectFlush:  false,
			tuners: tuners{
				mockFlush:   func(m *mocks.MockFlusher[int]) {},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
		},
		{
			name:         "flush error on current batch puts it on queue",
			initialBatch: []int{4, 5, 6},
			initialQueue: nil,
			wantBatch:    []int{},
			wantQueueLen: 1,
			expectFlush:  true,
			tuners: tuners{
				mockFlush: func(m *mocks.MockFlusher[int]) {
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{4, 5, 6})).Return(fmt.Errorf("flush error")).Times(1)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {
					m.EXPECT().OnError(gomock.Any()).Times(1)
				},
			},
		},
		{
			name:         "drainQueue handles successful queued batches and flushes current",
			initialBatch: []int{7},
			initialQueue: [][]int{{8, 9}, {10}},
			wantBatch:    []int{},
			wantQueueLen: 0,
			expectFlush:  true,
			tuners: tuners{
				mockFlush: func(m *mocks.MockFlusher[int]) {
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{8, 9})).Return(nil).Times(1)
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{10})).Return(nil).Times(1)
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{7})).Return(nil).Times(1)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
		},
		{
			name:         "drainQueue fails on a queued batch, current batch is also requeued",
			initialBatch: []int{11},
			initialQueue: [][]int{{12, 13}, {14}},
			wantBatch:    []int{},
			wantQueueLen: 3,
			expectFlush:  true,
			tuners: tuners{
				mockFlush: func(m *mocks.MockFlusher[int]) {
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{12, 13})).Return(fmt.Errorf("drain error")).Times(1)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {
					m.EXPECT().OnError(gomock.Any()).Times(1)
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			flusher := mocks.NewMockFlusher[int](ctrl)
			tt.tuners.mockFlush(flusher)

			errHandler := mocks.NewMockErrorHandler(ctrl)
			tt.tuners.mockOnError(errHandler)

			s := NewMemory(flusher, WithBatchSize(10), WithOnError(errHandler)).(*memory[int])

			s.batch = tt.initialBatch
			for _, item := range tt.initialQueue {
				s.queue.Enqueue(item)
			}

			s.tryFlush()

			if !reflect.DeepEqual(s.batch, tt.wantBatch) {
				t.Errorf("batch after flush got %v, want %v", s.batch, tt.wantBatch)
			}

			if s.queue.Size() != tt.wantQueueLen {
				t.Errorf("queue length got %d, want %d", s.queue.Size(), tt.wantQueueLen)
			}
		})
	}
}

func TestMemory_flush(t *testing.T) {
	type tuners struct {
		mockFlush   func(m *mocks.MockFlusher[int])
		mockOnError func(m *mocks.MockErrorHandler)
	}

	tests := []struct {
		name      string
		entries   []int
		tuners    tuners
		wantError bool
	}{
		{
			name:    "successful flush",
			entries: []int{1, 2, 3},
			tuners: tuners{
				mockFlush: func(m *mocks.MockFlusher[int]) {
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{1, 2, 3})).Return(nil).Times(1)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
			wantError: false,
		},
		{
			name:    "flush with error",
			entries: []int{1, 2, 3},
			tuners: tuners{
				mockFlush: func(m *mocks.MockFlusher[int]) {
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{1, 2, 3})).Return(fmt.Errorf("mock flush error")).Times(1)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {
					m.EXPECT().OnError(gomock.Any()).Times(1)
				},
			},
			wantError: true,
		},
		{
			name:    "empty entries do not call flusher",
			entries: []int{},
			tuners: tuners{
				mockFlush:   func(m *mocks.MockFlusher[int]) {},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			flusher := mocks.NewMockFlusher[int](ctrl)
			errHandler := mocks.NewMockErrorHandler(ctrl)

			tt.tuners.mockFlush(flusher)
			tt.tuners.mockOnError(errHandler)

			s := NewMemory(flusher, WithOnError(errHandler), WithFlushTimeout(10*time.Millisecond)).(*memory[int])

			err := s.flush(tt.entries)

			if (err != nil) != tt.wantError {
				t.Errorf("flush() error = %v, wantErr %v", err, tt.wantError)
			}
		})
	}
}

func TestMemory_drainQueue(t *testing.T) {
	type tuners struct {
		mockFlush   func(m *mocks.MockFlusher[int])
		mockOnError func(m *mocks.MockErrorHandler)
	}

	tests := []struct {
		name         string
		queueItems   [][]int
		tuners       tuners
		wantError    bool
		wantQueueLen int
	}{
		{
			name:       "successfully drains all queued items",
			queueItems: [][]int{{1, 2}, {3, 4}, {5}},
			tuners: tuners{
				mockFlush: func(m *mocks.MockFlusher[int]) {
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{1, 2})).Return(nil).Times(1)
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{3, 4})).Return(nil).Times(1)
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{5})).Return(nil).Times(1)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
			wantError:    false,
			wantQueueLen: 0,
		},
		{
			name:       "drains partially then fails",
			queueItems: [][]int{{1, 2}, {3, 4}, {5}},
			tuners: tuners{
				mockFlush: func(m *mocks.MockFlusher[int]) {
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{1, 2})).Return(nil).Times(1)
					m.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{3, 4})).Return(fmt.Errorf("mock drain error")).Times(1)
				},
				mockOnError: func(m *mocks.MockErrorHandler) {
					m.EXPECT().OnError(gomock.Any()).Times(1)
				},
			},
			wantError:    true,
			wantQueueLen: 2,
		},
		{
			name:       "empty queue returns without error",
			queueItems: [][]int{},
			tuners: tuners{
				mockFlush:   func(m *mocks.MockFlusher[int]) {},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
			wantError:    false,
			wantQueueLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			flusher := mocks.NewMockFlusher[int](ctrl)
			errHandler := mocks.NewMockErrorHandler(ctrl)

			tt.tuners.mockFlush(flusher)
			tt.tuners.mockOnError(errHandler)

			s := NewMemory(flusher, WithOnError(errHandler)).(*memory[int])

			for _, item := range tt.queueItems {
				s.queue.Enqueue(item)
			}

			err := s.drainQueue()

			if (err != nil) != tt.wantError {
				t.Errorf("drainQueue() error = %v, wantErr %v", err, tt.wantError)
			}

			if s.queue.Size() != tt.wantQueueLen {
				t.Errorf("queue length after partial drain got %d, want %d", s.queue.Size(), tt.wantQueueLen)
			}
		})
	}
}

func TestMemory_requeueAndFlush(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	flusher := mocks.NewMockFlusher[int](ctrl)
	flusher.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{0, 1, 2})).Return(fmt.Errorf("initial flush error")).Times(1)
	flusher.EXPECT().Flush(gomock.Any(), gomock.Eq([]int{0, 1, 2})).Return(nil).Times(1)

	errHandler := mocks.NewMockErrorHandler(ctrl)
	errHandler.EXPECT().OnError(gomock.Any()).Times(1)

	c := quartz.NewMock(t)

	s := NewMemory(flusher, WithBatchSize(3), WithOnError(errHandler), WithClock(c)).(*memory[int])

	for i := 0; i < 3; i++ {
		s.buffer <- i
	}

	s.once(context.Background())
	s.once(context.Background())
	s.once(context.Background())

	if s.queue.Size() != 1 {
		t.Fatalf("Expected one batch in the queue after flush failure, got %d", s.queue.Size())
	}

	c.AdvanceNext()
	s.once(context.Background())

	if s.queue.Size() != 0 {
		t.Errorf("Expected queue to be empty after successful retry, got %d", s.queue.Size())
	}
}
