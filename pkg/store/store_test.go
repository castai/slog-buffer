package store

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/castai/slog-buffer/pkg/store/mocks"
	"github.com/coder/quartz"
	"go.uber.org/mock/gomock"
)

type testEntry struct {
	value string
}

func TestMemory_Write(t *testing.T) {
	type tuners struct {
		mockFlush   func(m *mocks.MockFlusher[int])
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
				mockFlush:   func(flusher *mocks.MockFlusher[int]) {},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
		},
		{
			name:      "should flush exactly once when batch size is reached",
			batchSize: 5,
			entries:   5,
			flushes:   1,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
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
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
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
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
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
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
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
				mockFlush:   func(flusher *mocks.MockFlusher[int]) {},
				mockOnError: func(m *mocks.MockErrorHandler) {},
			},
		},
		{
			name:      "should handle entries larger than batch size in a single call",
			batchSize: 5,
			entries:   6,
			flushes:   1,
			tuners: tuners{
				mockFlush: func(flusher *mocks.MockFlusher[int]) {
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

			flusher := mocks.NewMockFlusher[int](ctrl)
			tt.tuners.mockFlush(flusher)
			errHandler := mocks.NewMockErrorHandler(ctrl)
			tt.tuners.mockOnError(errHandler)
			c := quartz.NewMock(t)
			s := NewMemory(flusher, WithBatchSize(tt.batchSize), WithInterval(1*time.Hour), WithClock(c), WithOnError(errHandler))

			for i := 0; i < tt.entries; i++ {
				s.Write(i)
				s.(*memory[int]).once(t.Context())
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
	type setupFn func(m *memory[int])
	tests := []struct {
		name       string
		entries    []int
		fields     fields
		setup      setupFn
		wantBatch  []int
		wantBuffer int
	}{
		{
			name:       "drain with empty buffer",
			entries:    []int{},
			fields:     fields{batchSize: 5, bufferCap: 5},
			setup:      nil,
			wantBatch:  []int{},
			wantBuffer: 0,
		},
		{
			name:       "drain with single entry",
			entries:    []int{1},
			fields:     fields{batchSize: 5, bufferCap: 5},
			setup:      nil,
			wantBatch:  []int{1},
			wantBuffer: 0,
		},
		{
			name:       "drain with multiple entries",
			entries:    []int{1, 2, 3},
			fields:     fields{batchSize: 5, bufferCap: 5},
			setup:      nil,
			wantBatch:  []int{1, 2, 3},
			wantBuffer: 0,
		},
		{
			name:       "buffer full, drain all",
			entries:    []int{1, 2, 3, 4, 5},
			fields:     fields{batchSize: 5, bufferCap: 5},
			setup:      nil,
			wantBatch:  []int{1, 2, 3, 4, 5},
			wantBuffer: 0,
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
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			flusher := mocks.NewMockFlusher[int](ctrl)
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
