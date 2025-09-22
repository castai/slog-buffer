package handler

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/castai/slog-buffer/pkg/store/mocks"
)

func tr(r slog.Record, a []slog.Attr) (string, error) {
	return "", nil
}

func TestNewBuffered(t *testing.T) {
	tests := []struct {
		name      string
		options   []Option
		wantLvl   slog.Level
		tuneStore func(s *mocks.MockStore[string])
	}{
		{
			name:    "default config",
			options: nil,
			wantLvl: defaultLevel,
			tuneStore: func(s *mocks.MockStore[string]) {
			},
		},
		{
			name:    "custom level",
			options: []Option{WithLevel(slog.LevelError)},
			wantLvl: slog.LevelError,
			tuneStore: func(s *mocks.MockStore[string]) {
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			st := mocks.NewMockStore[string](ctrl)
			tt.tuneStore(st)
			h := NewBuffered[string](tr, st, tt.options...)
			gotLevel := defaultLevel
			if h != nil && h.cfg != nil {
				gotLevel = h.cfg.Level
			}
			if gotLevel != tt.wantLvl {
				t.Errorf("cfg.Level got %v, want %v", gotLevel, tt.wantLvl)
			}
		})
	}
}

func TestBuffered_Enabled(t *testing.T) {
	tests := []struct {
		name  string
		level slog.Level
		want  bool
	}{
		{
			name:  "always enabled",
			level: slog.LevelInfo,
			want:  true,
		},
		{
			name:  "enabled at error level",
			level: slog.LevelError,
			want:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			st := mocks.NewMockStore[string](ctrl)
			h := NewBuffered(tr, st, WithLevel(tt.level))

			if got := h.Enabled(context.Background(), tt.level); got != tt.want {
				t.Errorf("Enabled() got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuffered_Handle(t *testing.T) {
	type args struct {
		transform func(slog.Record, []slog.Attr) (string, error)
		record    slog.Record
	}
	tests := []struct {
		name      string
		args      args
		wantErr   bool
		wantData  []string
		tuneStore func(s *mocks.MockStore[string])
	}{
		{
			name: "successful handle",
			args: args{
				transform: func(r slog.Record, a []slog.Attr) (string, error) { return "ok", nil },
				record:    slog.Record{},
			},
			wantErr:  false,
			wantData: []string{"ok"},
			tuneStore: func(s *mocks.MockStore[string]) {
				s.EXPECT().Write(gomock.Any())
			},
		},
		{
			name: "transform error",
			args: args{
				transform: func(r slog.Record, a []slog.Attr) (string, error) { return "", errors.New("err") },
				record:    slog.Record{},
			},
			wantErr:  true,
			wantData: nil,
			tuneStore: func(s *mocks.MockStore[string]) {
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			st := mocks.NewMockStore[string](ctrl)
			tt.tuneStore(st)

			h := NewBuffered(tr, st)
			h.transform = tt.args.transform
			err := h.Handle(context.Background(), tt.args.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("Handle() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBuffered_WithAttrs(t *testing.T) {
	attrs := []slog.Attr{slog.String("foo", "bar")}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	st := mocks.NewMockStore[string](ctrl)
	h := NewBuffered(tr, st)
	h.attrs = []slog.Attr{slog.String("baz", "qux")}
	ret := h.WithAttrs(attrs)
	h2 := ret.(*Buffered[string])

	wantAttrs := append(h.attrs, attrs...)
	if !reflect.DeepEqual(h2.attrs, wantAttrs) {
		t.Errorf("WithAttrs attrs got %v, want %v", h2.attrs, wantAttrs)
	}
}
