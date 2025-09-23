package store

import (
	"testing"
)

func TestRingQueue(t *testing.T) {
	t.Parallel()
	type testCase struct {
		name    string
		size    int
		actions []struct {
			actionType string
			value      int
			expected   int
			expectOK   bool
		}
	}

	tests := []testCase{
		{
			name: "basic enqueue/dequeue sequence",
			size: 3,
			actions: []struct {
				actionType string
				value      int
				expected   int
				expectOK   bool
			}{
				{"enqueue", 1, 0, false},
				{"enqueue", 2, 0, false},
				{"peek", 0, 1, true},
				{"dequeue", 0, 1, true},
				{"dequeue", 0, 2, true},
				{"dequeue", 0, 0, false},
				{"peek", 0, 0, false},
			},
		},
		{
			name: "full queue and overwrite behavior",
			size: 2,
			actions: []struct {
				actionType string
				value      int
				expected   int
				expectOK   bool
			}{
				{"enqueue", 10, 0, false},
				{"enqueue", 20, 0, false},
				{"enqueue", 30, 0, false},
				{"peek", 0, 20, true},
				{"dequeue", 0, 20, true},
				{"dequeue", 0, 30, true},
				{"dequeue", 0, 0, false},
			},
		},
		{
			name: "queue wrap-around and overwrite",
			size: 3,
			actions: []struct {
				actionType string
				value      int
				expected   int
				expectOK   bool
			}{
				{"enqueue", 1, 0, false},
				{"enqueue", 2, 0, false},
				{"enqueue", 0, 1, true},
				{"enqueue", 3, 0, false},
				{"enqueue", 4, 0, false},
				{"enqueue", 5, 0, false},
				{"dequeue", 0, 3, true},
				{"dequeue", 0, 4, true},
				{"dequeue", 0, 5, true},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q := NewRingQueue[int](tc.size)

			for i, action := range tc.actions {
				switch action.actionType {
				case "enqueue":
					q.Enqueue(action.value)
				case "dequeue":
					actual, ok := q.Dequeue()
					if ok != action.expectOK {
						t.Errorf("action %d (%s): expected ok=%v, got %v", i, action.actionType, action.expectOK, ok)
					}
					if ok && actual != action.expected {
						t.Errorf("action %d (%s): expected %v, got %v", i, action.actionType, action.expected, actual)
					}
				case "peek":
					actual, ok := q.Peek()
					if ok != action.expectOK {
						t.Errorf("action %d (%s): expected ok=%v, got %v", i, action.actionType, action.expectOK, ok)
					}
					if ok && actual != action.expected {
						t.Errorf("action %d (%s): expected %v, got %v", i, action.actionType, action.expected, actual)
					}
				default:
					t.Fatalf("unknown action type: %s", action.actionType)
				}
			}
		})
	}

	t.Run("Panic on size <= 0", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("NewRingQueue did not panic with a non-positive size")
			}
		}()
		NewRingQueue[int](0)
	})
}
