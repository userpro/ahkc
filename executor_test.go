package ahkc

import (
	"slices"
	"testing"
)

func TestManualExecutorWrapGrowthAndReentrantSubmit(t *testing.T) {
	e := NewManualExecutor()
	if e.TrySubmit(nil) || e.Pending() != 0 {
		t.Fatal("nil task was accepted")
	}
	var got []int
	for i := 0; i < 4; i++ {
		e.TrySubmit(func() { got = append(got, i) })
	}
	if e.Drain(2) != 2 || e.Pending() != 2 {
		t.Fatal("partial drain lost tasks")
	}
	for i := 4; i < 9; i++ {
		e.TrySubmit(func() { got = append(got, i) })
	}
	e.TrySubmit(func() { got = append(got, 9); e.TrySubmit(func() { got = append(got, 10) }) })
	e.Drain(100)
	if !slices.Equal(got, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}) || e.Pending() != 0 {
		t.Fatal("FIFO order changed", got)
	}
	for _, f := range e.queue {
		if f != nil {
			t.Fatal("queue retained a completed task")
		}
	}
}

func TestManualExecutorSteadyQueueZeroAlloc(t *testing.T) {
	e := NewManualExecutor()
	task := func() {}
	e.TrySubmit(task)
	e.Drain(1)
	if a := testing.AllocsPerRun(1000, func() { e.TrySubmit(task); e.Drain(1) }); a != 0 {
		t.Fatalf("queue allocated %.0f objects per task", a)
	}
}
