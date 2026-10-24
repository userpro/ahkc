package main

import "testing"

func TestArrivalAccounting(t *testing.T) {
	for _, rate := range []int64{1, 7, 999, 1001, 1_100_000} {
		for _, workers := range []int{1, 7, 256} {
			for _, ticks := range []int64{1, 999, 1000, 12345} {
				var total int64
				for worker := 0; worker < workers; worker++ {
					total += arrivalsThrough(ticks, rate, worker, workers)
				}
				if total != ticks*rate/1000 {
					t.Fatalf("rate=%d workers=%d ticks=%d: %d arrivals", rate, workers, ticks, total)
				}
			}
		}
	}
}

func TestLatencyHistogram(t *testing.T) {
	if (&histogram{}).percentile(99) != 0 {
		t.Fatal("empty histogram has a latency")
	}
	for _, ns := range []int64{0, 1, 999, 1000, 1_250_000, 10_001_000, 10_002_000, 100_000_000, 1_000_000_000, 1 << 62} {
		h := &histogram{}
		for i := 0; i < 99; i++ {
			h.record(ns, nil, true)
		}
		h.record(ns+1_000_000_000, nil, false)
		got := h.percentile(99)
		if got < ns || got > ns+max(int64(1000), ns/64+1) {
			t.Fatalf("latency=%d percentile=%d", ns, got)
		}
	}
}
