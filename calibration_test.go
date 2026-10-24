package ahkc

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"
)

// Calibrate regular arrival / fixed-duration cells with the actual bounded
// singleflight API and real loader invocations. Manual time makes overlaps
// deterministic; the loader remains running until its release event.
func TestNoCacheSingleflightCalibration(t *testing.T) {
	type cell struct {
		Lambda                     float64
		LoadDuration               time.Duration
		Requests, Sources, Merged  int
		MergeFactor, AvoidableRate float64
	}
	var cells []cell
	var bands []RateBand
	for _, lambda := range []int{10, 100, 1000} {
		for _, duration := range []time.Duration{time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond} {
			env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, SampleP: 1e-12}, func(c *Config) {
				c.ObserveOnly, c.ObserveHotKeys = true, -1
				c.ShardCount, c.FlightStripes, c.FlightsPerStripe = 1, 2, 8
				c.MaxConcurrentLoads, c.MaxWaitersPerKey, c.MaxWaitersTotal = 4, 256, 512
				c.MaxEntryBytes, c.MaxResultBytes, c.RetiredRecords = 1024, 1024, 32
				c.GateWidth, c.ValueWidth, c.ReaderSlotsPerShard = 64, 16, 8
			})
			period := time.Second / time.Duration(lambda)
			perSource := int(ceilDiv(int64(duration), int64(period)))
			cycle := period * time.Duration(perSource)
			start := env.clk.Now()
			const rounds = 20
			requests := 0
			for round := 0; round < rounds; round++ {
				at := start + int64(round)*int64(cycle)
				env.clk.Set(at)
				release, started, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				loader := func(context.Context, string) (string, error) { close(started); <-release; return "v", nil }
				f, err := env.ns.FetchAsync("calibrate", loader)
				if err != nil {
					t.Fatal(err)
				}
				waiters := []*Future[string]{f}
				requests++
				go func() { defer close(done); env.ex.Drain(2000) }()
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("source dispatch")
				}
				for request := 1; request < perSource; request++ {
					env.clk.Set(at + int64(request)*int64(period))
					f, err := env.ns.FetchAsync("calibrate", loader)
					if err != nil {
						t.Fatal(err)
					}
					waiters = append(waiters, f)
					requests++
				}
				env.clk.Set(at + int64(duration))
				close(release)
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("source settlement")
				}
				for _, f := range waiters {
					if value, ready, err := f.Take(); !ready || err != nil || value != "v" {
						t.Fatalf("response: %q %v %v", value, err, ready)
					}
				}
			}
			stats := env.c.Stats()
			if int(stats.Loads) != rounds || int(stats.LoadMerged) != requests-rounds || stats.ResidentEntries != 0 {
				t.Fatalf("calibration invariant: %+v", stats)
			}
			factor := float64(stats.Loads) / float64(requests)
			cells = append(cells, cell{float64(lambda), duration, requests, int(stats.Loads), int(stats.LoadMerged), factor, float64(stats.Loads) / (cycle.Seconds() * rounds)})
			bands = append(bands, RateBand{CostClass: 0, MaxLambda: float64(lambda), MaxLoadDuration: duration, MergeFactor: factor})
			closeManual(t, env.c, env.ex)
		}
	}
	clock := NewManualClock(0)
	m := newCostTable(t, CostTableConfig{Clock: clock, Classes: 1, Alpha: 1, RateBands: bands, DisableAutoObserve: true})
	for _, cell := range cells {
		m.ObserveLoad(0, 1, cell.LoadDuration)
		r, _, _, _, _, ok := m.Lookup(cell.Lambda, 0)
		if !ok || math.Abs(r-cell.AvoidableRate) > 1e-9 {
			t.Fatalf("calibrated cell: %+v rate=%v ok=%v", cell, r, ok)
		}
	}
	if path := os.Getenv("AHKC_CALIBRATION_REPORT"); path != "" {
		report := struct {
			Scope     string
			Cells     []cell
			RateBands []RateBand
		}{"constructed regular arrivals, actual loader/singleflight, no cache, deterministic manual clock; grid points validated, not a production backend calibration", cells, bands}
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
}
