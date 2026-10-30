// cacheload generates reproducible open-loop traffic. Latency starts at the
// scheduled batch arrival, including scheduling and backlog, so an overloaded
// run cannot hide tail latency by sending less work.
package main

import (
	"ahkc"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/bits"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

type histogram struct {
	Buckets                                                [12601]uint64
	MaxNS                                                  int64
	Total, Success, Hits, Misses, Errors, Busy, Overloaded uint64
	FirstError                                             string
}

func (h *histogram) record(ns int64, err error, hit bool) {
	if ns < 0 {
		ns = 0
	}
	if ns > h.MaxNS {
		h.MaxNS = ns
	}
	bucket := ns / 1000
	if bucket > 10001 {
		width := bits.Len64(uint64(ns))
		base := uint64(1) << (width - 1)
		bucket = int64(10002+(width-24)*64) + int64((uint64(ns)-base)>>uint(width-7))
	}
	h.Buckets[bucket]++
	h.Total++
	if err == nil {
		h.Success++
		if hit {
			h.Hits++
		} else {
			h.Misses++
		}
	} else {
		h.Errors++
		if h.FirstError == "" {
			h.FirstError = err.Error()
		}
	}
}

func (h *histogram) percentile(p uint64) int64 {
	if h.Total == 0 {
		return 0
	}
	target := (h.Total*p + 99) / 100
	var n uint64
	for i, count := range h.Buckets {
		n += count
		if n >= target {
			if i > 10001 {
				group, fraction := (i-10002)/64, (i-10002)%64
				base := uint64(1) << uint(group+23)
				upper := base + uint64(fraction+1)*(base/64)
				if upper > uint64(h.MaxNS) {
					return h.MaxNS
				}
				return int64(upper)
			}
			return int64(i+1) * 1000
		}
	}
	return 0
}

type result struct {
	Scenario                                                                 string
	ReadMode                                                                 string
	HotPercent                                                               float64
	GoVersion, OS, Arch                                                      string
	CPUs, GOMAXPROCS, Workers, Keys, Shards                                  int
	Seed                                                                     uint64
	OfferedQPS                                                               int64
	DurationSeconds, ElapsedSeconds, SuccessfulQPS, CompletedQPS             float64
	P50NS, P95NS, P99NS, MaxNS                                               int64
	Requests, Successes, Errors, Busy, Overloaded, SourceLoads, SourceMerged uint64
	Hits, Misses                                                             uint64
	FirstError                                                               string
	Entries                                                                  int64
	ValueMin, ValueMax, HotSlots                                             int
	HotSamplesDropped                                                        uint64
	PinSlotsExhausted, PinEpochRetriesExhausted                              uint64
	LoadDelayNS                                                              int64
	MissPercent, SourcePercent                                               int
	MaxManaged                                                               ahkc.MemoryLimits
	HeapAlloc, HeapInUse, TotalAllocBytes                                    uint64
	GCCycles                                                                 uint32
	Passed                                                                   bool
}

func arrivalsThrough(ticks, rate int64, worker, workers int) int64 {
	return (ticks*rate/1000 + int64(workers-1-worker)) / int64(workers)
}

// readHot includes value access and the complete read scope in latency.
func readHot(ns *ahkc.Namespace[string], key, mode string) (bool, error, uint64) {
	var request ahkc.Request[string]
	var borrowed ahkc.Borrowed[string]
	if err := ns.BeginRequestOn(&request, key); err != nil {
		return false, err, 0
	}
	var busy uint64
	for attempt := 0; attempt < 64; attempt++ {
		var hit bool
		var err error
		switch mode {
		case "value":
			var value string
			value, hit, err = ns.GetOn(&request)
			runtime.KeepAlive(value)
		case "owned":
			var handle *ahkc.Handle[string]
			handle, hit, err = ns.GetOwnedOn(&request)
			if handle != nil {
				runtime.KeepAlive(handle.Value())
				handle.Release()
			}
		default:
			hit, err = ns.BorrowOn(&request, &borrowed)
			if hit {
				runtime.KeepAlive(borrowed.Value())
				borrowed.Release()
			}
		}
		if !errors.Is(err, ahkc.ErrBusy) {
			return hit, err, busy
		}
		busy++
		runtime.Gosched()
	}
	return false, ahkc.ErrBusy, busy
}

func main() {
	rate := flag.Int64("qps", 1_100_000, "offered requests/second (millisecond batches)")
	duration := flag.Duration("duration", 20*time.Second, "measurement duration; accepts e.g. 72h")
	workers := flag.Int("workers", 256, "independent client workers")
	keys := flag.Int("keys", 4096, "resident hot key count")
	shards := flag.Int("shards", 64, "cache shard count")
	scenario := flag.String("scenario", "mixed", "hot or mixed")
	readMode := flag.String("read-mode", "borrow", "borrow, value or owned; release included")
	hotPercent := flag.Float64("hot-percent", 0, "percentage of hot reads directed to one key; 0 is uniform")
	missPercent := flag.Int("miss-percent", 3, "confirmed cold Get misses in mixed traffic")
	sourcePercent := flag.Int("source-percent", 2, "uncached Fetch calls in mixed traffic")
	loadDelay := flag.Duration("loader-delay", 500*time.Microsecond, "simulated source wall time")
	seed := flag.Uint64("seed", 20261001, "reproducible traffic seed")
	minimum := flag.Float64("min-qps", 1_000_000, "minimum successful throughput")
	maximum := flag.Duration("max-p99", 3*time.Millisecond, "maximum full scheduled-to-completion P99")
	output := flag.String("output", "", "optional JSON report path")
	entries := flag.Int64("entries", 0, "cache capacity in entries; 0 means keys*4")
	valueMin := flag.Int("value-min", 0, "minimum loader value bytes; 0 keeps the short value:<key> payload")
	valueMax := flag.Int("value-max", 0, "maximum loader value bytes (uniform in [value-min,value-max])")
	hotSlots := flag.Int("hot-slots", 0, "per-shard request hot-key tracking slots (1s history windows, sampled at 1/16); 0 disables")
	prefault := flag.Bool("prefault", false, "touch index and gate memory at startup")
	hugePages := flag.Bool("hugepages", false, "advise transparent huge pages for the reserved regions")
	flag.Parse()
	if *rate <= 0 || *rate > 1_000_000_000 || *workers <= 0 || *workers > 4096 || *keys <= 0 || *keys > 1<<20 || *shards < 1 || *shards > 65536 || *duration < time.Millisecond || *duration > 7*24*time.Hour ||
		(*scenario != "hot" && *scenario != "mixed") || *missPercent < 0 || *sourcePercent < 0 ||
		*missPercent+*sourcePercent > 100 || *loadDelay < 0 ||
		(*readMode != "borrow" && *readMode != "value" && *readMode != "owned") || math.IsNaN(*hotPercent) || *hotPercent < 0 || *hotPercent > 100 ||
		math.IsNaN(*minimum) || math.IsInf(*minimum, 0) || *minimum <= 0 || *maximum <= 0 ||
		*entries < 0 || *valueMin < 0 || *valueMax < *valueMin || *valueMax > 1<<20 || *hotSlots < 0 {
		fmt.Fprintln(os.Stderr, "invalid load configuration")
		os.Exit(2)
	}
	capacity, entryBytes, perEntry := int64(*keys)*4, int64(512), int64(2048)/4
	if *entries > 0 {
		capacity = *entries
	}
	if *valueMax > 0 {
		entryBytes = int64(*valueMax) + 512
		perEntry = int64(*valueMin+*valueMax)/2 + 256
	}
	cfg := ahkc.Config{
		Shards:         *shards,
		MaxEntries:     capacity,
		MaxBytes:       capacity * perEntry,
		MaxEntryBytes:  entryBytes,
		MaxResultBytes: entryBytes,
		Namespaces:     map[string]ahkc.NamespaceConfig{"load": {TTL: *duration + time.Hour, Tin: 1}},
		Memory: ahkc.MemoryConfig{
			Prefault:  *prefault,
			HugePages: *hugePages,
		},
		Limits: ahkc.LimitsConfig{
			MaxConcurrentLoads: 1024,
			MaxWaitersTotal:    4096,
			FillQueueSize:      128,
		},
	}
	if *hotSlots > 0 {
		cfg.Hot.RequestSlots, cfg.Hot.RequestSampleP = *hotSlots, 1.0/16
		cfg.Hot.Windows, cfg.Hot.Interval = 4, time.Second
	}
	cache, err := ahkc.NewBytes[string](cfg, ahkc.WithResultMetadata[string](func(key, _ string) ahkc.ResultMetadata {
		return ahkc.ResultMetadata{NoCache: strings.HasPrefix(key, "source/")}
	}))
	if err != nil {
		fatal(err)
	}
	ns, _ := cache.Namespace("load")
	hot := make([]string, *keys)
	cold := make([]string, *keys)
	source := make([]string, *keys)
	loader := func(_ context.Context, key string) (string, error) {
		if strings.HasPrefix(key, "source/") && *loadDelay > 0 {
			time.Sleep(*loadDelay)
		}
		if *valueMax > 0 { // 确定性变长值：长度由 key 决定，便于核对
			n := *valueMin + int(next(uint64(len(key))*0x9e3779b97f4a7c15+uint64(key[len(key)-1]))%uint64(*valueMax-*valueMin+1))
			return strings.Repeat(key+"|", n/(len(key)+1)+1)[:n], nil
		}
		return "value:" + key, nil
	}
	ctx := context.Background()
	for i := range hot {
		hot[i], cold[i], source[i] = fmt.Sprintf("hot/%d", i), fmt.Sprintf("cold/%d", i), fmt.Sprintf("source/%d", i)
	}
	// Revisit the whole working set so early keys stay hot while later fills
	// publish. Pace retries and allow a complete 2*ValueDelta fault recovery;
	// spinning on one unpublished key can itself cause sample contention.
	warmDeadline := time.Now().Add(time.Minute)
	for {
		ready := true
		for _, key := range hot {
			_, hit, err := ns.Get(key)
			if err != nil && !retryable(err) {
				fatal(err)
			}
			if err == nil && hit {
				continue
			}
			ready = false
			if _, err := ns.Fetch(ctx, key, loader); err != nil && !retryable(err) {
				fatal(err)
			}
		}
		if ready {
			break
		}
		if time.Now().After(warmDeadline) {
			fatal(fmt.Errorf("warmup did not publish all keys: %+v", cache.Stats()))
		}
		time.Sleep(10 * time.Millisecond)
	}
	runtime.GC()
	before := cache.Stats()
	var memoryBefore runtime.MemStats
	runtime.ReadMemStats(&memoryBefore)
	histograms := make([]histogram, *workers)
	start := time.Now().Add(50 * time.Millisecond)
	ticks := int64(*duration / time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(*workers)
	for worker := 0; worker < *workers; worker++ {
		go func(id int) {
			defer wg.Done()
			h := &histograms[id]
			random := *seed ^ uint64(id+1)*0x9e3779b97f4a7c15
			var sent int64
			for tick := int64(0); tick < ticks; tick++ {
				arrival := start.Add(time.Duration(tick) * time.Millisecond)
				if wait := time.Until(arrival); wait > 0 {
					time.Sleep(wait)
				}
				// Difference of cumulative counts gives exactly the configured rate,
				// even when it is not divisible by workers or 1000.
				end := arrivalsThrough(tick+1, *rate, id, *workers)
				for ; sent < end; sent++ {
					random = next(random)
					keyID := int(random % uint64(len(hot)))
					kind := int((random >> 32) % 100)
					var err error
					var hit bool
					if *scenario == "mixed" && kind < *sourcePercent {
						var request ahkc.Request[string]
						err = ns.BeginRequestOn(&request, source[keyID])
						if err == nil {
							for attempt := 0; attempt < 512; attempt++ {
								_, err = ns.FetchOn(ctx, &request, loader)
								if errors.Is(err, ahkc.ErrBusy) {
									h.Busy++
								}
								if errors.Is(err, ahkc.ErrOverloaded) {
									h.Overloaded++
								}
								if !retryable(err) || time.Since(arrival) > 10*time.Millisecond {
									break
								}
								runtime.Gosched()
							}
						}
					} else if *scenario == "mixed" && kind < *sourcePercent+*missPercent {
						_, hit, err = ns.Get(cold[keyID])
					} else {
						if *hotPercent > 0 && random%1_000_000 < uint64(*hotPercent*10_000) {
							keyID = 0
						}
						var busy uint64
						hit, err, busy = readHot(ns, hot[keyID], *readMode)
						h.Busy += busy
						if err == nil && !hit {
							err = fmt.Errorf("resident hot key missed: %s", hot[keyID])
						}
					}
					h.record(time.Since(arrival).Nanoseconds(), err, hit)
				}
			}
		}(worker)
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	combined := histogram{}
	for _, h := range histograms {
		for i, count := range h.Buckets {
			combined.Buckets[i] += count
		}
		combined.Total += h.Total
		combined.Success += h.Success
		combined.Hits += h.Hits
		combined.Misses += h.Misses
		combined.Errors += h.Errors
		combined.Busy += h.Busy
		combined.Overloaded += h.Overloaded
		if combined.FirstError == "" {
			combined.FirstError = h.FirstError
		}
		if h.MaxNS > combined.MaxNS {
			combined.MaxNS = h.MaxNS
		}
	}
	var memoryAfter runtime.MemStats
	runtime.ReadMemStats(&memoryAfter)
	after := cache.Stats()
	r := result{
		Scenario: *scenario, ReadMode: *readMode, HotPercent: *hotPercent, GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		CPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), Workers: *workers, Keys: *keys, Shards: 1 << bits.Len(uint(*shards-1)), Seed: *seed,
		OfferedQPS: *rate, DurationSeconds: float64(ticks) / 1000, ElapsedSeconds: elapsed,
		SuccessfulQPS: float64(combined.Success) / elapsed, CompletedQPS: float64(combined.Total) / elapsed,
		P50NS: combined.percentile(50), P95NS: combined.percentile(95), P99NS: combined.percentile(99), MaxNS: combined.MaxNS,
		Requests: combined.Total, Successes: combined.Success, Errors: combined.Errors, Busy: combined.Busy, Overloaded: combined.Overloaded,
		Hits: combined.Hits, Misses: combined.Misses, FirstError: combined.FirstError,
		Entries: capacity, ValueMin: *valueMin, ValueMax: *valueMax, HotSlots: *hotSlots,
		HotSamplesDropped: after.HotRequestSamplesDropped - before.HotRequestSamplesDropped,
		PinSlotsExhausted: after.PinSlotsExhausted - before.PinSlotsExhausted, PinEpochRetriesExhausted: after.PinEpochRetriesExhausted - before.PinEpochRetriesExhausted,
		SourceLoads: after.Loads - before.Loads, SourceMerged: after.LoadMerged - before.LoadMerged,
		LoadDelayNS: int64(*loadDelay), MissPercent: *missPercent, SourcePercent: *sourcePercent, MaxManaged: cache.MemoryLimits(),
		HeapAlloc: memoryAfter.HeapAlloc, HeapInUse: memoryAfter.HeapInuse, TotalAllocBytes: memoryAfter.TotalAlloc - memoryBefore.TotalAlloc,
		GCCycles: memoryAfter.NumGC - memoryBefore.NumGC,
	}
	r.Passed = r.SuccessfulQPS >= *minimum && r.P99NS <= int64(*maximum) && r.Errors == 0
	closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cache.Close(closeCtx); err != nil {
		fatal(err)
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		fatal(err)
	}
	if *output != "" {
		if err := os.WriteFile(*output, append(data, '\n'), 0644); err != nil {
			fatal(err)
		}
	}
	fmt.Println(string(data))
	if !r.Passed {
		os.Exit(1)
	}
}

func next(x uint64) uint64 { x ^= x << 13; x ^= x >> 7; return x ^ (x << 17) }
func retryable(err error) bool {
	return errors.Is(err, ahkc.ErrBusy) || errors.Is(err, ahkc.ErrOverloaded)
}
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(2) }
