package ahkc

// Small reference policy models for reproducible algorithm comparisons. These
// are sequential simulators, not implementations for serving requests. The
// report explicitly separates logical resident capacity from total metadata,
// service CPU/latency, prediction error and bounded-search regret.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

const replayCount = 24
const replayBytes = 64 << 10
const replayDelta = int64(20 * time.Millisecond)

var replaySizes = [4]int{32, 512, 4096, 16384}
var replayCosts = [4]float64{1, 4, 16, 64}

type replayEvent struct{ id, invalidate int }
type replayResult struct {
	Scenario, Policy, TraceSHA256                                        string
	Requests, Hits, Loads, Published, PredictionChecks, PriorPredictions int
	AvoidedCost, TotalCost, PredictionAbsError, FutureCost               float64
	PeakCount                                                            int
	PeakBytes                                                            int64
}
type replayHeat struct {
	era       int64
	cur, prev float64
}

func (h *replayHeat) normalize(now int64) {
	era := now / replayDelta
	if era == h.era+1 {
		h.prev, h.cur, h.era = h.cur, 0, era
	} else if era > h.era+1 {
		*h = replayHeat{era: era}
	}
}
func (h *replayHeat) add(now int64) { h.normalize(now); h.cur = min(64, h.cur+1) }
func (h *replayHeat) value(now int64) float64 {
	h.normalize(now)
	return h.cur + (1-float64(now%replayDelta)/float64(replayDelta))*h.prev
}

type replayEntry struct {
	id, region, freq     int
	bytes                int64
	deadline, grace, low int64
	visited              bool
	heat                 replayHeat
}

// A bounded TinyLFU estimator: four rows of four-bit counters, a two-hash
// Bloom doorkeeper and request-count aging. Every request updates it.
type replayTiny struct {
	counts  [4][512]uint8
	door    [32]uint64
	samples int
}

func replayTinyHash(id int, gen uint64, row int) uint64 {
	return mix64(uint64(id) ^ gen*0x9e3779b97f4a7c15 ^ uint64(row+1)*0xbf58476d1ce4e5b9)
}
func (f *replayTiny) seen(id int, gen uint64) bool {
	for row := 0; row < 2; row++ {
		bit := replayTinyHash(id, gen, row) % 2048
		if f.door[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}
func (f *replayTiny) frequency(id int, gen uint64) int {
	n := uint8(15)
	for row := range f.counts {
		n = min(n, f.counts[row][replayTinyHash(id, gen, row)%512])
	}
	if f.seen(id, gen) {
		return int(n) + 1
	}
	return int(n)
}
func (f *replayTiny) add(id int, gen uint64) {
	if f.seen(id, gen) {
		n := uint8(f.frequency(id, gen) - 1)
		for row := range f.counts {
			at := replayTinyHash(id, gen, row) % 512
			if f.counts[row][at] == n && n < 15 {
				f.counts[row][at]++
			}
		}
	} else {
		for row := 0; row < 2; row++ {
			bit := replayTinyHash(id, gen, row) % 2048
			f.door[bit/64] |= 1 << (bit % 64)
		}
	}
	f.samples++
	if f.samples == replayCount*10 {
		for row := range f.counts {
			for at := range f.counts[row] {
				f.counts[row][at] >>= 1
			}
		}
		clear(f.door[:])
		f.samples /= 2
	}
}

type replayPolicy struct {
	kind    string
	entries map[int]*replayEntry
	queues  [3][]int // oldest first: FIFO/window, main probation, protected
	ghost   []int
	gate    [256]replayHeat
	gen     [2]uint64
	tiny    replayTiny
	bytes   int64
	hand    int
	ttl     int64
}

func removeReplayID(q []int, id int) []int {
	if at := slices.Index(q, id); at >= 0 {
		copy(q[at:], q[at+1:])
		q[len(q)-1] = 0
		q = q[:len(q)-1]
	}
	return q
}
func (p *replayPolicy) remove(id int) {
	e := p.entries[id]
	if e == nil {
		return
	}
	if e.region == 0 && p.kind == "SIEVE" {
		if at := slices.Index(p.queues[0], id); at < p.hand {
			p.hand--
		}
	}
	p.queues[e.region] = removeReplayID(p.queues[e.region], id)
	delete(p.entries, id)
	p.bytes -= e.bytes
	if p.hand >= len(p.queues[0]) {
		p.hand = 0
	}
}
func (p *replayPolicy) move(e *replayEntry, region int) {
	p.queues[e.region] = removeReplayID(p.queues[e.region], e.id)
	e.region = region
	p.queues[region] = append(p.queues[region], e.id)
}
func (p *replayPolicy) regionBytes(region int) (n int64) {
	for _, id := range p.queues[region] {
		n += p.entries[id].bytes
	}
	return
}
func (p *replayPolicy) insert(e *replayEntry, region int) {
	e.region = region
	p.entries[e.id] = e
	p.queues[region] = append(p.queues[region], e.id)
	p.bytes += e.bytes
}
func (p *replayPolicy) promoteWT(e *replayEntry) {
	if e.region == 0 {
		p.move(e, 0)
		return
	}
	p.move(e, 2)
	for len(p.queues[2]) > (replayCount-1)*4/5 || p.regionBytes(2) > (replayBytes-replayBytes/100)*4/5 {
		p.move(p.entries[p.queues[2][0]], 1)
	}
}
func (p *replayPolicy) hit(e *replayEntry, now int64) {
	e.heat.add(now)
	switch p.kind {
	case "LRU":
		p.move(e, 0)
	case "SIEVE":
		e.visited = true
	case "S3-FIFO":
		e.freq = min(3, e.freq+1)
	case "W-TinyLFU-AV":
		p.promoteWT(e)
	}
}
func (p *replayPolicy) evictS3Main() {
	for len(p.queues[1]) > 0 {
		e := p.entries[p.queues[1][0]]
		if e.freq == 0 {
			p.remove(e.id)
			return
		}
		e.freq--
		p.move(e, 1)
	}
}
func (p *replayPolicy) victim() {
	switch p.kind {
	case "LRU":
		p.remove(p.queues[0][0])
	case "SIEVE":
		for {
			at := p.hand % len(p.queues[0])
			e := p.entries[p.queues[0][at]]
			if !e.visited {
				p.remove(e.id)
				return
			}
			e.visited = false
			p.hand = (at + 1) % len(p.queues[0])
		}
	case "S3-FIFO":
		if len(p.queues[0]) > 0 && (len(p.queues[0]) >= max(1, replayCount/10) || p.regionBytes(0) >= replayBytes/10 || len(p.queues[1]) == 0) {
			for len(p.queues[0]) > 0 {
				e := p.entries[p.queues[0][0]]
				if e.freq > 1 {
					e.freq = 0
					p.move(e, 1)
					if len(p.queues[1]) > replayCount-replayCount/10 || p.regionBytes(1) > replayBytes-replayBytes/10 {
						p.evictS3Main()
						return
					}
				} else {
					p.remove(e.id)
					p.ghost = append(p.ghost, e.id)
					if len(p.ghost) > replayCount-replayCount/10 {
						p.ghost = p.ghost[1:]
					}
					return
				}
			}
		}
		p.evictS3Main()
	}
}
func (p *replayPolicy) admitWT(e *replayEntry) bool {
	mainBytes, mainCount := int64(replayBytes-replayBytes/100), replayCount-1
	if e.bytes > mainBytes {
		return false
	}
	order := append(slices.Clone(p.queues[1]), p.queues[2]...)
	var victims []*replayEntry
	var released int64
	loss := 0
	n := len(order)
	b := p.regionBytes(1) + p.regionBytes(2)
	for _, id := range order {
		if n+1-len(victims) <= mainCount && b+e.bytes-released <= mainBytes {
			break
		}
		v := p.entries[id]
		victims = append(victims, v)
		released += v.bytes
		loss += p.tiny.frequency(id, p.gen[id%2])
		if loss > p.tiny.frequency(e.id, p.gen[e.id%2]) {
			break
		}
	}
	if n+1-len(victims) > mainCount || b+e.bytes-released > mainBytes || loss > p.tiny.frequency(e.id, p.gen[e.id%2]) {
		for _, v := range victims {
			p.promoteWT(v)
		}
		return false
	}
	for _, v := range victims {
		p.remove(v.id)
	}
	p.insert(e, 1)
	return true
}
func (p *replayPolicy) admit(e *replayEntry) {
	if p.kind == "W-TinyLFU-AV" {
		if e.bytes > replayBytes/100 {
			p.admitWT(e)
			return
		}
		p.insert(e, 0)
		for len(p.queues[0]) > 1 || p.regionBytes(0) > replayBytes/100 {
			v := p.entries[p.queues[0][0]]
			p.remove(v.id)
			p.admitWT(v)
		}
		return
	}
	for len(p.entries)+1 > replayCount || p.bytes+e.bytes > replayBytes {
		p.victim()
	}
	region := 0
	if p.kind == "S3-FIFO" && slices.Contains(p.ghost, e.id) {
		region = 1
		p.ghost = removeReplayID(p.ghost, e.id)
	}
	p.insert(e, region)
}
func (p *replayPolicy) maintain(now int64) {
	for _, e := range p.entries {
		if now >= e.deadline {
			p.remove(e.id)
			continue
		}
		if now < e.grace {
			continue
		}
		if e.heat.value(now) >= 1 {
			e.low = -1
		} else if e.low < 0 {
			e.low = now
		} else if now-e.low >= replayDelta {
			p.remove(e.id)
		}
	}
}

func makeReplayTrace(name string, count int) ([]replayEvent, string) {
	rng := rand.New(rand.NewSource(20261001))
	zipf := rand.NewZipf(rng, 1.15, 1, 255)
	events := make([]replayEvent, count)
	h := sha256.New()
	for i := range events {
		id := int(zipf.Uint64())
		switch name {
		case "scan":
			if i%5 == 0 {
				id = 64 + (i/5)%192
			}
		case "hot-shift":
			id = int(zipf.Uint64()%64) + (i/(max(1, count/4))%4)*64
		case "uniform-size-cost":
			id = rng.Intn(256)
		case "namespace-invalidation":
			id = int(zipf.Uint64() % 64)
		}
		invalidate := -1
		if name == "namespace-invalidation" && i > 0 && i%997 == 0 {
			invalidate = (i / 997) % 2
		}
		events[i] = replayEvent{id, invalidate}
		var raw [16]byte
		binary.LittleEndian.PutUint64(raw[:8], uint64(id))
		binary.LittleEndian.PutUint64(raw[8:], uint64(invalidate+1))
		h.Write(raw[:])
	}
	return events, hex.EncodeToString(h.Sum(nil))
}
func replayTTL(name string) int64 {
	if name == "short-ttl" {
		return int64(20 * time.Millisecond)
	}
	return int64(2 * time.Second)
}
func replayKey(id int) string   { return fmt.Sprintf("k%03d", id/2) }
func replayValue(id int) string { return strings.Repeat(string(byte('a'+id%4)), replaySizes[id%4]) }

func runReferenceReplay(t *testing.T, name, kind, digest string, events []replayEvent) replayResult {
	t.Helper()
	p := replayPolicy{kind: kind, entries: make(map[int]*replayEntry), ttl: replayTTL(name)}
	r := replayResult{Scenario: name, Policy: kind, TraceSHA256: digest, Requests: len(events)}
	for i, event := range events {
		now := int64(time.Second) + int64(i)*int64(100*time.Microsecond)
		if i%32 == 0 {
			p.maintain(now)
		}
		if event.invalidate >= 0 {
			for id := range p.entries {
				if id%2 == event.invalidate {
					p.remove(id)
				}
			}
			p.gen[event.invalidate]++
		}
		id := event.id
		r.TotalCost += replayCosts[id%4]
		p.tiny.add(id, p.gen[id%2])
		e := p.entries[id]
		if e != nil && now >= e.deadline {
			p.remove(id)
			e = nil
		}
		if e != nil {
			r.Hits++
			r.AvoidedCost += replayCosts[id%4]
			p.hit(e, now)
		} else {
			r.Loads++
			p.gate[id].add(now)
			if p.gate[id].value(now) >= 2 {
				p.admit(&replayEntry{id: id, bytes: bytesWeigh(replayKey(id), replayValue(id)), deadline: now + p.ttl, grace: now + 2*replayDelta, low: -1, heat: replayHeat{era: now / replayDelta}})
			}
		}
		if len(p.entries) > replayCount || p.bytes > replayBytes || p.bytes < 0 {
			t.Fatalf("%s capacity: %d %d", kind, len(p.entries), p.bytes)
		}
		r.PeakCount = max(r.PeakCount, len(p.entries))
		r.PeakBytes = max(r.PeakBytes, p.bytes)
	}
	return r
}

func runNativeReplay(t *testing.T, name, variant, digest string, events []replayEvent) replayResult {
	t.Helper()
	clock, executor := NewManualClock(int64(time.Second)), NewManualExecutor()
	tin, victims := 2.0, 8
	if variant == "AHKC-m1" {
		victims = 1
	}
	if variant == "AHKC-low-gate" {
		tin = .01
	}
	shards := 1
	if variant == "AHKC-4shards" {
		shards = 4
	}
	cfg := Config{
		Shards:         shards,
		MaxEntries:     replayCount,
		MaxBytes:       replayBytes,
		MaxEntryBytes:  32 << 10,
		MaxResultBytes: 32 << 10,
		Namespaces: map[string]NamespaceConfig{
			"a": {TTL: time.Duration(replayTTL(name)), Tin: tin},
			"b": {TTL: time.Duration(replayTTL(name)), Tin: tin},
		},
		Heat: HeatConfig{
			Delta: time.Duration(replayDelta),
		},
		Evict: EvictConfig{
			Horizon:     200 * time.Millisecond,
			VictimLimit: victims,
		},
		Limits: LimitsConfig{
			MaxConcurrentLoads:  8,
			HandoffTimeout:      time.Millisecond,
			MaxNamespaces:       2,
			MaxWaitersTotal:     64,
			FillQueueSize:       16,
			PendingBytes:        64 << 10,
			FlightStripes:       2,
			FlightsPerStripe:    8,
			ReaderSlotsPerShard: 8,
		},
		Runtime: RuntimeConfig{
			Clock:            clock,
			Executor:         executor,
			DisableTicker:    true,
			PeriodicInterval: time.Millisecond,
		},
	}
	c, err := NewBytes[string](cfg, WithResultMetadata[string](func(_ string, value string) ResultMetadata {
		return ResultMetadata{Cost: float32(replayCosts[value[0]-'a'])}
	}))
	if err != nil {
		t.Fatal(err)
	}
	namespaces := [2]*Namespace[string]{}
	namespaces[0], _ = c.Namespace("a")
	namespaces[1], _ = c.Namespace("b")
	r := replayResult{Scenario: name, Policy: variant, TraceSHA256: digest, Requests: len(events)}
	for i, event := range events {
		clock.Set(int64(time.Second) + int64(i)*int64(100*time.Microsecond))
		if i%32 == 0 {
			c.MaintenanceStep()
			executor.Drain(2000)
		}
		if event.invalidate >= 0 {
			h, err := c.InvalidateNamespaceAsync([]string{"a", "b"}[event.invalidate])
			if err != nil {
				t.Fatal(err)
			}
			executor.Drain(2000)
			if err := h.Wait(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		id := event.id
		ns := namespaces[id%2]
		r.TotalCost += replayCosts[id%4]
		var request Request[string]
		if err := ns.BeginRequestOn(&request, replayKey(id)); err != nil {
			t.Fatal(err)
		}
		_, hit, err := ns.GetOn(&request)
		if err != nil {
			t.Fatal(err)
		}
		if hit {
			r.Hits++
			r.AvoidedCost += replayCosts[id%4]
		} else {
			future, err := ns.FetchAsyncOn(&request, func(context.Context, string) (string, error) {
				r.Loads++
				clock.Advance(time.Microsecond)
				return replayValue(id), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			executor.Drain(2000)
			if _, ready, err := future.Take(); err != nil || !ready {
				t.Fatalf("load settlement: %v %v", ready, err)
			}
		}
		st := c.Stats()
		r.PeakCount = max(r.PeakCount, int(st.ResidentEntries))
		r.PeakBytes = max(r.PeakBytes, st.ResidentBytes)
		if r.PeakCount > replayCount || r.PeakBytes > replayBytes || st.IOInFlight != 0 {
			t.Fatal("native resource invariant")
		}
		if i%1000 == 999 {
			measureReplayForecast(c, namespaces, events, i, &r)
		}
	}
	r.Published = int(c.Stats().FillPublished)
	closeManual(t, c, executor)
	return r
}

func measureReplayForecast(c *Cache[string], namespaces [2]*Namespace[string], events []replayEvent, at int, r *replayResult) {
	t := c.c.now()
	duration := min(int64(c.c.cfg.Evict.Horizon), int64(namespaces[0].ns.cfg.TTL), int64(len(events)-1-at)*int64(100*time.Microsecond))
	if duration <= 0 {
		return
	}
	for id := 0; id < 16; id++ {
		heat, ok := namespaces[id%2].Heat(replayKey(id))
		if !ok {
			continue
		}
		g := c.c.heatValue(heat, float32(replayCosts[id%4]), t+duration, t)
		future := 0
		for i := at + 1; i < len(events) && int64(i-at)*int64(100*time.Microsecond) <= duration; i++ {
			if events[i].invalidate == id%2 {
				break
			}
			if events[i].id == id {
				future++
			}
		}
		actual := float64(future) * replayCosts[id%4]
		r.PredictionAbsError += math.Abs(g - actual)
		r.FutureCost += actual
		r.PredictionChecks++
	}
}

// 默认只把六类流量回放给 AHKC 本身，断言资源上限、IO 归零与加载结算（库的不变量）。
// 设置 AHKC_REPLAY_REPORT 才会额外运行四个参考策略并写出对比报告：它们是对照用的顺序模型，
// 不守护库的正确性，所以平时不跑。
func TestSyntheticPolicyReplay(t *testing.T) {
	report := os.Getenv("AHKC_REPLAY_REPORT")
	count := 256
	if report != "" {
		count = 20000
	}
	var results []replayResult
	for _, scenario := range []string{"zipf", "scan", "hot-shift", "uniform-size-cost", "short-ttl", "namespace-invalidation"} {
		events, digest := makeReplayTrace(scenario, count)
		if report != "" {
			for _, policy := range []string{"LRU", "SIEVE", "S3-FIFO", "W-TinyLFU-AV"} {
				results = append(results, runReferenceReplay(t, scenario, policy, digest, events))
			}
		}
		variants := []string{"AHKC"}
		if count > 256 {
			variants = append(variants, "AHKC-m1", "AHKC-low-gate", "AHKC-4shards")
		}
		for _, variant := range variants {
			results = append(results, runNativeReplay(t, scenario, variant, digest, events))
		}
	}
	if path := report; path != "" {
		report := struct {
			Seed                           int64
			Model, Scope, Limits           string
			References                     []string
			CapacityEntries, CapacityBytes int
			Results                        []replayResult
		}{20261001, "sequential arrivals every 100us, load ready after 1us, synthetic avoidable work units; r=lambda calibrated because no overlapping arrivals", "one shard, same billed resident capacity, full keys, TTL, per-policy residency-dependent exact heat gate and cold grace; native AHKC uses its bounded maintenance and approximate sketches; transfer ablation compares AHKC-4shards to AHKC-no-transfer separately", "reference policies are sequential models with different metadata, not equal-total-memory service baselines; future-value error is conditional on retaining one object; no global dynamic OPT or CPU-benefit claim; sparse sampling uses the library PRNG", []string{"https://jasony.me/publication/sosp23-s3fifo.pdf", "https://arxiv.org/abs/1512.00727", "https://arxiv.org/abs/2105.08770"}, replayCount, replayBytes, results}
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
}

// 校验报告里 W-TinyLFU-AV 参考模型本身的老化与 AV 比较；它不测库，只在出报告时才有意义。
func TestReplayTinyLFUAgingAndAV(t *testing.T) {
	if os.Getenv("AHKC_REPLAY_REPORT") == "" {
		t.Skip("reference-model check; runs with AHKC_REPLAY_REPORT")
	}
	var f replayTiny
	f.add(1, 0)
	if f.frequency(1, 0) != 1 {
		t.Fatal("first arrival not in doorkeeper")
	}
	f.add(1, 0)
	if f.frequency(1, 0) != 2 {
		t.Fatal("repeat not counted")
	}
	for i := 0; i < 1000; i++ {
		f.add(1, 0)
	}
	before := f.frequency(1, 0)
	for i := 0; i < 5000; i++ {
		f.add(20+i%200, 0)
	}
	if f.frequency(1, 0) >= before {
		t.Fatal("frequency did not age")
	}
	p := replayPolicy{kind: "W-TinyLFU-AV", entries: make(map[int]*replayEntry)}
	for id := 0; id < 2; id++ {
		p.insert(&replayEntry{id: id, bytes: 25000}, 1)
		for j := 0; j < 6; j++ {
			p.tiny.add(id, 0)
		}
	}
	for i := 0; i < 8; i++ {
		p.tiny.add(8, 0)
	}
	if p.admitWT(&replayEntry{id: 8, bytes: 40000}) || len(p.entries) != 2 {
		t.Fatal("AV compared candidate with only one victim or evicted on rejection")
	}
}

// Full-resident enumeration is deliberately independent of exactSelect and
// keeps the same removal, retirement and index constraints. It relaxes only Q,
// so its objective is a one-decision bound, not a dynamic trace optimum.
func TestBoundedSearchAgainstFullResidentOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(20261001))
	var report struct {
		Scope                                                                 string
		Decisions, LocalVictims, FullVictims, FeasibilityGaps, PositiveRegret int
		LocalGain, FullResidentGain, SearchRegret                             float64
	}
	report.Scope = "same frozen estimates and resource constraints; independent subset enumeration over all 12 residents versus Q of 8; no prediction or global dynamic OPT claim"
	report.LocalVictims, report.FullVictims = 8, 12
	for trial := 0; trial < 128; trial++ {
		q := make([]victimInfo, 12)
		var bytes int64
		for i := range q {
			q[i] = victimInfo{id: uint64(i + 1), billed: int64(1 + rng.Intn(8)), loss: float64(rng.Intn(50) - 5)}
			bytes += q[i].billed
		}
		s := fillSnapshot{q: q, aBytes: int64(1 + rng.Intn(16)), n: 12, bytes: bytes, countQuota: 12, byteQuota: bytes,
			installWithoutQ: true, maxChecks: 256}
		if trial%5 == 0 {
			s.installWithoutQ = false
			s.installMask = 1 << 11
		}
		candidateGain := float64(1 + rng.Intn(100))
		full := bruteForce(&s)
		local := s
		local.q = slices.Clone(q[:8])
		local.installMask &= (1 << 8) - 1
		chosen := local.exactSelect()
		if chosen != bruteForce(&local) {
			t.Fatal("local optimizer differs from independent oracle")
		}
		gain := func(mask int64, victims []victimInfo) float64 {
			if mask < 0 {
				return 0
			}
			loss := 0.0
			for i, v := range victims {
				if mask&(1<<i) != 0 {
					loss += v.loss
				}
			}
			return max(0, candidateGain-loss)
		}
		localGain, fullGain := gain(chosen, local.q), gain(full, q)
		if fullGain < localGain {
			t.Fatal("relaxing Q worsened the optimum")
		}
		report.Decisions++
		report.LocalGain += localGain
		report.FullResidentGain += fullGain
		report.SearchRegret += fullGain - localGain
		if chosen < 0 && full >= 0 {
			report.FeasibilityGaps++
		}
		if fullGain > localGain {
			report.PositiveRegret++
		}
	}
	if report.PositiveRegret == 0 || report.FeasibilityGaps == 0 {
		t.Fatal("oracle experiment did not exercise missing candidates")
	}
	if path := os.Getenv("AHKC_ORACLE_REPORT"); path != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
}
