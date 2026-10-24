package ahkc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCalibratedRateGrid(t *testing.T) {
	clk := NewManualClock(0)
	bands := []RateBand{
		{CostClass: 0, MaxLambda: 100, MaxLoadDuration: time.Millisecond, MergeFactor: .5},
		{CostClass: 0, MaxLambda: 10, MaxLoadDuration: time.Millisecond, MergeFactor: .9},
		{CostClass: 0, MaxLambda: 100, MaxLoadDuration: 10 * time.Millisecond, MergeFactor: .1},
	}
	m := newCostTable(t, CostTableConfig{Clock: clk, Classes: 1, Alpha: 1, RateBands: bands, FallbackCost: 2, FallbackLoadDuration: time.Millisecond})
	bands[1].MergeFactor = 0 // caller mutation cannot change a calibrated model
	for _, test := range []struct{ lambda, rate float64 }{{10, 9}, {11, 5.5}, {100, 50}} {
		r, _, _, _, _, ok := m.Lookup(test.lambda, 0)
		if !ok || r != test.rate {
			t.Fatalf("lambda %v: r=%v ok=%v", test.lambda, r, ok)
		}
	}
	if _, _, _, _, _, ok := m.Lookup(101, 0); ok {
		t.Fatal("accepted rate outside calibration")
	}
	m.ObserveLoad(0, 3, 2*time.Millisecond)
	snapshot := m.SnapshotAt(0)
	if r, c, _, _, _, ok := snapshot.Lookup(10, 0); !ok || r != 1 || c != 3 {
		t.Fatal("duration bucket or cost lost")
	}
	m.ObserveLoad(0, 4, 11*time.Millisecond)
	if _, _, _, _, _, ok := m.Lookup(10, 0); ok {
		t.Fatal("accepted duration outside calibration")
	}
	if r, _, _, _, _, ok := snapshot.Lookup(10, 0); !ok || r != 1 {
		t.Fatal("snapshot changed")
	}
	for _, cfg := range []CostTableConfig{{}, {RateBands: bands, FallbackCost: 2}, {MergeFactor: 1, Unit: "cpu"}} {
		bad, err := NewCostTableModel(cfg)
		if err != nil {
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatal(err)
			}
			continue
		}
		if _, _, _, _, _, ok := bad.Lookup(1, 0); ok {
			t.Fatal("missing calibration/duration/unit contract accepted")
		}
	}
	m.Observe(math.MaxUint32, 1)
	if _, _, _, _, _, ok := m.Lookup(1, math.MaxUint32); ok {
		t.Fatal("accepted out-of-range class")
	}
}

func TestOwnedHandleBudgetAndConcurrentRelease(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, func(c *Config) { c.MaxOwnedHandles = 1 })
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	h, hit, err := env.ns.GetOwned("k")
	if err != nil || !hit {
		t.Fatalf("owned read: %v %v", hit, err)
	}
	if _, err := h.Clone(); !errors.Is(err, ErrBusy) {
		t.Fatalf("global handle limit: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = h.Value()
				h.Release()
			}
		}()
	}
	wg.Wait()
	if env.c.c.handleBudget.available() != 1 {
		t.Fatal("concurrent release lost or duplicated the receipt")
	}
	closeManual(t, env.c, env.ex)
}

func TestCreditTransitAndDuplicateDelivery(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, func(c *Config) { c.MaxEntries = 65 })
	c, s := env.c.c, env.c.c.shards[0]
	s.wantCredit.Store(true)
	c.creditCoordinator()
	state, ok := env.c.Credits()
	if !ok || state.CountInTransit == 0 || state.CountGranted+state.CountFree+state.CountInTransit != state.CountLimit || state.ByteGranted+state.ByteFree+state.ByteInTransit != state.ByteLimit {
		t.Fatalf("transit ledger: %+v %v", state, ok)
	}
	op := <-s.creditQueue
	if !c.processControl(s, op) {
		t.Fatal("cannot deliver grant")
	}
	before := s.countQuota.Load()
	if !c.processControl(s, op) || s.countQuota.Load() != before {
		t.Fatal("duplicate grant applied twice")
	}
	state, ok = env.c.Credits()
	if !ok || state.CountInTransit != 0 || state.CountGranted+state.CountFree != state.CountLimit || state.ByteGranted+state.ByteFree != state.ByteLimit {
		t.Fatalf("settled ledger: %+v", state)
	}
	closeManual(t, env.c, env.ex)
}

func TestCheckedIDsAndClockStopAdmission(t *testing.T) {
	var id atomic.Uint64
	id.Store(math.MaxUint64 - 1)
	if n, ok := checkedNext(&id); !ok || n != math.MaxUint64 {
		t.Fatal("last unique ID was not usable")
	}
	if _, ok := checkedNext(&id); ok || id.Load() != math.MaxUint64 {
		t.Fatal("ID wrapped")
	}
	d := newEpochDomain(2)
	d.epoch.Store(math.MaxUint64)
	if _, ok := d.pin(); ok {
		t.Fatal("epoch wrapped into inactive sentinel")
	}
	d.epoch.Store(0)
	if g, ok := d.pin(); !ok {
		t.Fatal("pin failed after epoch reset")
	} else {
		g.unpin()
	}
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	old := env.ns.ns.model.Load()
	env.clk.Set(env.c.c.timeLimit)
	if err := env.ns.SetModel(hitCountModel{}); !errors.Is(err, ErrClosed) || env.ns.ns.model.Load() != old {
		t.Fatal("model published after clock exhaustion")
	}
	if _, err := env.ns.FetchAsync("k", okLoader("v")); !errors.Is(err, ErrClosed) {
		t.Fatal("clock exhaustion did not stop loads")
	}
	closeManual(t, env.c, env.ex)
}

func TestControlCanBeWaitedRepeatedly(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	h, err := env.ns.InvalidateAsync("k")
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	for i := 0; i < 3; i++ {
		if err := h.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	closeManual(t, env.c, env.ex)
}

func TestLargeEstimateIsUnavailable(t *testing.T) {
	cfg := Config{ValueDelta: time.Second, HorizonW: time.Second, NMin: 1, Advanced: Advanced{PriorMaxAge: time.Minute}}
	vs := newValueStats(1, 1, 0)
	model := newCostTable(t, CostTableConfig{MergeFactor: 1, FallbackCost: math.MaxFloat64 / 2})
	e := vs.estimate(estimateItem{hardDeadline: int64(time.Second)}, 0, &NamespaceConfig{Prior: &Prior{Lambda: 1}}, model, &cfg)
	if e.mode != estUnavailable {
		t.Fatal("finite terms can overflow subset comparisons")
	}
}

func TestModelMemoryContract(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	m := newCostTable(t, CostTableConfig{MergeFactor: 1})
	env.c.c.cfg.MaxModelBytes = m.RetainedBytes() - 1
	if err := env.ns.SetModel(m); err == nil {
		t.Fatal("oversized model retained")
	}
	closeManual(t, env.c, env.ex)
}

func TestManagedLayoutRejectsBeforeAllocation(t *testing.T) {
	cfg := validConfig()
	cfg.MaxManagedBytes = 1
	if _, err := New[string](cfg); err == nil {
		t.Fatal("managed byte cap ignored")
	}
}

func TestTypedNilHostResourcesRejected(t *testing.T) {
	var clock *ManualClock
	var executor *ManualExecutor
	for _, cfg := range []Config{func() Config { c := validConfig(); c.Clock = clock; return c }(), func() Config { c := validConfig(); c.Executor = executor; return c }()} {
		if _, err := New[string](cfg); err == nil {
			t.Fatal("typed nil host resource accepted")
		}
	}
	if m, err := NewCostTableModel(CostTableConfig{MergeFactor: 1, Clock: clock}); m != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatal("typed nil model clock accepted", err)
	}
}

func TestSampleQueueAgeAndNeverEmptyRing(t *testing.T) {
	s := newSampleStripe(3)
	for _, at := range []int64{10, 2, 7} {
		if !s.tryPush(sample{eventTime: at}) {
			t.Fatal("push")
		}
	}
	if s.oldest.Load() != 2 {
		t.Fatal("out-of-order minimum")
	}
	if v, _ := s.pop(); v.eventTime != 10 || s.oldest.Load() != 2 {
		t.Fatal("FIFO or minimum")
	}
	if v, _ := s.pop(); v.eventTime != 2 || s.oldest.Load() != 7 {
		t.Fatal("removed minimum retained")
	}
	for i := int64(8); i < 10000; i++ {
		if !s.tryPush(sample{eventTime: i}) {
			t.Fatal("ring full")
		}
		if v, ok := s.pop(); !ok || v.eventTime != i-1 || s.oldest.Load() != i {
			t.Fatal("sustained ring retains consumed age")
		}
		if s.head >= len(s.buf) || s.tail >= len(s.buf) {
			t.Fatal("ring counter is unbounded")
		}
	}
}

func TestScopedBorrowZeroAlloc(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, SampleP: 1e-12, Prior: testPrior}, nil)
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		var request Request[string]
		var borrowed Borrowed[string]
		if err := env.ns.BeginRequestOn(&request, "k"); err != nil {
			t.Fatal(err)
		}
		if hit, err := env.ns.BorrowOn(&request, &borrowed); !hit || err != nil {
			t.Fatalf("borrow: %v %v", hit, err)
		}
		if borrowed.Value() != "v" {
			t.Fatal("value")
		}
		borrowed.Release()
	})
	if allocs != 0 {
		t.Fatalf("scoped borrow: %.2f allocs", allocs)
	}
	closeManual(t, env.c, env.ex)
}

func TestReclamationRespectsRoundByteQuota(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, func(c *Config) { c.ShardCount = 1 })
	for _, key := range []string{"a", "b"} {
		if _, err := env.fetch(key, okLoader("v")); err != nil {
			t.Fatal(err)
		}
	}
	s := env.c.c.shards[0]
	g, ok := s.epoch.pin()
	if !ok {
		t.Fatal("pin")
	}
	for _, key := range []string{"a", "b"} {
		if _, err := env.ns.InvalidateAsync(key); err != nil {
			t.Fatal(err)
		}
	}
	env.drive()
	if len(s.retired) != 2 {
		t.Fatal("retired fixtures")
	}
	g.unpin()
	for i := 0; i < 3; i++ {
		s.epoch.advanceStep(len(s.epoch.slots))
	}
	s.roundActive = true
	s.reclaimBytesLeft = s.retired[0].billed
	env.c.c.reclaimStep(s, 256)
	s.roundActive = false
	submitted := 0
	for _, r := range s.retired {
		if r.state.Load() == retiredSubmitting {
			submitted++
		}
	}
	if submitted != 1 {
		t.Fatalf("round submitted %d objects", submitted)
	}
	closeManual(t, env.c, env.ex)
}

func TestFillRejectsMixedModelUnits(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, func(c *Config) { c.ShardCount = 1 })
	model := newCostTable(t, CostTableConfig{MergeFactor: 1, FallbackCost: 1, DisableAutoObserve: true, Clock: env.clk})
	other, err := env.c.RegisterNamespace("other", NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior, Model: model})
	if err != nil {
		t.Fatal(err)
	}
	f, err := other.FetchAsync("old", okLoader("old"))
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if _, err := f.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := env.c.c.shards[0].idx.find("old", other.ns.hashKey("old"), other.ns.id).e
	if old == nil {
		t.Fatal("old entry not published")
	}
	s, a, f := takeCandidate(t, env, env.ns, "candidate")
	if snap, _, _, reason := env.c.c.freezeInput(s, a, nil, []*entry[string]{old}); snap != nil || reason != fillRejectedNoEstimate {
		t.Fatalf("mixed units: %v", reason)
	}
	env.c.c.rejectCandidate(a, fillRejectedNoEstimate)
	f.Cancel()
	closeManual(t, env.c, env.ex)
}

// This exercises 72 hours of monotonic lifecycle time, not 72 wall-clock hours
// of sustained load. The load command is used for the separate real soak gate.
func TestVirtual72HourLifecycle(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, func(c *Config) {
		c.GateWidth, c.ValueWidth, c.ReaderSlotsPerShard = 64, 16, 8
		c.FlightStripes, c.FlightsPerStripe, c.RetiredRecords = 2, 8, 64
	})
	// Eight bounded rounds cover this fixture's 64 index slots, eight reader
	// slots, sixteen flight slots and both reclamation epochs.
	drive := func() {
		for i := 0; i < 8; i++ {
			env.ex.Drain(512)
			env.c.MaintenanceStep()
			env.ex.Drain(512)
		}
	}
	for step := 0; step < 288; step++ {
		if err := env.ns.SetPrior(&Prior{Lambda: 10}); err != nil {
			t.Fatal(err)
		}
		f, err := env.ns.FetchAsync("cycle", okLoader("v"))
		if err != nil {
			t.Fatal(err)
		}
		drive()
		if _, ready, err := f.Take(); err != nil || !ready {
			t.Fatalf("virtual load: %v %v", err, ready)
		}
		h, hit, err := env.ns.GetOwned("cycle")
		if err != nil || !hit {
			t.Fatalf("step %d read: %v %v", step, hit, err)
		}
		drive() // finish sample transfer before the simulated idle interval
		env.clk.Advance(15 * time.Minute)
		if _, hit, err := env.ns.Get("cycle"); err != nil || hit {
			t.Fatalf("step %d expired hit", step)
		}
		drive()
		if h.Value() != "v" {
			t.Fatal("owned value lost across wheel rebase")
		}
		h.Release()
		drive()
		st := env.c.Stats()
		if st.IOInFlight != 0 || st.ResidentEntries != 0 || st.PendingBytesAvail != env.c.c.pendingBytes.limit || st.ResponseBytesAvail != env.c.c.responseBytes.limit || st.RetiredRecordsAvail != env.c.c.retiredRecords.limit {
			t.Fatalf("step %d retained responsibility: %+v", step, st)
		}
	}
	closeManual(t, env.c, env.ex)
}

func TestFillChecksUndrainedSampleQuality(t *testing.T) {
	for _, fault := range []bool{false, true} {
		t.Run(fmt.Sprintf("fault=%v", fault), func(t *testing.T) {
			env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
			s, a, f := takeCandidate(t, env, env.ns, "candidate")
			if fault {
				s.stripes[0].faultFlag.Store(true)
			} else {
				s.stripes[0].tryPush(sample{nsID: env.ns.ns.id, nsGen: env.ns.ns.gen.Load(), eventTime: env.clk.Now() - int64(env.c.c.cfg.MaxSampleLag) - 1, invP: 1})
			}
			if plan, reason := env.c.c.planFill(s, a); plan != nil || reason != fillRejectedNoEstimate {
				t.Fatalf("undrained polluted input produced a decision: %v", reason)
			}
			env.c.c.rejectCandidate(a, fillRejectedNoEstimate)
			env.drive()
			f.Cancel()
		})
	}
}

func TestBorrowedAndClonedHandlesKeepRetiredEntry(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	b, hit, err := env.ns.GetBorrowed("k")
	if err != nil || !hit {
		t.Fatalf("borrow: %v %v", hit, err)
	}
	e := b.e
	h, err := b.Clone()
	if err != nil {
		t.Fatal(err)
	}
	i, err := env.ns.InvalidateAsync("k")
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if err := i.Wait(context.Background()); err != nil || b.Value() != "v" || e.inPool {
		t.Fatal("borrowed view was reclaimed during its read scope")
	}
	b.Release()
	clone, err := h.Clone()
	if err != nil {
		t.Fatal(err)
	}
	h.Release()
	env.drive()
	if h.e != nil || clone.Value() != "v" || e.inPool {
		t.Fatal("cloned ownership was lost on release")
	}
	clone.Release()
	if _, err := clone.Clone(); !errors.Is(err, ErrCanceled) {
		t.Fatal("released handle could be cloned")
	}
	closeManual(t, env.c, env.ex)
}

func TestCostTableSnapshotsFreezeTimeAndVersion(t *testing.T) {
	clk := NewManualClock(0)
	m := newCostTable(t, CostTableConfig{MergeFactor: 1, Clock: clk, Classes: 2, MaxAge: time.Second, FallbackCost: -1})
	m.Observe(0, 1e100) // no micro-unit integer overflow, and time zero is valid
	snapshot := m.SnapshotAt(0)
	if snapshot == nil {
		t.Fatal("snapshot unavailable at time zero")
	}
	clk.Advance(2 * time.Second)
	m.Observe(0, 3)
	_, cost, _, _, _, ok := snapshot.Lookup(1, 0)
	if !ok || cost != 1e100 {
		t.Fatal("frozen cost changed with live observation or clock")
	}
	if snapshot.Version() == m.Version() {
		t.Fatal("observation reused a model version")
	}
	if bad, err := NewCostTableModel(CostTableConfig{MergeFactor: 1, Alpha: math.NaN()}); bad != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatal("invalid model configuration was accepted", err)
	}
}

func TestResultMetadataControlsCaching(t *testing.T) {
	for _, noCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("noCache=%v", noCache), func(t *testing.T) {
			env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
			deadline := env.clk.Now() + int64(200*time.Millisecond)
			class := uint32(3)
			env.c.c.metadata = func(string, string) ResultMetadata {
				return ResultMetadata{NoCache: noCache, HardDeadline: deadline, CostClass: &class}
			}
			value, err := env.fetch("k", okLoader("business"))
			if err != nil || value != "business" {
				t.Fatalf("load: %q %v", value, err)
			}
			e, hit := findEntry(env, "k")
			if noCache {
				if hit || env.c.Stats().FillRejected["NOT_CACHEABLE"] != 1 {
					t.Fatal("NoCache result was admitted")
				}
				return
			}
			if !hit || e.hardDeadline != deadline || e.costClass != class {
				t.Fatal("per-result validity or cost class was lost")
			}
			env.clk.Set(deadline)
			if _, hit, err := env.ns.Get("k"); hit || err != nil {
				t.Fatal("per-result deadline did not enforce a hard miss")
			}
		})
	}
}

func TestLoadedErrorRetainsResponseUntilTake(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	want := errors.New("source error")
	f, err := env.ns.FetchAsync("k", func(context.Context, string) (string, error) { return "", want })
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if avail := env.c.c.responseBytes.available(); avail != env.c.c.responseBytes.limit-env.c.c.cfg.MaxResultBytes {
		t.Fatal("error response reservation returned before delivery")
	}
	if _, ok, err := f.Take(); !ok || !errors.Is(err, want) {
		t.Fatalf("take: %v %v", err, ok)
	}
	if env.c.c.responseBytes.available() != env.c.c.responseBytes.limit || f.delivery != nil {
		t.Fatal("settled error still retains library responsibility")
	}
}

func TestResultSizeCheckedBeforeHeatAdmission(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1000, Prior: testPrior}, nil)
	env.c.c.weigh = func(string, string) int64 { return env.c.c.cfg.MaxResultBytes + 1 }
	if _, err := env.fetch("cold", okLoader("oversize")); !errors.Is(err, ErrResultTooLarge) {
		t.Fatal("cold output bypassed the result-size contract")
	}
}

func TestWeigherPanicSettlesLoad(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	env.c.c.weigh = func(string, string) int64 { panic("weigher") }
	_, err := env.fetch("k", okLoader("v"))
	var panicErr *PanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("weigher panic: %v", err)
	}
	closeManual(t, env.c, env.ex)
}

func TestFutureCancellationWakesWait(t *testing.T) {
	f := newFuture[string](0, nil)
	done := make(chan error, 1)
	go func() { _, err := f.Wait(context.Background()); done <- err }()
	f.Cancel()
	select {
	case err := <-done:
		if !errors.Is(err, ErrCanceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not wake the waiter")
	}
}

func TestFutureCancelPausedPublisher(t *testing.T) {
	b := newBudget(1)
	r, _ := b.tryReserve(1)
	f := newFuture[string](0, r)
	f.state.Store(waiterBinding) // publisher is paused before its payload becomes ready
	f.Cancel()
	if b.available() != 1 || f.state.Load() != waiterCancelled {
		t.Fatal("cancellation waited for publication")
	}
	if _, ok, err := f.Take(); !ok || !errors.Is(err, ErrCanceled) {
		t.Fatalf("take: %v %v", err, ok)
	}
}

func TestCachedFutureHoldsEntryUntilTaken(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	f, err := env.ns.FetchAsync("k", okLoader("unused"))
	if err != nil {
		t.Fatal(err)
	}
	e, ok := findEntry(env, "k")
	if !ok || e.ownedPins.Load() != 1 {
		t.Fatal("cached future did not retain an owning pin")
	}
	h, err := env.ns.InvalidateAsync("k")
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if err := h.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e.inPool {
		t.Fatal("entry reclaimed before the consumer took its value")
	}
	if v, ok, err := f.Take(); !ok || err != nil || v != "v" {
		t.Fatalf("take: %q %v %v", v, err, ok)
	}
	if f.delivery != nil {
		t.Fatal("settled future still retains its payload")
	}
	env.drive()
	if !e.inPool {
		t.Fatal("entry was not reclaimed after the result was transferred")
	}
}

func TestReclaimSkipsPinnedPrefix(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, func(c *Config) { c.ShardCount = 1; c.RoundWorkLimit = 10; c.RetiredRecords = 8 })
	var pins []*Handle[string]
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("key%d", i)
		if _, err := env.fetch(key, okLoader("v")); err != nil {
			t.Fatal(err)
		}
		if i < 3 {
			h, hit, err := env.ns.GetOwned(key)
			if !hit || err != nil {
				t.Fatal(err)
			}
			pins = append(pins, h)
		}
	}
	for i := 0; i < 8; i++ {
		if _, err := env.ns.InvalidateAsync(fmt.Sprintf("key%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	env.drive()
	if avail := env.c.c.retiredRecords.available(); avail != 5 {
		t.Fatalf("unheld records starved behind pinned prefix: available=%d", avail)
	}
	for _, h := range pins {
		h.Release()
	}
	closeManual(t, env.c, env.ex)
}

func TestColdScanProgressDuringWheelRebase(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	s := env.c.c.shards[0]
	s.wheel.startRebase(env.clk.Now() / int64(env.c.c.cfg.WheelTick))
	env.c.c.lifecycleStep(s, 10)
	if s.coldCursor == 0 {
		t.Fatal("wheel rebase starved the cold scan")
	}
	if work := env.c.c.coldSweep(s, 3); work != 3 {
		t.Fatal("empty index slots escaped the scan budget")
	}
}

func TestCreditQueueFullReturnsGrant(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, func(c *Config) { c.MaxEntries = 65; c.ControlQueueSize = 1 })
	c := env.c.c
	s := c.shards[0]
	s.creditQueue <- &controlOp[string]{kind: ctlCreditApply}
	s.wantCredit.Store(true)
	before := c.globalFreeCount.Load()
	c.creditCoordinator()
	if c.globalFreeCount.Load() != before {
		t.Fatal("queue rejection lost a count grant")
	}
	env.drive()
}

func TestCreditCanGrowBeyondInitialShare(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, func(c *Config) { c.ShardCount = 2 })
	c := env.c.c
	s := c.shards[0]
	before := s.countQuota.Load()
	s.wantCredit.Store(true)
	c.creditCoordinator()
	env.ex.Drain(100)
	c.creditCoordinator()
	env.ex.Drain(100)
	if s.countQuota.Load() <= before {
		t.Fatal("physical shard ceiling prevents every count grant")
	}
	n, b := c.globalFreeCount.Load(), c.globalFreeBytes.Load()
	for _, s := range c.shards {
		n += s.countQuota.Load()
		b += s.byteQuota.Load()
	}
	if n != c.cfg.MaxEntries || b != c.cfg.MaxResidentBytes {
		t.Fatalf("credits not conserved: %d %d", n, b)
	}
}

func TestInvalidNumericInputsRejected(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.SampleP = math.NaN() },
		func(c *Config) { c.FillRatePerSec = math.Inf(1) },
		func(c *Config) { c.HeatDelta = -time.Second },
		func(c *Config) { c.WheelLevels = math.MaxInt },
		func(c *Config) { c.ShardCount = math.MaxInt },
		func(c *Config) { c.Namespaces["t"] = NamespaceConfig{TTL: time.Minute, Tin: math.Inf(1)} },
		func(c *Config) { c.Namespaces["t"] = NamespaceConfig{TTL: time.Minute, Tin: 2, Tout: math.NaN()} },
		func(c *Config) { c.HeatCap = 2 },
		func(c *Config) { c.MaxLoadFactor = math.NaN() },
		func(c *Config) { c.GateWidth = -1 },
		func(c *Config) { c.MaxOwnedPinsPerEntry = -1 },
		func(c *Config) { c.ModelMaxAge = -time.Second },
	} {
		cfg := validConfig()
		mutate(&cfg)
		if _, err := New[string](cfg); err == nil {
			t.Fatal("invalid numeric input was accepted")
		}
	}
}

func TestRegistrationCopiesPrior(t *testing.T) {
	p := &Prior{Lambda: 7}
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: p}, nil)
	if p.BuiltAt != 0 {
		t.Fatal("registration mutated the caller's prior")
	}
	p.Lambda = 99
	if env.ns.ns.prior.Load().Lambda != 7 {
		t.Fatal("caller mutation changed an active prior")
	}
}

func TestOnRequestRejectsForeignContext(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	other, err := env.c.RegisterNamespace("other", NamespaceConfig{TTL: time.Minute, Tin: 1})
	if err != nil {
		t.Fatal(err)
	}
	r, err := env.ns.BeginRequest("k")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := other.GetOn(r); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, _, err := other.GetOwnedOn(r); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, err := other.FetchAsyncOn(r, okLoader("unused")); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, _, err := env.ns.GetOn(nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, _, err := env.ns.GetOwnedOn(nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
}

func TestReviewCloseAccounting(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	env.c.Close(ctx)
	for i := 0; i < 100; i++ {
		env.c.MaintenanceStep()
		env.ex.Drain(1000)
		reclaimed := env.c.CloseProgress().Reclaimed
		if reclaimed {
			return
		}
	}
	t.Logf("stats=%+v admissions=%d nsDrain=%v creditOwner=%v state=%d", env.c.Stats(), env.c.c.admissions.Load(), env.c.c.nsCtlDraining.Load(), env.c.c.creditOwner.Load(), env.c.c.state.Load())
	for _, s := range env.c.c.shards {
		t.Logf("shard %d owner=%d residents=%d retired=%d flights=%d fill=%d control=%d", s.id, s.ownerState.Load(), s.residentCount.Load(), s.retiredCount.Load(), s.flightCount.Load(), len(s.fillQueue), len(s.controlQueue))
	}
	t.Fatal("close did not reclaim")
}

func TestSampleArrivingAfterDecisionTimestamp(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	c, s := env.c.c, env.c.c.shards[0]
	decisionAt := env.clk.Now()
	env.clk.Advance(time.Nanosecond)
	if !s.stripes[0].tryPush(sample{nsID: env.ns.ns.id, nsGen: env.ns.ns.gen.Load(), eventTime: env.clk.Now(), invP: 1}) {
		t.Fatal("enqueue")
	}
	if !c.sampleQuality(s, decisionAt) || s.stats.lastFaultAt != noFaultAt {
		t.Fatal("a concurrent arrival was treated as a future sample")
	}
	// A genuinely future timestamp must still poison the decision.
	env.clk.Set(decisionAt)
	if c.sampleQuality(s, decisionAt) {
		t.Fatal("future timestamp was accepted")
	}
}

func TestDrainUsesTimeAfterReadingSample(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	c, s := env.c.c, env.c.c.shards[0]
	before := env.clk.Now()
	c.cfg.Clock = &advancingReadClock{before: before, after: before + 1, advanceAt: 2}
	defer func() { c.cfg.Clock = env.clk }()
	sm := sample{nsID: env.ns.ns.id, nsGen: env.ns.ns.gen.Load(), policyHash: 7, eventTime: before + 1, invP: 1}
	if !s.stripes[0].tryPush(sm) {
		t.Fatal("enqueue")
	}
	if c.drainSamples(s, 1) != 1 || s.stats.lastFaultAt != noFaultAt {
		t.Fatal("a sample arriving during a drain was rejected")
	}
	if _, count := s.stats.query(sm.policyHash, before+1, int64(c.cfg.ValueDelta)); count != 1 {
		t.Fatalf("sample was not counted: %g", count)
	}
}

func TestOwnedRetryCountsOneLogicalRequest(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, func(c *Config) { c.MaxOwnedHandles = 1 })
	if _, err := env.fetch("owned", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	held, hit, err := env.ns.GetOwned("owned")
	if err != nil || !hit {
		t.Fatal(hit, err)
	}
	before := env.c.Stats()
	var request Request[string]
	if err := env.ns.BeginRequestOn(&request, "owned"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := env.ns.GetOwnedOn(&request); !errors.Is(err, ErrBusy) {
		t.Fatal("expected bounded contention", err)
	}
	held.Release()
	h, hit, err := env.ns.GetOwnedOn(&request)
	if err != nil || !hit {
		t.Fatal(hit, err)
	}
	h.Release()
	after := env.c.Stats()
	if after.Hits-before.Hits != 1 || after.Samples-before.Samples != 1 {
		t.Fatalf("retry duplicated one logical request: before=%+v after=%+v", before, after)
	}
}

// TestInvalidateNotBlockedByBusyCreditOwner：额度转移取不到全局 creditOwner 时，
// 同一分片上排在后面的单 key 失效仍须在同一轮内确认（不再队头阻塞一个周期）。
func TestInvalidateNotBlockedByBusyCreditOwner(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	env.drive()
	c := env.c.c
	s := c.shardOf(env.ns.ns.hashKey("k"))
	if !c.creditOwner.CompareAndSwap(false, true) { // 模拟协调者正在持有 creditOwner
		t.Fatal("creditOwner unexpectedly busy")
	}
	s.creditQueue <- &controlOp[string]{kind: ctlCreditApply}
	h, err := env.ns.InvalidateAsync("k")
	if err != nil {
		t.Fatal(err)
	}
	c.stepClass(s, classControl, 8)
	select {
	case <-h.result.ready:
	default:
		t.Fatal("invalidate confirmation stalled behind a deferred credit operation")
	}
	if s.deferredControl.Load() == nil {
		t.Fatal("credit operation should stay deferred while creditOwner is busy")
	}
	c.creditOwner.Store(false)
	env.drive()
	if s.deferredControl.Load() != nil {
		t.Fatal("deferred credit operation was not retried after creditOwner freed")
	}
	closeManual(t, env.c, env.ex)
}
