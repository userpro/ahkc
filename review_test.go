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

func TestOwnedHandleBudgetAndConcurrentRelease(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, func(c *Config) { c.Limits.MaxOwnedHandles = 1 })
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	env.clk.Set(env.c.c.timeLimit)
	if _, err := env.ns.FetchAsync("k", okLoader("v")); !errors.Is(err, ErrClosed) {
		t.Fatal("clock exhaustion did not stop loads")
	}
	closeManual(t, env.c, env.ex)
}

func TestControlCanBeWaitedRepeatedly(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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

func TestManagedLayoutRejectsBeforeAllocation(t *testing.T) {
	cfg := validConfig()
	cfg.Limits.MaxManagedBytes = 1
	if _, err := NewBytes[string](cfg); err == nil {
		t.Fatal("managed byte cap ignored")
	}
}

func TestTypedNilHostResourcesRejected(t *testing.T) {
	var clock *ManualClock
	var executor *ManualExecutor
	for _, cfg := range []Config{func() Config { c := validConfig(); c.Runtime.Clock = clock; return c }(), func() Config { c := validConfig(); c.Runtime.Executor = executor; return c }()} {
		if _, err := NewBytes[string](cfg); err == nil {
			t.Fatal("typed nil host resource accepted")
		}
	}
}

// One key is filled, read, left to expire, retired and reclaimed 288 times
// (72 simulated hours at 15 minutes per round). After every round no budget,
// reader slot, flight or resident entry may remain. It is a single-goroutine
// leak check on the simulated clock; real sustained load is the cacheload command.
func TestRepeatedFillAndExpiryLeaksNothing(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, func(c *Config) {
		c.Heat.GateCells, c.Limits.ReaderSlotsPerShard = 64, 8
		c.Limits.FlightStripes, c.Limits.FlightsPerStripe = 2, 8
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
			t.Fatal("owned value lost across expiry")
		}
		h.Release()
		drive()
		st := env.c.Stats()
		if st.IOInFlight != 0 || st.ResidentEntries != 0 || st.PendingBytesAvail != env.c.c.pendingBytes.limit || st.ResponseBytesAvail != env.c.c.responseBytes.limit {
			t.Fatalf("step %d retained responsibility: %+v", step, st)
		}
	}
	closeManual(t, env.c, env.ex)
}

func TestBorrowedAndClonedHandlesKeepRetiredEntry(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	b, hit, err := env.ns.GetBorrowed("k")
	if err != nil || !hit {
		t.Fatalf("borrow: %v %v", hit, err)
	}
	h, err := b.Clone()
	if err != nil {
		t.Fatal(err)
	}
	i, err := env.ns.InvalidateAsync("k")
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if err := i.Wait(context.Background()); err != nil || b.Value() != "v" || retiredPending(env) != 1 {
		t.Fatal("borrowed view was reclaimed during its read scope")
	}
	b.Release()
	clone, err := h.Clone()
	if err != nil {
		t.Fatal(err)
	}
	h.Release()
	env.drive()
	if h.e != nil || clone.Value() != "v" || retiredPending(env) != 1 {
		t.Fatal("cloned ownership was lost on release")
	}
	clone.Release()
	if _, err := clone.Clone(); !errors.Is(err, ErrCanceled) {
		t.Fatal("released handle could be cloned")
	}
	closeManual(t, env.c, env.ex)
}

func TestResultMetadataControlsCaching(t *testing.T) {
	for _, noCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("noCache=%v", noCache), func(t *testing.T) {
			env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
			deadline := env.clk.Now() + int64(200*time.Millisecond)
			env.c.c.metadata = func(string, string) ResultMetadata {
				return ResultMetadata{NoCache: noCache, HardDeadline: deadline, Cost: 3}
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
			if !hit || e.hardDeadline != deadline || e.cost != 3 {
				t.Fatal("per-result validity or cost was lost")
			}
			env.clk.Set(deadline)
			if _, hit, err := env.ns.Get("k"); hit || err != nil {
				t.Fatal("per-result deadline did not enforce a hard miss")
			}
		})
	}
}

func TestLoadedErrorRetainsResponseUntilTake(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1000}, nil)
	env.c.c.weigh = func(string, string) int64 { return env.c.c.cfg.MaxResultBytes + 1 }
	if _, err := env.fetch("cold", okLoader("oversize")); !errors.Is(err, ErrResultTooLarge) {
		t.Fatal("cold output bypassed the result-size contract")
	}
}

func TestWeigherPanicSettlesLoad(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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
	if retiredPending(env) != 1 {
		t.Fatal("entry reclaimed before the consumer took its value")
	}
	if v, ok, err := f.Take(); !ok || err != nil || v != "v" {
		t.Fatalf("take: %q %v %v", v, err, ok)
	}
	if f.delivery != nil {
		t.Fatal("settled future still retains its payload")
	}
	env.drive()
	if retiredPending(env) != 0 {
		t.Fatal("entry was not reclaimed after the result was transferred")
	}
}

func TestReclaimSkipsPinnedPrefix(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, func(c *Config) { c.Shards = 1; c.Runtime.RoundWorkLimit = 10 })
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
	if n := env.c.c.shards[0].retiredCount.Load(); n != 3 {
		t.Fatalf("unheld entries starved behind pinned prefix: still retired=%d, want only the 3 pinned", n)
	}
	for _, h := range pins {
		h.Release()
	}
	closeManual(t, env.c, env.ex)
}

func TestSweepStepCoversEmptySlotsAndWraps(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	c, s := env.c.c, env.c.c.shards[0]
	// 时钟没走：没有欠账，allowance 不是扫描下限（维护被频繁唤醒时不能多扫）。
	if work := c.sweepStep(s, 3, false); work != 0 || s.sweepCursor != 0 {
		t.Fatalf("a woken-up shard scanned without owed time: work=%d cursor=%d", work, s.sweepCursor)
	}
	// 空槽也计入扫描量：走过 1/4 个周期扫 1/4 的槽。
	n := len(s.idx.words)
	env.clk.Advance(c.cfg.Runtime.SweepPeriod / 4)
	if work := c.sweepStep(s, 3, false); work < n/4-1 || work > n/4+1 || s.sweepCursor != work {
		t.Fatalf("quarter period scanned %d of %d slots (cursor %d)", work, n, s.sweepCursor)
	}
	// 关闭模式一圈扫完整张索引并回绕。
	if work := c.sweepStep(s, 0, true); work != len(s.idx.words) {
		t.Fatalf("closing sweep must cover the whole index: %d/%d", work, len(s.idx.words))
	}
}

func TestInvalidNumericInputsRejected(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Heat.Delta = -time.Second },
		func(c *Config) { c.Shards = math.MaxInt },
		func(c *Config) { c.Namespaces["t"] = NamespaceConfig{TTL: time.Minute, Tin: math.Inf(1)} },
		func(c *Config) { c.Memory.LoadFactor = math.NaN() },
		func(c *Config) { c.Limits.MaxOwnedPinsPerEntry = -1 },
	} {
		cfg := validConfig()
		mutate(&cfg)
		if _, err := NewBytes[string](cfg); err == nil {
			t.Fatal("invalid numeric input was accepted")
		}
	}
}

func TestOnRequestRejectsForeignContext(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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
	t.Logf("stats=%+v admissions=%d nsDrain=%v state=%d", env.c.Stats(), env.c.c.admissions.Load(), env.c.c.nsCtlDraining.Load(), env.c.c.state.Load())
	for _, s := range env.c.c.shards {
		t.Logf("shard %d owner=%d residents=%d retired=%d flights=%d fill=%d control=%d", s.id, s.ownerState.Load(), s.residentCount.Load(), s.retiredCount.Load(), s.flightCount.Load(), len(s.fillQueue), len(s.controlQueue))
	}
	t.Fatal("close did not reclaim")
}

func TestOwnedRetryCountsOneLogicalRequest(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, func(c *Config) { c.Limits.MaxOwnedHandles = 1 })
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
	if after.Hits-before.Hits != 1 {
		t.Fatalf("retry duplicated one logical request: before=%+v after=%+v", before, after)
	}
}

// 借用视图占用读者槽：同一分片并发持有数超过 ReaderSlotsPerShard 时，多出的读者得到 ErrBusy 并计数；
// 释放后立即恢复。默认值足以容纳数百个慢处理函数同时持有同一热 key 的视图。
func TestBorrowedViewsBeyondReaderSlotsGetBusy(t *testing.T) {
	const slots = 4
	env := newTestEnv(t, NamespaceConfig{TTL: time.Hour, Tin: 1}, func(c *Config) {
		c.Shards, c.Limits.ReaderSlotsPerShard = 1, slots
	})
	if _, err := env.fetch("hot", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	var held [slots]Borrowed[string]
	var reqs [slots]Request[string]
	for i := range held {
		env.ns.BeginRequestOn(&reqs[i], "hot")
		if ok, err := env.ns.BorrowOn(&reqs[i], &held[i]); !ok || err != nil {
			t.Fatalf("holder %d: ok=%v err=%v", i, ok, err)
		}
	}
	var r Request[string]
	var b Borrowed[string]
	env.ns.BeginRequestOn(&r, "hot")
	if _, err := env.ns.BorrowOn(&r, &b); !errors.Is(err, ErrBusy) {
		t.Fatalf("holder %d beyond the slots: err=%v, want ErrBusy", slots+1, err)
	}
	if got := env.c.Stats().PinSlotsExhausted; got != 1 {
		t.Fatalf("PinSlotsExhausted = %d, want 1", got)
	}
	held[0].Release()
	env.ns.BeginRequestOn(&r, "hot")
	if ok, err := env.ns.BorrowOn(&r, &b); !ok || err != nil {
		t.Fatalf("after a release: ok=%v err=%v", ok, err)
	}
	b.Release()
	for i := 1; i < slots; i++ {
		held[i].Release()
	}
	if cfg := (Config{}); true {
		cfg.applyDefaults()
		if cfg.Limits.ReaderSlotsPerShard < 1024 {
			t.Fatalf("default ReaderSlotsPerShard = %d, want at least 1024", cfg.Limits.ReaderSlotsPerShard)
		}
	}
}
