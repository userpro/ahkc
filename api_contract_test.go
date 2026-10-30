package ahkc

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRequestResetFailureInvalidatesOldKey(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	defer closeManual(t, env.c, env.ex)
	if _, err := env.fetch("old", okLoader("value")); err != nil {
		t.Fatal(err)
	}
	var r Request[string]
	if err := env.ns.BeginRequestOn(&r, "old"); err != nil {
		t.Fatal(err)
	}
	if err := env.ns.BeginRequestOn(&r, strings.Repeat("x", env.c.c.cfg.MaxKeyBytes+1)); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatal(err)
	}
	if v, _, err := env.ns.GetOn(&r); v != "" || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("stale request: %q %v", v, err)
	}
	if r.key != "" {
		t.Fatal("failed request retained its old key")
	}
	if err := env.ns.BeginRequestOn(&r, "old"); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := env.ns.GetOn(&r); err != nil || !ok || v != "value" {
		t.Fatalf("reinitialized request: %q %v %v", v, ok, err)
	}
	env.c.Shutdown()
	if err := env.ns.BeginRequestOn(&r, "new"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, _, err := env.ns.GetOn(&r); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
}

func TestSettledHandlesReleaseCache(t *testing.T) {
	for _, mode := range []string{"owned", "future_hit", "future_load", "future_cancel", "control_key", "control_namespace"} {
		t.Run(mode, func(t *testing.T) {
			done := make(chan struct{})
			var kept any
			func() {
				env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
				defer closeManual(t, env.c, env.ex)
				sentinel := new([1024]byte)
				runtime.SetFinalizer(sentinel, func(*[1024]byte) { close(done) })
				env.c.c.metadata = func(string, string) ResultMetadata {
					runtime.KeepAlive(sentinel)
					return ResultMetadata{}
				}
				if strings.HasPrefix(mode, "control_") {
					var h *ControlHandle
					var err error
					if mode == "control_key" {
						h, err = env.ns.InvalidateAsync("key")
					} else {
						h, err = env.c.InvalidateNamespaceAsync("t")
					}
					if err != nil {
						t.Fatal(err)
					}
					env.drive()
					if err := h.Wait(context.Background()); err != nil {
						t.Fatal(err)
					}
					kept = h
					return
				}
				if mode == "owned" || mode == "future_hit" {
					if _, err := env.fetch("key", okLoader("v")); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "owned" {
					h, hit, err := env.ns.GetOwned("key")
					if err != nil || !hit {
						t.Fatal("owned miss", err)
					}
					h.Release()
					kept = h
					return
				}
				f, err := env.ns.FetchAsync("key", okLoader("v"))
				if err != nil {
					t.Fatal(err)
				}
				if mode == "future_cancel" {
					f.Cancel()
				} else {
					env.drive()
					if _, err := f.Wait(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				kept = f
			}()
			deadline := time.Now().Add(2 * time.Second)
			for {
				runtime.GC()
				select {
				case <-done:
					runtime.KeepAlive(kept)
					return
				default:
				}
				if time.Now().After(deadline) {
					t.Fatal("settled handle still retains the closed cache")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestInvalidAndCanceledContextsDoNotAdmitWork(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	defer closeManual(t, env.c, env.ex)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		ctx  context.Context
		want error
	}{{nil, ErrNilContext}, {canceled, context.Canceled}} {
		if _, err := env.ns.Fetch(tc.ctx, "key", okLoader("v")); !errors.Is(err, tc.want) {
			t.Fatal(err)
		}
		if err := env.ns.Invalidate(tc.ctx, "key"); !errors.Is(err, tc.want) {
			t.Fatal(err)
		}
		if err := env.c.InvalidateNamespace(tc.ctx, "t"); !errors.Is(err, tc.want) {
			t.Fatal(err)
		}
	}
	env.drive()
	st := env.c.Stats()
	if st.Loads != 0 || st.KeyInvalidations != 0 || st.NamespaceInvalidations != 0 {
		t.Fatalf("invalid calls performed work: %+v", st)
	}
	if err := env.c.Close(nil); !errors.Is(err, ErrNilContext) || env.c.CloseProgress().Stopped {
		t.Fatal("nil Close changed cache state", err)
	}
	if _, err := env.ns.FetchAsync("missing", nil); !errors.Is(err, ErrNilLoader) {
		t.Fatal(err)
	}
	if st := env.c.Stats(); st.Loads != 0 || st.IOInFlight != 0 || st.LoadErrors != 0 {
		t.Fatalf("nil loader was admitted: %+v", st)
	}
}

func TestFutureTerminalPollingAndNilContext(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	defer closeManual(t, env.c, env.ex)
	f, err := env.ns.FetchAsync("key", okLoader("v"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ready, err := f.Take(); ready || err != nil {
		t.Fatal("pending future reported terminal", err)
	}
	env.drive()
	if _, err := f.Wait(nil); !errors.Is(err, ErrNilContext) {
		t.Fatal(err)
	}
	if v, err := f.Wait(context.Background()); err != nil || v != "v" {
		t.Fatalf("invalid wait consumed result: %q %v", v, err)
	}
	for i := 0; i < 3; i++ {
		if _, ready, err := f.Take(); !ready || !errors.Is(err, ErrConsumed) {
			t.Fatalf("consumed future looked pending: %v %v", ready, err)
		}
	}
	// A nil loader is harmless for an existing cached value.
	if v, err := env.ns.Fetch(context.Background(), "key", nil); err != nil || v != "v" {
		t.Fatal(v, err)
	}
}

func TestConcurrentFutureTakeReportsOneResult(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	defer closeManual(t, env.c, env.ex)
	f, err := env.ns.FetchAsync("key", okLoader("v"))
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ready, err := f.Take()
			if !ready || (err == nil && v != "v") {
				results <- ErrBusy
			} else {
				results <- err
			}
		}()
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, ErrConsumed) {
			t.Fatal("terminal future looked pending", err)
		}
	}
	if success != 1 {
		t.Fatalf("delivered %d results", success)
	}
}

func TestCachedFuturesHaveIndependentLifetimes(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	defer closeManual(t, env.c, env.ex)
	if _, err := env.fetch("key", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		first, err := env.ns.FetchAsync("key", nil)
		if err != nil {
			t.Fatal(err)
		}
		second, err := env.ns.FetchAsync("key", nil)
		if err != nil {
			t.Fatal(err)
		}
		first.Cancel()
		if _, ready, err := first.Take(); !ready || !errors.Is(err, ErrCanceled) {
			t.Fatal("canceled future was not terminal", err)
		}
		if v, err := second.Wait(context.Background()); err != nil || v != "v" {
			t.Fatal("another future was affected", v, err)
		}
	}
	if a := testing.AllocsPerRun(1000, func() {
		f, err := env.ns.FetchAsync("key", nil)
		if err != nil {
			t.Fatal(err)
		}
		f.Cancel()
	}); a > 2 {
		t.Fatalf("cached future allocated %.0f objects, want at most 2", a)
	}
}

func TestCompletedConfirmationWinsOverCanceledContext(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute}, nil)
	defer closeManual(t, env.c, env.ex)
	h, err := env.ns.InvalidateAsync("key")
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 100; i++ {
		if err := h.Wait(ctx); err != nil {
			t.Fatal("confirmed operation was reported canceled", err)
		}
	}
	if err := h.Wait(nil); !errors.Is(err, ErrNilContext) {
		t.Fatal(err)
	}
	closeManual(t, env.c, env.ex)
	if err := env.c.Close(ctx); err != nil {
		t.Fatal("completed Close was not idempotent", err)
	}
}

func TestUninitializedPublicHandlesAreSafe(t *testing.T) {
	ctx := context.Background()
	for _, c := range []*Cache[string]{nil, {}} {
		c.Shutdown()
		c.MaintenanceStep()
		_ = c.Stats()
		_ = c.MemoryLimits()
		_ = c.HotKeys(1)
		_ = c.HotKeyHistory(1)
		if _, lost := c.HotWindowsSince(0); lost != 0 {
			t.Fatal("uninitialized history lost data")
		}
		for _, err := range []error{c.Close(ctx), c.WaitDrained(ctx), c.WaitReclaimed(ctx)} {
			if !errors.Is(err, ErrUninitialized) {
				t.Fatal(err)
			}
		}
		if _, err := c.Namespace("t"); !errors.Is(err, ErrUninitialized) {
			t.Fatal(err)
		}
		if _, err := c.RegisterNamespace("t", NamespaceConfig{}); !errors.Is(err, ErrUninitialized) {
			t.Fatal(err)
		}
		if _, err := c.InvalidateNamespaceAsync("t"); !errors.Is(err, ErrUninitialized) {
			t.Fatal(err)
		}
	}
	for _, n := range []*Namespace[string]{nil, {}} {
		if _, _, err := n.Get("key"); !errors.Is(err, ErrUninitialized) {
			t.Fatal(err)
		}
		if _, _, err := n.GetOn(&Request[string]{}); !errors.Is(err, ErrInvalidRequest) {
			t.Fatal(err)
		}
		if _, err := n.InvalidateAsync("key"); !errors.Is(err, ErrUninitialized) {
			t.Fatal(err)
		}
		if _, ok := n.Heat("key"); ok {
			t.Fatal("uninitialized heat succeeded")
		}
	}
	for _, f := range []*Future[string]{nil, {}} {
		f.Cancel()
		_ = f.Deadline()
		if _, ready, err := f.Take(); !ready || !errors.Is(err, ErrUninitialized) {
			t.Fatal(ready, err)
		}
		if _, err := f.Wait(ctx); !errors.Is(err, ErrUninitialized) {
			t.Fatal(err)
		}
	}
	for _, h := range []*ControlHandle{nil, {}} {
		if err := h.Wait(ctx); !errors.Is(err, ErrUninitialized) {
			t.Fatal(err)
		}
	}
	var owned *Handle[string]
	owned.Release()
	if owned.Value() != "" {
		t.Fatal("nil owned value")
	}
	if _, err := owned.Clone(); !errors.Is(err, ErrCanceled) {
		t.Fatal(err)
	}
	var borrowed *Borrowed[string]
	borrowed.Release()
	if borrowed.Value() != "" {
		t.Fatal("nil borrowed value")
	}
	if _, err := borrowed.Clone(); !errors.Is(err, ErrCanceled) {
		t.Fatal(err)
	}
}

func TestModeObserveMinimalConfiguration(t *testing.T) {
	ex := NewManualExecutor()
	c, err := NewBytes[string](Config{
		Mode:       ModeObserve,
		Namespaces: map[string]NamespaceConfig{"t": {}},
		Runtime: RuntimeConfig{
			DisableTicker: true,
			Executor:      ex,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeManual(t, c, ex)
	n, err := c.Namespace("t")
	if err != nil {
		t.Fatal(err)
	}
	if _, hit, err := n.Get("key"); err != nil || hit {
		t.Fatal(hit, err)
	}
	if heat, ok := n.Heat("key"); !ok || heat != 1 {
		t.Fatal(heat, ok)
	}
}

func TestAllocationFreeRejectedAndEmptyQueries(t *testing.T) {
	e := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, func(c *Config) { c.Limits.MaxWaitersTotal = 1; c.Limits.MaxOwnedHandles = 4 })
	defer closeManual(t, e.c, e.ex)
	if _, err := e.fetch("key", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	for name, query := range map[string]func(){
		"borrowed miss": func() {
			if _, hit, err := e.ns.GetBorrowed("absent"); hit || err != nil {
				t.Fatal(hit, err)
			}
		},
		"empty rejection stats": func() { _ = e.c.Stats() },
		"cancellation error": func() {
			if !errors.Is(contextError(context.Canceled), ErrCanceled) {
				t.Fatal("lost cancellation")
			}
		},
	} {
		if a := testing.AllocsPerRun(1000, query); a != 0 {
			t.Errorf("%s: %.0f allocations", name, a)
		}
	}
	f, err := e.ns.FetchAsync("key", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Cancel()
	if a := testing.AllocsPerRun(1000, func() {
		if f, err := e.ns.FetchAsync("key", nil); f != nil || !errors.Is(err, ErrOverloaded) {
			t.Fatal(f, err)
		}
	}); a != 0 {
		t.Fatalf("rejected async hit: %.0f allocations", a)
	}
}

func TestOptionConfigurationErrorsAreExplicit(t *testing.T) {
	if c, err := NewBytes[string](validConfig(), nil); c != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatal("nil option accepted", err)
	}
	e := newTestEnv(t, NamespaceConfig{TTL: time.Minute}, func(c *Config) { c.Limits.MaxNamespaces = 1 })
	defer closeManual(t, e.c, e.ex)
	if _, err := e.c.RegisterNamespace("t", NamespaceConfig{TTL: time.Minute}); !errors.Is(err, ErrNamespaceExists) {
		t.Fatal(err)
	}
	if _, err := e.c.RegisterNamespace("new", NamespaceConfig{TTL: time.Minute}); !errors.Is(err, ErrOverloaded) {
		t.Fatal(err)
	}
}

func TestFullControlQueuesRejectWithoutAllocation(t *testing.T) {
	for _, namespace := range []bool{false, true} {
		e := newTestEnv(t, NamespaceConfig{TTL: time.Minute}, func(c *Config) { c.Shards, c.Limits.ControlQueueSize = 1, 1 })
		defer closeManual(t, e.c, e.ex)
		submit := func() (*ControlHandle, error) {
			if namespace {
				return e.c.InvalidateNamespaceAsync("t")
			}
			return e.ns.InvalidateAsync("key")
		}
		h, err := submit()
		if err != nil {
			t.Fatal(err)
		}
		if a := testing.AllocsPerRun(1000, func() {
			if h, err := submit(); h != nil || !errors.Is(err, ErrBusy) {
				t.Fatal(h, err)
			}
		}); a != 0 {
			t.Fatalf("rejected control allocated %.0f objects", a)
		}
		e.ex.Drain(1000)
		if err := h.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
