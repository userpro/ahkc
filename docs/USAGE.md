# Usage guide

[中文](USAGE.zh.md) | **English**

[Quick start](../README.md) · [Hot-key statistics](OBSERVABILITY.md) · [Architecture](../ARCHITECTURE.md) · [Example index](../example/README.md)

## API overview

Generic instance: `Cache[V]` / `Namespace[V]` (`V` is the value type; the key is always a full string, and the namespace only isolates, it is not concatenated into the key).

### Read-only queries (no load)

```go
v, ok, err := ns.Get(key)        // returns a plain Go value; retention is the caller's responsibility
b, ok, err := ns.GetBorrowed(key) // epoch-bound borrowed view: call b.Release() when done
h, ok, err := ns.GetOwned(key)   // owned handle: hold across threads/async, call h.Release() when done
```

- `ok=true`: hit; `ok=false, err=nil`: definitely absent / expired / generation mismatch;
- `err=ErrBusy`: temporary contention (not a definite absence), may be retried later;
- `err=ErrClosed`: the instance has stopped accepting work.

A borrowed view may only be used within a synchronous scope on a single goroutine; it must not cross an await and must not be copied by value. `BorrowOn(r, &b)` and `BeginRequestOn(&r, key)` use caller-provided storage and avoid allocating a borrowed handle; `Release` before reuse, and reusing without releasing returns `ErrInvalidRequest`. `b.Clone()` / `h.Clone()` create independent owned handles, bounded by the per-entry pin limit and the global `MaxOwnedHandles`. `Value()` on a nil or released handle returns the zero value, `Release()` is idempotent, and `Clone()` returns `ErrCanceled`.

`GetOwnedOn(r)` supports bounded BUSY retries on one logical request without duplicate sampling or hit counting; the host controls retries and every attempt still has a library-side limit.

### Query and load (singleflight)

```go
v, err := ns.Fetch(ctx, key, loader)         // synchronous
fut, err := ns.FetchAsync(key, loader)       // asynchronous: fut.Wait(ctx) / Take() (V, ok, err) / Cancel(); exactly one must finalize it
```

- Concurrent misses are merged into one real origin load; the waiter count is bounded by `MaxWaitersPerKey/MaxWaitersTotal`;
- `err=ErrOverloaded`: I/O concurrency / waiter / response-byte budget exhausted (no queuing, no blocking; back off in business code);
- `err=ErrTimeout`: `LoadTimeout` exceeded (**the underlying I/O is not cancelled**; late results are not used for filling, so clean up on your own);
- `ctx` cancellation releases only the caller's own wait and does not affect other waiters or the underlying I/O;
- **A successful response does not mean the value was cached**: admission and benefit decisions finish in the background handoff.

`ok=false` from `Take` only means it is still waiting; a cancelled future returns `ok=true, ErrCanceled`, and an already taken one returns `ok=true, ErrConsumed`. `Wait` returns a result that was already ready at the time of the call first; `Wait(nil)` returns `ErrNilContext` without consuming the result. A nil loader may only be used for cache hits; on a miss it returns `ErrNilLoader` and starts no load.

Synchronous `Fetch` / `Invalidate` / `InvalidateNamespace` check ctx before acceptance: nil returns `ErrNilContext`, and an already cancelled ctx returns an error matching both the library error and `ctx.Err()` without performing new work. After acceptance, cancellation only stops the current wait, and an accepted invalidation still completes.

### Request: one logical request

```go
var request ahkc.Request[string]
if err := ns.BeginRequestOn(&request, key); err != nil { panic(err) }
r := &request
v, ok, err := ns.GetOn(r)
v, err = ns.FetchOn(ctx, r, loader)
```

"Query then load" within the same business request should pass the same `Request` (the `Get(key)`/`Fetch(...)` convenience forms create their own internally).
A `Request` must not be copied or used concurrently; `go vet` detects by-value copies. After a request finishes, `BeginRequestOn` can reinitialize it; a failed initialization clears the old request to prevent accidentally using the old key. Passing the request to another namespace returns `ErrInvalidRequest`.

### Invalidation and namespaces

```go
h, err := ns.InvalidateAsync(key)      // not accepted surfaces as ErrBusy (control queue full) / ErrClosed / ErrKeyTooLarge; a completion receipt if accepted
err = ns.Invalidate(ctx, key)          // synchronously waits for confirmation
err = c.InvalidateNamespace(ctx, name) // advances the generation: the old generation is logically hidden immediately, removed in background batches
nh, err := c.InvalidateNamespaceAsync(name)
ns2, err := c.RegisterNamespace(name, nsCfg)
```

**Confirmation semantics**: an invalidation confirmation means logical invalidation is complete — after confirmation no new query may obtain the old readable version; it does not wait for physical reclamation.

Re-registering returns `ErrNamespaceExists`; exceeding `MaxNamespaces` returns `ErrOverloaded`; looking up an unregistered name returns `ErrNoSuchNamespace`. Methods on a nil / zero-value Cache, Namespace, or async receipt return `ErrUninitialized`; `On` queries still use `ErrInvalidRequest` for an invalid request. Read-only statistics return an empty snapshot, and releasing or shutting down an empty object has no effect.

**Granularity note**: single-key invalidation advances the `fillFence` of the **shard** it belongs to, so all in-flight fill candidates in that shard are voided together (rejected as `STALE`; business responses are unaffected and the next access reloads). Frequent single-key invalidation repeatedly voids in-flight fills in the same shard.

### Shutdown (three stages)

```go
c.Shutdown()                           // stop accepting immediately (new requests get ErrClosed), no waiting
err := c.WaitDrained(ctx)              // all accepted control / fill / real I/O / responses have finished
err = c.WaitReclaimed(ctx)             // readers, owned handles, retired records, and budget all cleared
err = c.Close(ctx)                     // = Shutdown + WaitReclaimed; an expired ctx returns ErrCloseIncomplete (also satisfying errors.Is(err, ctx.Err()))
p := c.CloseProgress()                 // non-blocking snapshot: p.Stopped / p.Drained / p.Reclaimed
```

- Unfinished real I/O blocks RECLAIMED: Close reports a ctx error but **does not force I/O to end**;
- Waiters finish with `ErrClosed`; late results get only bounded state cleanup.
- `Close(nil)` returns `ErrNilContext` and does not stop the instance; a non-nil ctx that is already cancelled still stops acceptance first. A completed shutdown or invalidation confirmation can be awaited repeatedly and returns completion even if ctx is cancelled.

### Metrics

```go
st := c.Stats()  // hits/misses/BUSY/overload, loads and merges, fill publications and rejections by reason, budget headroom, dropped window notifications, etc.
limits := c.MemoryLimits() // sum of fixed structures, auxiliary state, and classified byte caps
credits, ok := c.Credits() // consistent Granted + Free + InTransit credit ledger; ok=false on contention
```

`Stats().FillRejected` is nil when there are no rejection records; it can be read or ranged over directly, but you must allocate the map yourself before writing to it. Non-empty maps and exported hot-key snapshots are owned solely by the caller, and modifications do not change internal state.

## Configuration

Internal resource bounds are fixed at initialization: arrays, pools, queues, iteration counts, and work budgets all come from `Config`, and work is accepted at runtime within those budgets.
When an initialization constraint is not satisfied (§8.2), `New` fails immediately (for example `Grace >= 2Δ`, `ColdFor >= Δ`, `Tout < Tin`, or a timing wheel span that does not cover `MaxTTL`).

`Config` itself contains only common items such as capacity, heat/estimate semantics, load deadlines, and host resources; structure sizes for the index / gate / sketch / timing wheel / queues / various budgets / maintenance quotas live in the embedded `Config.Advanced`, all with defaults derived from the core configuration.
When reading you can write `cfg.WheelTick` directly (field promotion); when constructing a literal you must write `Advanced: ahkc.Advanced{WheelTick: ...}`. Fields marked ★ in the table below are `Advanced` fields. Full fields and defaults are documented in [config.go](../config.go) comments:

| Group | Field | Default | Meaning |
| --- | --- | --- | --- |
| Capacity | `MaxEntries` / `MaxResidentBytes` | required in cache mode | Global entry/byte quota, split per shard plus a global free pool; may be omitted in ObserveOnly |
| Capacity | `MaxKeyBytes` / `MaxEntryBytes` / `MaxResultBytes` | 512 / 1MiB / 4MiB | Full-key cap, per-entry billed cap, per-load result cap |
| Capacity | `ShardCount` | 16 | Number of shards (rounded up to a power of two) |
| Heat | `HeatDelta` / ★`HeatCap` / ★`GateRows×GateWidth` / ★`GateEpsilon+GateDelta` | 1s / 4×maxTin / 4×4096 / unset | Heat bucket width, saturation cap, gate size; ε/δ are sizing hints and do not carry the original CMS error guarantee |
| Admission | `NamespaceConfig.Tin/Tout` | 3 / 1.5 | Entry/exit thresholds (hysteresis) |
| Estimation | `SampleP` / `NMin` / `HorizonW` | 1/1024 / 8 / 60s | Sampling probability, minimum sample count, prediction horizon |
| Replacement | ★`SieveScanLimit` / ★`VictimLimit` / ★`MaxSubsetChecks` | 64 / 8 / 256 | SIEVE scan limit, candidate set size, subset check limit |
| Loading | `MaxConcurrentLoads` / `LoadTimeout` / `HandoffTimeout` | 256 / 10s / 2s | Real I/O concurrency, cache-side deadline, handoff deadline |
| Waiting | ★`MaxWaitersPerKey` / ★`MaxWaitersTotal` | 32 / 16384 | Waiter limits |
| Holding | ★`MaxOwnedHandles` | `MaxWaitersTotal` | Total owned handles; each entry also has a pin limit |
| Full memory | `MaxManagedBytes` / ★`MaxModelBytes` | derived automatically / 64KiB | Fixed structures + bounded auxiliary state + all classified bytes; retention cap for each model and snapshot |
| Budget | ★`PendingBytes` / ★`ResponseBytes` / ★`RetiredBytes` / ★`RetiredRecords` | B/4 / `MaxConcurrentLoads`×`MaxResultBytes` / B/4 / 4096 | Handoff/response/retirement budget; the response budget is consistent with load concurrency by default |
| Timing wheel | ★`WheelTick` / ★`WheelSlots` / ★`WheelLevels` | 100ms / 64 / 4 | Total span must cover MaxTTL plus lag margin |
| Maintenance | ★`RoundWorkLimit` / ★`RoundSoftTimeBudget` | 512 / 1ms | Events per round and soft time budget (§7.1 long-tail constraint) |
| Maintenance | `FillRatePerSec` / `FillBytesPerSec` / `RetireBytesPerRound` / ★`RetireLimitPerRound` / ★`ReclaimBytesPerRound` | see Config | Fill entry/byte rates and per-round unlink/reclaim byte quotas (`RetireBytesPerRound` lives on `Config` itself) |
| Scheduling | ★`MaxExecutorTasks` / ★`PeriodicWorkLimit` | derived from fixed capacity / 512 | Total accepted unfinished callbacks; per-tick shard/flight/control access quota |
| Observation | `ObserveOnly` / ★`ObserveHotKeys` / ★`ObserveRequestKeys` / ★`ObserveRequestSampleP` | false / 128 in observe mode / 0 / 1/1024 | Observe without caching; origin-load and request hot-key tracking slots (per shard), request hot-key sampling rate |
| Observation | ★`HotHistoryWindows` / ★`HotHistoryInterval` / ★`HotHistoryTopN` / ★`OnHotWindow` / ★`HotKeyLabel` | 0 / `HeatDelta` / 16 / nil / nil | Window history, window width, ranking size, notification callback, export mapping (see the observability document) |
| Host | `Executor` / `Clock` | goroutine / monotonic clock | See below |

### Host executor contract (important)

```go
type Executor interface { TrySubmit(task func()) bool }
```

- **Accepted**: the task runs at most once; **rejected**: it does not run; **must not run on the caller or execute inline recursively** (that would break the boundedness and lock-order assumptions);
- The default executor uses one goroutine per task; when integrating a business thread pool, return `false` honestly when the pool is full;
- With `DisableTicker=true` the built-in periodic trigger is off, and the host must call `c.MaintenanceStep()` periodically (no slower than `PeriodicInterval`, default 25ms). Idle TTL removal/cooldown, `LoadTimeout` expiry notifications, quota transfer, retirement reclamation (including `Close` reaching RECLAIMED), and window history all depend on it; `Future.Wait` has no timer of its own and only reacts to result readiness and `ctx`.

ObserveOnly does not require a fictitious resident capacity or namespace TTL: when omitted, `ShardCount`, `MaxEntryBytes`, and `MaxTTL` are used as internal budget defaults; explicitly negative values still return `ErrInvalidConfig`.

### Billing (Weigher)

```go
c, _ := ahkc.New[MyValue](cfg, ahkc.WithWeigher(func(key string, v MyValue) int64 {
	return int64(len(key)) + v.BackingBytes() + ahkc.EntryOverhead[MyValue]()
}))
```

`DefaultWeigher` supports strings with full backing storage, `[]byte`, and flat values without references. Substrings, subslices, or pointer object graphs must supply a custom Weigher (without one, `New` returns `ErrNoWeigher` for value types containing pointers; when the weigher returns a non-positive weight the load returns `ErrInvalidWeight`), and the full retained backing capacity must be counted; accounting for shared storage must be additive across entries. Billing happens once after load completion, outside internal locks, and every successful result is size-checked, including cold keys and ObserveOnly.

If `MaxManagedBytes` is smaller than the planned layout, `New` refuses to create the instance before allocating pools. `MemoryLimits` is a retained upper bound derived from the host billing and model-size contracts; it is not the Go heap or process RSS. Plain values held by the caller after receipt, host executor storage, loader temporary space, and uncollected GC garbage are managed by the host. Fixed pools plus accepted callbacks, control, handles, and in-transit grants each have capacity limits.

## Runtime semantics and contracts

| Observation | Semantics |
| --- | --- |
| `Get` returns `ok=false, err=nil` | Definite miss after a full check over the fixed probe domain |
| `ErrBusy` | Temporary contention (protection slot/gate/token), **not a definite absence**, retryable |
| `ErrOverloaded` | A classified budget is exhausted; an unaccepted operation has no effect |
| `ErrKeyTooLarge` | The full key exceeds `MaxKeyBytes` (default 512); consistent across all entry points |
| `ErrExhausted` | The identity sequence or cache clock range is exhausted; the instance refuses further work |
| `Fetch` succeeds | The business result was delivered; **it does not mean the value was cached** |
| `Get` in `ObserveOnly` mode | Returns miss when open and a protection slot is available; may still return `ErrBusy` / `ErrClosed`; fills are always rejected and counted as `OBSERVE_ONLY` |
| `Fetch` returns a loader error | Errors are not cached in the first version; `PanicError` wraps a loader panic |
| Loader result > `MaxEntryBytes` and ≤ `MaxResultBytes` | The business result is delivered as usual; only caching is rejected |
| Loader result > `MaxResultBytes` | Violates the result size contract; delivers `ErrResultTooLarge` and increments a counter |
| `LoadTimeout` | Cache-side deadline; the underlying I/O is not cancelled and waiters get `ErrTimeout` |
| Invalidation confirmation | Logical invalidation is complete; physical reclamation happens in the background |
| Observation history / notification anomalies | See [hot-key statistics troubleshooting](OBSERVABILITY.md#overhead-and-troubleshooting) |
| `Close` returns nil | RECLAIMED: resident/retired/handoff counters are zero and all classified budgets are returned |

## Pitfalls and troubleshooting

| Observation | Cause | Action / investigation |
| --- | --- | --- |
| For a while after creating a cache, `FillPublished` is 0 and `FillRejected` is mostly `NO_ESTIMATE` | The benefit sketch needs `2×ValueDelta` (default 20s) after creation to warm up; before warm-up only `Prior` can produce an estimate | Configure `Prior` for the namespace; watch `Stats().FillUnavailable / FillPrior / FillMeasured` |
| Medium- and low-frequency keys never get cached (`NO_ESTIMATE`) | `MEASURED` requires the key's **sample count** within a window to be ≥ `NMin`, which needs roughly `NMin/SampleP` requests (about 8192 per window by default) | Configure `Prior`; raise `SampleP` or lower `NMin` according to traffic |
| Once the cache is full, new keys almost never replace old ones (`NON_POSITIVE_GAIN`) | With only `Prior`, every key has the same λ, so when both candidate and victim have remaining TTL ≥ `HorizonW` the benefits are equal and `ΔG=0`; behavior is unaffected before the cache is full | Let estimates reach `MEASURED`, or use a cost model that distinguishes key value |
| After `Invalidate` on one key, fills of other keys in the same shard show `STALE` | `fillFence` is per shard, so invalidation voids all in-flight fills in that shard | A deliberate design trade-off; avoid frequent invalidation of a single key or reduce the in-flight fill window |
| With `DisableTicker`, waiters do not time out, `Close` does not return, and expired entries are not released | All of these progressions depend on periodic calls | The host calls `MaintenanceStep` no slower than `PeriodicInterval`; callers also pass a `ctx` with a timeout |
| Treating `ErrBusy` as a miss and loading from origin | `ErrBusy` only means this attempt could not decide | Retry a bounded number of times on the same `Request` |
| A waiter slot is not released after `FetchAsync` | The `Future` was never finalized | Always use one of `Wait` / `Take` (ok=true) / `Cancel` |
| `New` returns `ErrNoWeigher` for a custom value type | `DefaultWeigher` does not guess reference object graphs | Use `WithWeigher` and bill the full backing storage |

## Cost model and benefit estimation

Admission and replacement use the same benefit formula: `G = (r̂×ĉ − ĥ) × min(W, remaining TTL)`, applied equally to candidates and residents.

- Three estimate states: **MEASURED** (samples ≥ `NMin`) / **PRIOR** (namespace prior, invalid once expired) / **UNAVAILABLE** (new fills deferred; resident entries unaffected);
- Dropped samples or sample lag beyond the limit enter a **contamination window** (2Δv): estimates are UNAVAILABLE during that time, and dirty data is never used silently;
- Cost model interface:

```go
type CostModel interface {
	Version() uint64 // monotonically increasing; model switches record the version to prevent mixing units
	Lookup(lambda float64, costClass uint32) (r, c, h, k, K float64, ok bool)
}
```

- Built-in default: `hitCountModel` (targets request hit count, G = λ̂·min(W,τ));
- `CostTableModel`: fixed-class lookup plus EWMA aging. The avoidable load rate must come from an explicitly calibrated `MergeFactor` or `RateBands`; the latter looks up by request rate and load duration and becomes UNAVAILABLE when out of range. Missing calibration never silently assumes r=λ.
- `NewCostTableModel(cfg)` returns `(*CostTableModel, error)`; the error must be checked. Invalid sizes, values, duplicate grids, or missing merge-rate calibration return `ErrInvalidConfig` immediately instead of producing an unusable model. Old single-return call sites must migrate to `model, err := ...`, see the [full example](../example/advanced/main.go).
- By default loader wall-clock seconds (including waiting) are observed automatically; for CPU units set `DisableAutoObserve: true` and record cost and load duration separately with `ObserveLoad(class, cpuCost, wallDuration)`. Namespaces taking part in the same decision must use the same unit; `UnitCostModel` can declare a unit.
- A decision freezes the current time, prior identity, model identity, and version; `SnapshotCostModel` can provide an immutable snapshot. `SetModel(m)` / `SetPrior(p)` update atomically, copy the prior, and return errors; `SetModel(nil)` restores the built-in hit-count target, and `SetPrior(nil)` disables prior fallback. Both configuration and update validation can be checked with `errors.Is(err, ErrInvalidConfig)`. External calibrated models are bounded by `ModelMaxAge`, while the built-in hit-count target needs no calibration.
- `WithResultMetadata` can disable caching per result, shorten the hard deadline, or set a limited cost class; `HardDeadline` uses the cache Clock's monotonic nanoseconds and cannot extend the namespace TTL. `NewMonotonicClock()` shares the same time origin with the default cache and default cost model; with the default clock you can express a relative deadline as `clock.Now() + int64(ttl)`, while a custom Clock must share that instance.

Value copying is Go's shallow copy; slices, maps, pointers, and interfaces may still share storage internally. Keep values immutable after publication, and copy explicitly when business code needs to modify them.
