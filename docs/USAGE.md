# Usage guide

[中文](USAGE.zh.md) | **English**

[Quick start](../README.md) · [Hot-key statistics](OBSERVABILITY.md) · [Architecture](../ARCHITECTURE.md) · [Example index](../example/README.md)

## API overview

Generic instances: `Cache[V]` / `Namespace[V]` (`V` is the value type; a key is always the full string, and a namespace only isolates, it is never concatenated). The value type is fixed by the constructor:

```go
c, err := ahkc.NewBytes[string](cfg)                      // string / []byte (including named types)
c, err := ahkc.NewFlat[Point](cfg)                        // numbers, arrays, pointer-free structs
c, err := ahkc.NewObject[*Doc](cfg, func(key string, v *Doc) int64 {
    return ahkc.EntryHeaderBytes + int64(len(key)) + v.RetainedBytes() // must cover all retained storage, and be > 0
})
```

`NewFlat` checks once, at construction, that the type holds no pointers (otherwise `ErrInvalidConfig`); a `NewObject` `weigh` that returns a value <= 0 makes the load return `ErrInvalidWeight`. The only `Option` for all three is `WithResultMetadata`.

### Read-only lookups (no loading)

```go
v, ok, err := ns.Get(key)         // returns a value the caller owns
b, ok, err := ns.GetBorrowed(key) // borrowed view: call b.Release() when done
h, ok, err := ns.GetOwned(key)    // owned handle: may cross threads/async; call h.Release() when done
```

- `ok=true`: hit; `ok=false, err=nil`: definitely absent, expired, or of another generation;
- `err=ErrBusy`: temporary contention (not a definite miss); retry later;
- `err=ErrClosed`: the instance has stopped admitting work.

**Read contract by layout**

| Layout | `Get` / `Fetch` hit | Borrowed view / handle |
| --- | --- | --- |
| `NewBytes` | copied once inside the guard (1 allocation) | zero-copy, points into the region, **valid only until `Release`** |
| `NewFlat` | value copy, zero allocations | value copy, zero allocations |
| `NewObject` | value copy, zero allocations | value copy, zero allocations |

A borrowed view is for a synchronous scope in one goroutine; it must not cross an await and must not be copied by value. `BorrowOn(r, &b)` with `BeginRequestOn(&r, key)` use caller-provided storage and avoid allocating the borrowed handle; `Release` before reusing, otherwise reuse returns `ErrInvalidRequest`. `b.Clone()` / `h.Clone()` create an independent owned handle, bounded by the per-entry pin limit and the global `MaxOwnedHandles`. `Value()` on a nil or released handle returns the zero value, `Release()` is idempotent, and `Clone()` returns `ErrCanceled`.

An owned handle delays reclaiming the block it points at, so release it promptly; to hold a value long-term use `Get` (a copy). Using a borrowed view after `Close` is an error: the mmap region has been returned and access faults.

`GetOwnedOn(r)` lets a single logical request retry BUSY a bounded number of times without double counting; the host controls the retry, and each attempt inside the library remains bounded.

### Lookup with loading (singleflight)

```go
v, err := ns.Fetch(ctx, key, loader)         // synchronous
fut, err := ns.FetchAsync(key, loader)       // asynchronous: fut.Wait(ctx) / Take() (V, ok, err) / Cancel(); finish with exactly one of them
```

- Concurrent misses are merged into one real origin load; waiters are bounded by `MaxWaitersPerKey/MaxWaitersTotal`;
- `err=ErrOverloaded`: I/O concurrency / waiter / response-byte budget exhausted (no queueing, no blocking; the caller backs off);
- `err=ErrTimeout`: `LoadTimeout` exceeded (**the underlying I/O is not cancelled**; a late result is not filled, and cleanup is your loader's job);
- `ctx` cancellation only releases the caller's own wait, not other waiters or the underlying I/O;
- **A successful response does not mean the value is cached**: admission and benefit decisions are handed off in the background.

`Take` returning `ok=false` only means it is still waiting; a cancelled future returns `ok=true, ErrCanceled`, and a consumed one returns `ok=true, ErrConsumed`. `Wait` prefers a result that is already ready at call time; `Wait(nil)` returns `ErrNilContext` and does not consume the result. A nil loader is only usable for cache hits; on a miss it returns `ErrNilLoader` without starting a load.

Synchronous `Fetch` / `Invalidate` / `InvalidateNamespace` check ctx before admission: nil returns `ErrNilContext`, a cancelled ctx returns an error matching both the library error and `ctx.Err()`, and no new operation runs. Cancelling after admission only stops the current wait; an admitted invalidation still completes.

### Request: one logical request

```go
var request ahkc.Request[string]
if err := ns.BeginRequestOn(&request, key); err != nil { panic(err) }
r := &request
v, ok, err := ns.GetOn(r)
v, err = ns.FetchOn(ctx, r, loader)
```

"Look up, then load" for one business request should pass the same `Request` (the `Get(key)`/`Fetch(...)` convenience forms already create one each). A `Request` must not be copied or used concurrently; `go vet` detects copies by value. After a request finishes it can be re-initialized with `BeginRequestOn`; a failed initialization clears the old request so a stale key is never reused. Passing a request to another namespace returns `ErrInvalidRequest`.

### Load result metadata

```go
ahkc.WithResultMetadata[string](func(key, v string) ahkc.ResultMetadata {
    return ahkc.ResultMetadata{
        NoCache:      false,                // true: deliver only, do not cache
        HardDeadline: clock.Now() + int64(30*time.Second), // may only shorten the namespace TTL
        Cost:         5,                    // origin cost of one load, default 1; finite and >= 0
    }
})
```

`Cost` takes part in eviction valuation: an entry's benefit = heat × `Cost` × min(`Evict.Horizon`, remaining TTL), so a key backed by a big object or a slow origin is less likely to be evicted. Namespaces that take part in one decision should use the same unit. `HardDeadline` uses the cache clock's monotonic nanoseconds and cannot extend the namespace TTL; `NewMonotonicClock()` shares its time origin with the default cache, so under the default clock a relative deadline is `clock.Now() + int64(ttl)`, while a custom Clock must share that instance. The metadata callback runs outside every internal lock and must be bounded and non-blocking.

### Invalidation and namespaces

```go
h, err := ns.InvalidateAsync(key)      // not admitted: ErrBusy (control queue full) / ErrClosed / ErrKeyTooLarge; admitted: a completion handle
err = ns.Invalidate(ctx, key)          // synchronous, waits for confirmation
err = c.InvalidateNamespace(ctx, name) // advances the generation: the old generation is hidden at once, removed in batches in the background
nh, err := c.InvalidateNamespaceAsync(name)
ns2, err := c.RegisterNamespace(name, nsCfg)
```

**Confirmation semantics**: a confirmed invalidation means the logical invalidation is done: after confirmation a new lookup must not obtain the old readable version. It does not wait for physical reclamation.

Registering a name twice returns `ErrNamespaceExists`; exceeding `MaxNamespaces` returns `ErrOverloaded`.

**Granularity**: invalidating one key advances the `fillFence` of its **shard**, and every in-flight fill candidate of that shard is voided (rejected as `STALE`; business responses are unaffected and the next access reloads). Invalidating single keys at a high rate repeatedly voids in-flight fills of the same shard.

### Shutdown (three phases)

```go
c.Shutdown()                           // stop admission at once (new requests get ErrClosed), do not wait
err := c.WaitDrained(ctx)              // all admitted control / fill / real I/O / responses have terminated
err = c.WaitReclaimed(ctx)             // readers, owned handles, retire chain and budgets are clear; the region is returned as a whole
err = c.Close(ctx)                     // = Shutdown + WaitReclaimed; if ctx expires first, ErrCloseIncomplete (also satisfies errors.Is(err, ctx.Err()))
p := c.CloseProgress()                 // non-blocking snapshot: p.Stopped / p.Drained / p.Reclaimed
```

- Unfinished real I/O blocks RECLAIMED: Close reports the ctx error but does **not force I/O to end**;
- Waiters terminate with `ErrClosed`; a late result only gets bounded state cleanup.
- While closing, the slow sweep ignores its period and retires every resident entry in one pass; the retire chain keeps advancing as long as it makes progress, so 300k entries are reclaimed in about 0.3 seconds.
- `Close(nil)` returns `ErrNilContext` and does not stop the instance; a cancelled non-nil ctx still stops admission first. A completed close or invalidation confirmation can be awaited again and returns the completed result even if the ctx is already cancelled.

### Metrics

```go
st := c.Stats()            // hits/misses/BUSY/overload, loads and merges, fills published and rejected by reason, budget headroom, window notifications dropped, ...
limits := c.MemoryLimits() // sum of fixed structures, auxiliary state, the region (RegionBytes), and categorized byte bounds
```

Watch region use through `ArenaUsedBytes` / `ArenaFreeBytes` (their sum is the total entry region) and `RetiredPending` (entries on the retire chain that are not reclaimed yet): used bytes clearly above `ResidentBytes` mean retired blocks are not reclaimed yet (usually an owned handle was never released); plenty of free bytes together with `ALLOCATION_FAILED` means fragmentation (`ArenaLargestFree` is the biggest single free block: when it is far below the size of your largest values, fills are being rejected), so raise `Memory.SlackPercent`. For the source of `ErrBusy` look at `PinSlotsExhausted` (reader slots exhausted: raise `ReaderSlotsPerShard`; one hot key lives on one shard, so adding shards does not help, and a handler that blocks while holding a borrowed view should use `Get` instead) and `PinEpochRetriesExhausted` (the epoch kept advancing while a reader announced itself); if both are 0 and `ErrBusy` still appears, it comes from load merging (stripe lock contention, or a flight for the same key still completing) and is normal bounded retry.

`Stats().FillRejected` is nil when nothing was rejected; you can read or range over it, but allocate a map before writing. Common reasons: `NOT_HOT` (heat below `Tin`), `NON_POSITIVE_GAIN` (net benefit not positive), `ALLOCATION_FAILED` (the region has no large enough free block: fragmentation, or retired blocks not yet reclaimed), `INFEASIBLE` (no index slot, or no feasible subset), `STALE` (an invalidation voided the in-flight fill). `ExpiredHints` counts hints that readers registered after meeting a stale entry and the maintainer consumed.

## Configuration

Internal resource bounds are fixed at initialization: arrays, regions, queues, loop counts and work budgets all come from `Config`, and work is admitted against budgets at run time. A constructor fails outright when a constraint is violated (for example `MaxEntries < Shards`, `Tin` outside (0, 4095), `MaxKeyBytes` above 65535, or a single shard region above 32GiB; a shard count that is not a power of two is rounded up).

`Config` itself holds capacity, mode and namespaces. Everything else is grouped by purpose: `Heat`, `Evict`, `Hot`, `Memory`, `Limits` and `Runtime`. The zero value of every field means "use the default", derived from the core configuration where possible, so a literal only names what it changes: `Config{MaxEntries: n, MaxBytes: b, Memory: ahkc.MemoryConfig{Prefault: true}}`. See the [config.go](../config.go) comments for every field and default:

| Group | Field | Default | Meaning |
| --- | --- | --- | --- |
| Capacity | `MaxEntries` / `MaxBytes` | required in cache mode | global entry/byte quota, split into fixed fair shares per shard (the remainder goes to the first shards); optional in `ModeObserve` |
| Capacity | `MaxKeyBytes` / `MaxEntryBytes` / `MaxResultBytes` | 512 / 1MiB / 4MiB | full-key limit (<=65535), billed bytes per entry, bytes per load result |
| Capacity | `Shards` | 16 | shard count (rounded up to a power of two); 64-256 for hundreds of millions of entries |
| `Memory` | `SlackPercent` | 15 | margin of the entry region above the byte quota; covers fragmentation and retired blocks not yet reclaimed; when it runs out a fill is rejected as `ALLOCATION_FAILED` |
| `Memory` | `LoadFactor` | 0.85 | index load factor; 0.7 uses about 20% more index memory and brings a miss down to about 3 groups and rejected fills below 0.4% |
| `Memory` | `Prefault` / `HugePages` / `Heap` | false | touch index and gate pages at startup; advise transparent huge pages (best with `Prefault`); force regions onto the Go heap (non linux/amd64/arm64 and `-race` builds always use the heap) |
| `Heat` | `Delta` / `GateCells` | 1s / derived from capacity | heat bucket width; gate cells (8 per cache line), default clamp(per-shard entries/2, 16384, 2^24); at scale set it to 4x the distinct missed keys per `Delta` |
| Admission | `NamespaceConfig.Tin` | 3 | admission threshold (windowed request heat), range (0, 4095) |
| `Evict` | `Horizon` / `VictimLimit` | 60s / 8 | benefit horizon; candidate-set limit (<=8, subset checks are bounded by 2^VictimLimit). The SIEVE scan limit is the fixed constant 64 |
| `Limits` | `MaxConcurrentLoads` / `LoadTimeout` / `HandoffTimeout` | 256 / 10s / 2s | real I/O concurrency, cache-side deadline, hand-off deadline (a namespace whose TTL is shorter is capped by that TTL) |
| `Limits` | `MaxWaitersPerKey` / `MaxWaitersTotal` | 32 / 16384 | waiter limits |
| `Limits` | `MaxOwnedHandles` / `MaxOwnedPinsPerEntry` | `MaxWaitersTotal` / 1024 | total owned handles; pin limit per entry |
| `Limits` | `MaxManagedBytes` | derived | fixed structures + bounded auxiliary state + region + every byte category; the constructor fails if it is set below the planned layout |
| `Limits` | `PendingBytes` / `ResponseBytes` | quota/4 / `MaxConcurrentLoads`×`MaxResultBytes` | hand-off and response budgets. A load reserves `MaxResultBytes` of the response budget when it starts and shrinks the reservation to the real size as soon as the loader returns, so the default only limits how many loads can be running, not how many small results can wait for their fill |
| `Limits` | `ReaderSlotsPerShard` | 1024 | read-protection slots per shard, one 64-byte cache line each; a borrowed view holds one from `BorrowOn` to `Release`, and readers beyond the slots get `ErrBusy` (`Stats.PinSlotsExhausted`) |
| `Limits` | `MaxExecutorTasks` | derived from fixed capacity | admitted unfinished callbacks |
| `Runtime` | `RoundWorkLimit` / `RoundSoftTimeBudget` / `SweepPeriod` | 512 / 1ms / 5min | events per round, soft time budget, target period for the slow sweep to cover the whole index |
| `Runtime` | `PeriodicInterval` / `PeriodicWorkLimit` / `DisableTicker` | 25ms / 512 / false | tick period, shard/flight/control visits per tick, host-driven maintenance |
| `Runtime` | `Executor` / `Clock` | goroutine / monotonic clock | see below |
| Mode | `Mode` | `ModeCache` | `ModeCache` / `ModeShadow` / `ModeObserve`, see below |
| `Hot` | `LoadSlots` / `RequestSlots` / `RequestSampleP` | 0 / 0 / 1/1024 | origin-load and request hot-key tracking slots (per shard; 0 disables, in every mode), request sampling rate |
| `Hot` | `Windows` / `Interval` / `TopN` / `OnWindow` / `Label` | 0 / `Heat.Delta` / 16 / nil / nil | window history, window width, entries per ranking, notification callback, export mapping (see the observability doc) |

### Modes

- `ModeCache`: a normal cache.
- `ModeShadow`: a shadow cache. Admission, valuation, eviction, TTL and invalidation are simulated with the real configuration, but resident entries keep only the header and key, and a hit looks like a miss to the caller (`Fetch` still goes to the origin). `Stats.Hits/Misses` are virtual hits/misses; hit rate = `Hits/(Hits+Misses)`. Capacity and TTL are still required.
- `ModeObserve`: statistics only, nothing is cached. The heat gate and singleflight work as usual and fill candidates are rejected as `OBSERVE_ONLY`. The entry region and index shrink to a minimal block; capacity and TTL may be omitted (the internal defaults are `Shards`, `MaxEntryBytes` and 1 hour); explicit negative values still return `ErrInvalidConfig`. Hot-key tracking is off here too until you set `Hot.LoadSlots` / `Hot.RequestSlots`.

### Host executor contract (important)

```go
type Executor interface { TrySubmit(task func()) bool }
```

- **Accepted**: the task runs at most once; **rejected**: it does not run; it **must not run caller-runs or inline recursively** (that breaks the bounded-work and lock-order assumptions);
- The default executor starts one goroutine per task; when plugging in a business thread pool return `false` honestly when the pool is full;
- With `DisableTicker=true` the built-in periodic trigger is off and the host must call `c.MaintenanceStep()` regularly (no slower than `PeriodicInterval`, default 25ms). Idle slow-sweep retirement, `LoadTimeout` notifications, retire-chain reclamation (including `Close` reaching RECLAIMED) and window history all depend on it; `Future.Wait` has no timer of its own and only reacts to readiness and `ctx`.

### Billing

For `NewBytes` and `NewFlat` the billed size is the actual block size: `EntryHeaderBytes` + the key rounded up to 8 bytes + the value rounded up to 8 bytes. A custom weigher is neither needed nor accepted. A `NewObject` `weigh` is called once, outside the internal locks, after the load completes; every successful result is size-checked, including cold keys and `ModeObserve`.

`MemoryLimits` is a bound derived from the configuration, not the Go heap or process RSS: `RegionBytes` is the reserved upper bound of the mmap region and only pages that were written occupy physical memory. Ordinary values the caller holds after receiving them, host executor storage, loader scratch space and uncollected GC garbage are the host's responsibility. `NewObject` values and the object side table live on the Go heap and are counted in `ResidentBytes`.

## Pitfalls and troubleshooting

| Symptom | Cause | Action / how to check |
| --- | --- | --- |
| A load succeeded but nothing was cached; `FillRejected` is mostly `NOT_HOT` | the key's request heat did not reach `Tin`; a gate that is too small also drowns heat in collision noise (overestimate) or loses it to contention (underestimate) | look at `ns.Heat(key)`; lower `Tin` or raise `Heat.GateCells` |
| A full cache admits no new keys, `NON_POSITIVE_GAIN` | the candidate's benefit (heat × `Cost` × remaining TTL) does not exceed that of the entries to evict | normal eviction; if new keys are worth more, express it with `ResultMetadata.Cost` |
| `ALLOCATION_FAILED` keeps appearing | the region has no large enough contiguous free block: fragmentation, or retired blocks not yet reclaimed | raise `Memory.SlackPercent`; check for owned handles that are never released (`Stats.OwnedHandles`) |
| Expired entries are still counted in `ResidentEntries` | lazy retirement: they are removed when read, met by the eviction scan, or reached by the slow sweep | by design; they retire within one `SweepPeriod` and are never read |
| After invalidating one key, fills of other keys in the same shard show `STALE` | `fillFence` is per shard, so an invalidation voids every in-flight fill of the shard | a design trade-off; avoid high-rate single-key invalidation |
| With `DisableTicker`, waiters never time out, `Close` does not return, expired entries are not freed | these all progress through periodic calls | call `MaintenanceStep` no slower than `PeriodicInterval`; also pass a `ctx` with a timeout |
| Treating `ErrBusy` as a miss and going to the origin | `ErrBusy` only means this attempt could not decide | retry a bounded number of times on the same `Request`; make the retry budget longer than one GC-cycle pause (see the [validation document](VALIDATION.md#load)) |
| `ErrOverloaded` during a burst of cold keys although CPU and the origin are idle | at most `MaxConcurrentLoads` loads run at once and each holds `MaxResultBytes` of the response budget until its loader returns; a loader that is slow or waits for a free execution slot keeps its reservation | raise `MaxConcurrentLoads` (and `ResponseBytes` with it), or make the loader faster; `Stats.Overloaded` counts it |
| A waiter slot stays occupied after `FetchAsync` | an unfinished `Future` | always finish with `Wait` / `Take` (ok=true) / `Cancel` |
| A string kept from a borrowed view reads different content later | a `NewBytes` view points at a block that gets reused | use `Get` (a copy), or `Clone` into an owned handle first |
| `NewFlat` returns `ErrInvalidConfig` | the type holds pointers, or is a `string`, slice, map or interface | use `NewBytes` for `string`/`[]byte` and `NewObject` for types with pointers |
| A small cache (a few dozen entries per shard) has a low hit rate | shard quotas are fixed fair shares, with no transfer between shards | lower `Shards` |
| `MemoryLimits.TotalBytes` is far above the resident quota | `ResponseBytes` defaults to `MaxConcurrentLoads × MaxResultBytes` = 1GiB | lower `MaxConcurrentLoads` or set `ResponseBytes` explicitly |

## Known limits

These trade-offs are marked with `shortcut:` comments in the code and collected here:

| Limit | Effect | When to change it |
| --- | --- | --- |
| The index probe limit is fixed at 8 groups (56 slots), and entries are not moved back when others retire | After a full cache has churned for a long time about 19% of keys sit in a successor group, a miss visits about 4 groups on average at the default 0.85 load, and a fill beyond the limit is rejected (about 0.4% at 85% load, far rarer at 70%) | Lower `Memory.LoadFactor`, raise `maxGroups`, or move entries back on delete (readers would need a version retry) |
| The heat gate's default size is approximated from capacity, not from the distinct keys per window | When distinct missed keys far exceed the default cells, heat is overestimated by collisions and admission loosens | Set `Heat.GateCells` explicitly at scale |
| The tracking pre-filter sketch is fixed at 4x the slot count | With far more long-tail keys than that, estimates run high, which only makes admission more conservative and never drops an already tracked hot key | Add tracking slots for higher recall |
| The request-table identity compares only a 64-bit hash (including the namespace), not the full key | A hash collision (probability about slots × keys / 2^64) merges two keys' counts; the origin-load table compares full keys | Nothing, unless used as an exact billing ledger (it should not be) |
| When the slow sweep hits its soft time budget it drops the owed scan work instead of accumulating it | Under heavy load the period to cover the whole index is longer than `SweepPeriod`; expired entries are still invisible to reads and only free space later | Switch to accumulated debt if a strict coverage guarantee is needed |
| The namespace TTL cap is fixed at 1 year | A longer lifetime is rejected | Make it a `Config` field when needed |
| In shadow mode a follower reuses its leader's "virtual hit" verdict | An entry that expires while in flight is filled one time fewer; the next request refills it; harmless | None |
| The namespace generation in the entry header keeps only its low 32 bits | A dead entry is mistaken for a live one only if its namespace is invalidated an exact multiple of 2^32 times before the entry is read or retired by the slow sweep, which cannot happen in practice | Go back to 64 bits (4 more bytes per entry) when a stronger guarantee is needed |
| The retire epoch in the entry header keeps only its low 32 bits and is compared by signed difference | An entry is held back an extra round only if it stays on the retire chain for more than 2^31 epochs; reclamation is only delayed, never early | Same: back to 64 bits (4 more bytes per entry) |
| The billed byte count and the pin count in the entry header are 32 bits | `MaxResultBytes` may not exceed 4GiB-1 and `MaxOwnedPinsPerEntry` may not exceed 2^30, both checked at construction | Go back to 64 bits when one entry needs more |
| Per-shard entry and byte quotas are fixed fair shares with no transfer between shards | Binomial spread of the hash distribution makes a shard that receives surplus keys evict on its own, more visibly in small caches | Leave quota headroom, or lower `Shards` |
| The hot-key tables keep keys in a key pool (a TLSF region) sized at slots × `MaxKeyBytes` plus 25%; it is reserved virtual address space on mmap backends, so only the written pages cost memory, but on the heap backend (`Memory.Heap`, `-race`, non linux 64-bit) it is a real heap allocation (65,536 slots × 512 B = 52 MiB, × 4096 B = 332 MiB). On close the live keys are compacted into a heap block that is just large enough, so rankings stay readable | A pool that fragments beyond that reserve drops the event that needed a block (counted in `HotEventsDropped` / `HotRequestSamplesDropped`); the tracked key stays intact. Not observed in tests | Raise the tracking slots, or lower `MaxKeyBytes` |
| The SIEVE scan limit per fill is the constant 64 (`sieveScanLimit`) | It only bounds the work of one fill and the hit rate is insensitive to it | Promote it to an `Evict` field if a workload shows otherwise |
| `ArenaLargestFree` is the head block of the highest non-empty size class | Within one class width (about 1/16) of the true largest free block | Walk that class's list if an exact value is needed |
| When the platform supports `mmap` but reserving the region fails (a virtual-memory limit, address space exhausted) | `New` returns an error wrapping `ErrInvalidConfig` and releases whatever it had already reserved; it does not fall back to a heap allocation of the same size, because Go cannot recover from running out of memory | Lower `MaxBytes` / `MaxEntries`, raise the limit (`ulimit -v`, the container memory limit), or set `Memory.Heap` to accept a heap-backed region |

## Eviction valuation

Admission and replacement use the same benefit formula: `G = λ × cost × min(Evict.Horizon, remaining TTL)`, where `λ = heat / Heat.Delta` (per second) and `cost` is `ResultMetadata.Cost` (default 1). A candidate uses its gate heat (admission already guarantees it is at least `Tin`); a resident entry uses hit heat plus gate heat (the same basis as `ns.Heat`); an expired or invalidated entry has zero loss.

Among the at most 8 victims found by the bounded SIEVE scan, every feasible subset is enumerated (entry count, bytes and index install position are checked together), the subset with the smallest loss sum is chosen, and the fill commits only when `G(candidate) − loss(old entry) − loss(victims)` is positive. "Exact" is relative to this local candidate set; a global optimum is not guaranteed.

There is no sampled valuation, prior (Prior) or cost model any more: at hundreds of millions of keys almost no key accumulates enough samples, heat is always available at the target scale, and once the cache is full a hot key reliably replaces a cold one.

`NewObject` values are Go shallow copies; slices, maps, pointers and interfaces may still share storage inside. Keep a value immutable after publishing it, and copy it yourself when the business needs to modify it.
