# Source walkthrough

[中文](WALKTHROUGH.zh.md) | **English**

[Architecture](../ARCHITECTURE.md) explains the mechanisms; this page only provides reading entry points. The actual implementation and function comments are authoritative.

## Reading along one request

| Order | File / entry | Focus |
| --- | --- | --- |
| 1 | [config.go](../config.go): `applyDefaults` / `validate`; [memory.go](../memory.go): `planLayout` | Defaults, invalid input, layout and memory bounds before allocation |
| 2 | [cache.go](../cache.go): `NewBytes` / `NewFlat` / `NewObject` / `newCache` | The three constructors, one-time reservation of each shard's region and index, namespaces |
| 3 | [region.go](../region.go), [arena.go](../arena.go) | mmap and heap backends; the TLSF allocator (O(1) alloc and free, coalescing of adjacent free blocks, 32-bit block references) |
| 4 | [entry.go](../entry.go), [layout.go](../layout.go) | The pointer-free header overlaid on a block; per-layout write, view, copy and billing |
| 5 | [api.go](../api.go): `beginRequest` / `lookup` / `recordHeatOnce` | One logical request records heat once; pin, then re-check state, then read; stale entries register a hint |
| 6 | [index.go](../index.go), [heat.go](../heat.go) | The cache-line-per-group index (slot = tag + block reference, group header counts overflow, no tombstones); the single-cache-line heat gate |
| 7 | [flight.go](../flight.go): `getOrLoad` / `runLoader` / `tryPumpFlight` | Single leader, bounded waiters, result mailbox, and completion pump |
| 8 | [future.go](../future.go): `bindReady` / `Take` / `Cancel` | Racing state transitions, result references, and single budget settlement; a cache hit is copied out before the handle is released |
| 9 | [fill.go](../fill.go): `tryPrepareCandidate` / `planFill` / `freezeInput` / `prepareCommit` / `commitFill` | Delivery separated from filling; heat × cost valuation; physical allocation completes before the irreversible point |
| 10 | [sieve.go](../sieve.go), [solver.go](../solver.go) | Bounded candidates, locally exact subset search |
| 11 | [maintain.go](../maintain.go), [retire.go](../retire.go) | Single maintainer, category quotas; hint consumption and the slow sweep; intrusive retire chain and reclaim continuation |
| 12 | [epoch.go](../epoch.go) | Unlinking separated from reclamation; epoch / pin protected block reuse |

## Locating by capability

| Capability | Entry | Most direct test |
| --- | --- | --- |
| Region and allocator | `region.go`, `arena.go` | `arena_test.go` (randomized model check) |
| Index and gate | `index.go`, `heat.go` | `index_test.go` (including churn steady state and concurrent readers), `gate_test.go`, `heat_test.go` |
| Eviction valuation and replacement | `fill.go`, `solver.go` | `evict_test.go`, `solver_test.go`, `fill_test.go` |
| Lazy retirement | `consumeHints` / `sweepStep` in `maintain.go`; `retire.go` | `lifecycle_test.go`, `TestTTLExpiry` |
| Capacity and budget | `budget.go`, `executor.go` | `budget_test.go` |
| Single-key / namespace invalidation | `Invalidate*` in `api.go`; maintenance control queue | `invalidation_test.go` |
| Read ownership | `Borrowed` / `Handle` in `api.go` | `TestBorrowedAndClonedHandlesKeepRetiredEntry` |
| Cache / shadow / statistics-only | `Mode` branches in `fill.go` and `api.go`; `beginRequest` | `TestCacheAndObservationModes`, `shadow_test.go` |
| Hot-key tracking and history export | `tracker.go`, `observe.go`, `hotwindow.go` | `tracker_test.go`, `observe_test.go`, `hotwindow_test.go` |
| Shutdown | `checkReclaimed` in `maintain.go`; shutdown interfaces in `api.go` | `TestCloseStagesAreIndependentlyAwaitable` |
| Scale | `scale_test.go` (skipped by default, enabled by environment variables) | see the [validation document](VALIDATION.md) |

## Debugging routes

- Origin load succeeded but nothing was cached: check `Stats.FillRejected` first, then follow the reason into `tryPrepareCandidate`, `freezeInput`, `prepareCommit` or `commitFill`. For `NOT_HOT` look at the gate heat, for `NON_POSITIVE_GAIN` at the two valuations in `freezeInput`, and for `ALLOCATION_FAILED` at the owned-handle count in `Stats` and `Memory.SlackPercent`.
- Duplicate loads: check the full flight identity and the invalidation token; a new-version leader after invalidation is expected, so a string key alone cannot be used to judge.
- Shutdown does not complete: inspect real I/O, Futures, readers, and owned handles, then the retire chain; in manual maintenance mode the executor and `MaintenanceStep` must keep being driven.
- Request ranking misses a key: check whether it was sampled, whether slots suffice, whether the window already rolled, and the quality fields. `HotKeys` only looks at origin-load activity.
- A borrowed view reads different content: the view points at a block that gets reused and must not be used after `Release`; see the [usage guide](USAGE.md).

For new concurrency regressions, prefer reusing `newTestEnv`'s manual clock and executor to drive tasks at explicit state points. Supplement real concurrency tests with race checks; a fixed sleep cannot prove that a specific interleaving occurred.
