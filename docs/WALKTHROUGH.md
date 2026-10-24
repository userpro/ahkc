# Source walkthrough

[中文](WALKTHROUGH.zh.md) | **English**

[Architecture](../ARCHITECTURE.md) explains the mechanisms; this page only provides reading entry points. The actual implementation and function comments take precedence over pseudocode in the original technical design.

## Reading along one request

| Order | File / entry | Focus |
| --- | --- | --- |
| 1 | [config.go](../config.go): `applyDefaults` / `validate`; [memory.go](../memory.go): `planLayout` | Defaults, invalid input, layout validation before allocation |
| 2 | [cache.go](../cache.go): `New` / `registerNamespace` | Fixed structures, namespaces, sampling thresholds, host resources |
| 3 | [api.go](../api.go): `beginRequest` / `lookup` / `recordHeatOnce` | One logical request samples and records heat once; pin before reading |
| 4 | [index.go](../index.go), [heat.go](../heat.go) | Full identity, fixed probe domain, double-bucket atomic heat |
| 5 | [flight.go](../flight.go): `getOrLoad` / `runLoader` / `tryPumpFlight` | Single leader, bounded waiters, result mailbox, and completion pump |
| 6 | [future.go](../future.go): `bindReady` / `Take` / `Cancel` | Racing state transitions, result references, and single budget settlement |
| 7 | [fill.go](../fill.go): `tryPrepareCandidate` / `planFill` / `freezeInput` / `commitFill` | Delivery separated from filling, frozen model, conditional commit |
| 8 | [sieve.go](../sieve.go), [solver.go](../solver.go) | Bounded candidates, locally exact subset search |
| 9 | [maintain.go](../maintain.go), [wheel.go](../wheel.go) | Single maintainer, category quotas, periodic progress, hard TTL removal |
| 10 | [retire.go](../retire.go), [epoch.go](../epoch.go), [entry.go](../entry.go) | Unlinking separated from reclamation, epoch / pin protected pool reuse |

## Locating by capability

| Capability | Entry | Most direct test |
| --- | --- | --- |
| Capacity and budget | `budget.go`, `credit.go`, `executor.go` | `budget_test.go`, `TestCreditTransitAndDuplicateDelivery` |
| Benefit sampling and calibration | `stats.go`, `costmodel.go` | `estimate_test.go`, `calibration_test.go` |
| Single-key / namespace invalidation | `Invalidate*` in `api.go`; maintenance control queue | `invalidation_test.go` |
| Read ownership | `Borrowed` / `Handle` in `api.go` | `TestBorrowedAndClonedHandlesKeepRetiredEntry` |
| Cache-only / statistics-only / combined | `ObserveOnly` branches in `fill.go`; `beginRequest` | `TestCacheAndObservationModes` |
| Hot-key rankings and history export | `observe.go`, `hotwindow.go` | `observe_test.go`, `hotwindow_test.go` |
| Shutdown | `checkReclaimed` in `maintain.go`; shutdown interfaces in `api.go` | `TestCloseStagesAreIndependentlyAwaitable` |

## Debugging routes

- Origin load succeeded but nothing was cached: check `Stats.FillRejected` first, then follow the reason into `tryPrepareCandidate`, `freezeInput`, or `commitFill`. `NO_ESTIMATE` usually means a missing Prior, sample warm-up, or contamination.
- Duplicate loads: check the full flight identity and the invalidation token; a new-version leader after invalidation is expected, so a string key alone cannot be used to judge.
- Shutdown does not complete: inspect real I/O, Futures, readers, and owned handles, then the retirement ledger; in manual maintenance mode the executor and `MaintenanceStep` must keep being driven.
- Request ranking misses a key: check whether it was sampled, whether slots suffice, whether the window already rolled, and the quality fields. `HotKeys` only looks at origin-load activity.

For new concurrency regressions, prefer reusing `newTestEnv`'s manual clock and executor to drive tasks at explicit state points. Supplement real concurrency tests with race checks; a fixed sleep cannot prove that a specific interleaving occurred.
