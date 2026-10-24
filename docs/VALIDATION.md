# Validation and performance

[中文](VALIDATION.zh.md) | **English**

[Getting started](../README.md) · [Architecture](../ARCHITECTURE.md)

## Status

Go 1.25.4 / linux amd64: build, vet, gofmt (no output), `go test -count=1 ./...` (4s) and `go test -race -count=1 ./...` (36s) all pass; the root and `cmd/cacheload` packages cross-compile for 386. Every number below comes from one run of `./bench/reproduce.sh` on this host; the source fingerprint (`SourceSHA256`) is in `bench/results/environment.json`.

Regressions cover Request-initialisation failure clearing the old key, Future terminal states and concurrent consumption, nil/cancelled ctx, nil loader, configuration and cost-model errors, full-queue rejection, and settled handles no longer pinning a closed cache. API changes (`NewCostTableModel` returns an error, ObserveOnly may omit capacity and TTL, ManualExecutor is a reusable FIFO ring) are in the [usage guide](USAGE.md).

Not proven: a real 72-hour steady state, 32 physical cores / cross-NUMA scaling, a native baseline under the same total M, real backend net CPU benefit, and business end-to-end replay. `TestVirtual72HourLifecycle` only simulates 72h.

## Reproduce

```bash
./bench/reproduce.sh              # ~5–8 min; writes bench/results/ (git-ignored)
DURATION=2s ./bench/reproduce.sh  # smoke; not comparable with these tables
```

One script produces the environment fingerprint, seven checks, microbenchmarks, five load scenarios, one deliberate failure sample, three model reports and a summary. Raw reports are never committed — only the numbers on this page are.

## Allocations

1,000,000 reads / 100,000 synchronous misses / 10,000 other iterations per group, 5 repetitions, medians. "Before" is the previous snapshot.

| Path | Before B/op, allocs | Now B/op, allocs | Now ns/op |
| --- | --- | --- | ---: |
| Get / synchronous Fetch hit | 0, 0 | 0, 0 | 92–104 |
| Borrowed miss | 32, 1 | 0, 0 | — |
| Async Fetch + Wait | 352, 4 | 144, 2 | 448–463 |
| Owned read rejected / accepted | — | 0, 0 / 48, 1 | 121 / 175–207 |
| Synchronous miss, default executor | 889, 8 | 808, 6 | — |
| Async miss / cancel, manual executor | 899, 10 | 803, 6 | — |
| 8 merged waiters, manual executor | 2515, 27 | 2419, 23 | — |
| Key / namespace invalidation, manual executor | 275,5 / 296,5 | 259,3 / 280,3 | — |
| Cost-model fill decision, after warm-up | 432, 2 | 0, 0 | — |
| Stats with no rejections / idle maintenance | 48,1 / 8,1 | 0,0 / 0,0 | — |

Heat cells, gates, sketches, sample queues, epochs, the timing wheel, the retired pool and the solver stay at 0 allocs/op. Rejected owned reads allocate nothing; an accepted one pays one 48B handle, asynchronous delivery 144B in two allocations (envelope plus value slot). Initialization allocates 28.64MB normally and 10.44MB in observe-only mode (~64% less; construction-time, not RSS). The observation switch costs 16.4 / 16.2 / 34.0 ns/op at off / q=1/1024 / q=1, all allocation-free — the first two are indistinguishable, q=1 is not.

## Load test

`cacheload` offers 1.1M QPS in millisecond batches and measures from scheduled arrival through value access and guard release, including backlog, bounded retries and GC. Acceptance: ≥1M successful QPS, P99 ≤3ms, zero final errors. 20s per scenario, 22M offered requests each, mixed traffic 95% warm / 3% cold / 2% uncached with a 500µs loader:

| Scenario | Successful QPS | P50 | P99 | Final errors |
| --- | ---: | ---: | ---: | ---: |
| Pure borrowed hits | 1,100,009 | 0.551ms | 1.252ms | 0 |
| 95/3/2 mixed borrowed | 1,099,923 | 0.611ms | 2.381ms | 0 |
| Mixed borrowed, second seed | 1,099,952 | 0.620ms | 2.377ms | 0 |
| Mixed borrowed, 99.9% concentrated / 64 keys | 1,099,943 | 0.615ms | 2.214ms | 8 in 1 of 6 runs |
| 95/3/2 mixed owned | 1,099,949 | 0.637ms | 2.457ms | 0 |

BUSY/OVERLOADED are attempt counts already inside the measured latency.

The concentrated scenario is **not guaranteed** to end with zero errors: across six 20s runs, five had zero and one had 8 errors in 22M requests (~6×10⁻⁸). The cause is client-side, not the library — `readHot` retries `ErrBusy` only 64 times with `runtime.Gosched()` between attempts, and ~390k BUSY signals per run can exhaust that budget; raising it to 512 in a scratch copy produced zero errors under identical load. `skew-4096keys-load.json` (default 4096 keys) fails by design: the cooled long tail yields ~13,500 resident-key misses per run.

## Microbenchmarks

Single-thread medians: Get 92.4–94.4ns/op, borrowed 110.1–110.2ns/op, protected-index baseline 67.8–68.7ns/op — Get is 1.34–1.39× and borrowed ~1.6× the baseline. Parallel Get 15.7–16.0ns/op, parallel miss 19.1–20.2ns/op (inverse throughput, not latency). The public-API package matches: Get 96.1, borrowed 104.1, parallel Get 15.4, owned parallel 72.2, full miss→load 185µs/op. All hit paths allocate nothing; roughly a third of the time is the monotonic clock read for TTL. `HotKeys(20)` takes 81–85µs and one 1792B allocation — a full snapshot, not per-request cost.

Microbenchmarks isolate sampling with SampleP=1e-12, while load tests use the default 1/1024. These are averages, not P99, and only plain Get reaches 1.34×, so 1.5× across all read modes is not claimed.

## Checks

All seven checks pass and are recorded with commands and timings in `bench/results/checks.json`: build, vet, gofmt, full suite, race, and 386 cross-compile (`GOARCH=386 go build` / `go vet`) — 32-bit execution is not claimed.

## Model reports

### Synthetic replay

`replay_test.go` generates six fixed-seed traffic classes — Zipf, scan, hot-shift, uniform mixed size/cost, short TTL, namespace invalidation — 20,000 requests each, against LRU, SIEVE, S3-FIFO, W-TinyLFU-AV and native AHKC under identical resident capacity, TTL and heat eligibility. Reference policies are sequential models, not native baselines at the same total M.

Avoided synthetic origin-load cost / total cost:

| Scenario | AHKC | W-TinyLFU-AV |
| --- | ---: | ---: |
| zipf | 45.1% | 42.7% |
| scan | 32.5% | 30.2% |
| hot-shift | 49.3% | 46.7% |
| uniform-size-cost | 4.7% | 4.0% |
| short-ttl | 38.4% | 36.6% |
| namespace-invalidation | 48.7% | 46.5% |

AHKC trades a lower Zipf hit ratio (55.4% vs 63.9%) for lower synthetic cost; that trade-off depends on cost weights and implies no net production CPU claim. Ratios shift by ~0.1pp between runs because the report depends on concurrent ordering, while `TestSyntheticPolicyReplay` assertions always pass.

### Search regret and merge calibration

- **Full-residency enumeration** (deterministic): 128 decisions, 87 positive-benefit gaps, 26 feasibility gaps; local gain 4214 vs 5939, search regret 1725. One static decision, not dynamic OPT.
- **Merge calibration** (deterministic): nine λ × duration points with the real loader and a manual clock. At λ=1000 with 100ms, 100 requests merge into one source, calibrating r=10/s; validates the grid and lifetime, not a real backend.

## Invariant coverage

| Invariant | Test |
| --- | --- |
| I1 capacity / I2 budget conservation | `TestReplacementWhenFull`, `TestConcurrentMixedStress`, `TestCloseBlockedByOrphanIO` |
| I3/I4 no hit after TTL or invalidation, no resurrection | `TestTTLExpiry`, `TestInvalidateKeyKillsInFlightFill`, `TestNamespaceInvalidateMidFillStale` |
| I5 solver equals exhaustive search | `solver_test.go` (spec cases + 4000 randomized) |
| I6 one terminal state per candidate / I7 one leader / I8 waiters settle once | full-path QUEUED→COMMITTING→terminal, `TestSingleflightMerge`, `TestWaiterSettleOnce` |
| I9 safe reclamation / I11 one sample per request | `epoch_test.go`, owned-handle tests, `TestSampleOncePerRequest` |
| Flight timeout cleanup, COMPLETED_PENDING reuse | `TestFlightTimeoutThenLateSourceCleanup`, `TestCompletedPendingReuse` |
| Overload and delivery past a full queue | `TestOverload*`, `TestFillQueueFullStillDelivers` |
| Estimate states, gate ε/δ error budget | `estimate_test.go`, `TestGateErrorBudgetDerivation`, `TestConfigValidationRejects` |
| Window history: alignment, skipped windows, incremental collection | `TestHotKeyHistoryRolls`, `TestHotWindow*` |
| Observation: zero allocation, concurrency safety, exports | `TestObserve*`, `TestHotWindow*`, `TestOnHotWindow*` |
| Shutdown, completion pump, model snapshots, size bounds | [review_test.go](../review_test.go), `TestPumpGateFailureDoesNotLoseSignal` |

Results hold only for the recorded hardware, parameters and version; a real 72-hour steady state and production traffic must be validated in the target environment.
