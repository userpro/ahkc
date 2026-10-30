# Validation and performance

[中文](VALIDATION.zh.md) | **English**

[Quick start](../README.md) · [Architecture](../ARCHITECTURE.md)

This page records only numbers and commands measured on this machine. Results are valid only for the recorded hardware, parameters and version; re-measure on your target hardware before relying on them.

## Status

Environment: Go 1.25.4, linux amd64, 16 cores, 31 GiB RAM. `go build`, `go vet ./...`, `gofmt -l` (no output), `go test -count=1 ./...` (about 5 s), `go test -race -count=1 .` (about 55 s), `GOARCH=386 go vet .` and `GOARCH=386 go test .` (about 8 s) all pass. The 386 test run is a 32-bit binary on a 64-bit machine, not 32-bit hardware validation.

Not yet shown: a real 72-hour steady state, cross-NUMA scaling, a measured hundred-million-entry run (the table below is ten million, and hundreds of millions is an extrapolation), real-backend net CPU benefit, and an end-to-end business replay. `TestRepeatedFillAndExpiryLeaksNothing` only simulates 72 hours.

## Scale: residency and GC

`scale_test.go` is skipped by default and enabled by environment variables. It fills N variable-size values and measures:

```bash
AHKC_SCALE_ENTRIES=10000000 AHKC_SCALE_VMIN=16 AHKC_SCALE_VMAX=240 AHKC_SCALE_OBJECT=0 \
  go test -run TestScaleResidentGC -v -timeout 30m .
```

16 shards, `NewBytes[string]`, value length uniform in [16, 240] bytes, keys `key-<n>`, admission `Tin=1`:

| Entries | Resident | Resident bytes | Reserved region | Process RSS | Go live heap | GC scan heap | Create | Fill | Random hit |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2M | 99% | 443 MiB | 659 MiB | 488 MiB | 3 MiB | 2 MiB | 1 ms | 4.8 s | 629 ns |
| 10M | 99% | 2222 MiB | 3299 MiB | 2379 MiB | 3 MiB | 2 MiB | 1 ms | 24.4 s | 643 ns |

- **GC scan work does not depend on the entry count**: with 5x the entries, the Go live heap and GC scan heap are unchanged (3 MiB / 2 MiB).
- **Residency matches the budget**: resident bytes (including the 88-byte header and alignment) are 2222 MiB and process RSS is 2379 MiB (including the runtime, index and gate); of the 3299 MiB reserved only pages that were written take physical memory.
- **Creation is nearly instant**: 1-2 ms, because the region and index are `mmap` virtual address space committed on demand.
- "Random hit" includes the test loop itself and is for comparison only, not the cost of one read (see "Read path and allocations" below).
- Resident is 99%, not 100%: in this test `MaxEntries` equals the key count exactly, while each shard's entry quota is fixed at `MaxEntries / Shards` (no transfer between shards). Hash distribution has binomial spread, and a shard that receives surplus keys must evict on its own. In a 300k-key, 16-shard reproduction the binomial estimate gives a per-shard standard deviation of about 133 and a total surplus of about 850 (an estimate, not a measurement); the run published 299,967 and kept 299,202 resident (a gap of 798), with only 32 `FillRejected` (`INFEASIBLE` 27, `NOT_HOT` 4, `NON_POSITIVE_GAIN` 1). Leave headroom in the entry quota instead of setting it to exactly the expected key count; the relative spread grows with more shards and fewer entries per shard.

Bytes versus object layout on the same data (2M entries, value length [64, 2047] bytes, one run, other parameters as above):

```bash
AHKC_SCALE_ENTRIES=2000000 go test -run TestScaleResidentGC -v .
```

| Layout | Resident | Process RSS | Go live heap | GC scan heap | Fill | Random hit |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `NewBytes` (values in the region) | 99% | 2247 MiB | 3 MiB | 2 MiB | 7.1 s | 700 ns |
| `NewObject` (values on the Go heap) | 99% | 3302 MiB | 2161 MiB | 37 MiB | 4.0 s | 683 ns |

For the same data the object layout's Go live heap is about 720x that of the bytes layout, its GC scan heap about 18x, and RSS about 50% higher (the Go heap needs GC headroom); hit latency is the same. The object layout's live heap grows linearly with the data, which is the design cost of `NewObject`; the bytes and flat layouts are unaffected.

Hundreds of millions of entries is an extrapolation, not a measurement: the bytes layout's Go heap should still be a few MiB (only fixed structures that do not depend on the entry count), and physical memory is about "header + key + value per entry + about 15% margin", as reported by `MemoryLimits.RegionBytes`. Creation time depends on `Prefault` (without it the region is a millisecond-scale `mmap` reservation). Ten million entries on 16 shards was measured; re-run `scale_test.go` on your target machine for larger scales.

## Lifecycle acceptance at one million entries

`TestScaleLifecycle` (skipped by default) runs the whole life of one instance with the real clock and the built-in ticker (TTL and sweep period shortened to fit the run): fill, miss probing, hits, replacement on a full cache, heat decay, expiry, retire and reclaim.

```bash
AHKC_SCALE_LIFECYCLE=1000000 go test -run TestScaleLifecycle -v -timeout 10m .
```

16 shards, `NewBytes[string]`, values uniform in [16, 240] bytes, 1,000,000 entries, byte quota 288 per entry:

| Check | Measured |
| --- | --- |
| Create | 1 ms |
| Fill | 1.8 s, 553k fills/s, 99.1% resident (the rest are shard-quota surplus) |
| Memory | billed 235 B/entry, process RSS +265 MiB (281 B/entry, 20% above billed: index, gate, runtime); Go live heap 4 MiB, GC scan heap 3 MiB, independent of the entry count |
| Miss probing on the full index | 2.2 groups per lookup (limit 8) |
| Borrowed hit, one goroutine | p50 581 ns, p99 1.0 us, p99.9 8.1 us (random keys over a 258 MiB region, timer overhead removed), 0 allocations in 400k ops |
| Borrowed hit, 16 goroutines | 18 M ops/s |
| Full cache, 250k new cold keys | 17,346 replacements published; 19,808 of 19,808 protected hot entries kept |
| After heat decay, 50k new hot keys | 97.1% resident (the rest are shard-quota surplus); resident total never above `MaxEntries` |
| Expiry | nothing is removed before its TTL; no expired value is delivered; the last entry left 10.0 s after the last deadline with a 10 s sweep period; retired blocks fully reclaimed |
| Retire and reclaim allocations | 0 over the whole expiry and reclaim of 1,000,000 entries |
| Free space after full reclaim | one 19 MiB block per shard, 315 MiB free in total |

The single-goroutine latency is dominated by cache misses on a region far larger than the CPU cache (the 4096-key benchmark in "Read path" stays at 94 ns); the tail is OS and runtime noise.

## Hot-key tracking

`TestScaleHotTracker` (skipped by default): 16 shards with 65,536 request-tracking slots in total, a Zipf(1.1) stream over 100 million keys, 20 million sampled events recorded by one goroutine:

```bash
AHKC_SCALE_HOT=20000000 go test -run TestScaleHotTracker -v .
```

| Metric | Measured |
| --- | ---: |
| Record cost | 64.7 ns/event, 0 allocations |
| Lock-contention drops | 0 (single goroutine; not the drop rate under concurrency) |
| top-10k recall (against exact counts, which cover only the top 2^24 ranks) | 99.7% |
| Window collection (scan all 65,536 slots) | 5.7 ms |
| Sort and write columnar records (10k entries) | 2.1 ms |
| History: 1440 windows × 10k entries | 851 MiB (including key bytes), 0 MiB GC scan heap |
| Tracker arrays | 12 MiB |

Record cost versus slots (`BenchmarkTrackerRecordBySlots`, 80% cold long-tail keys):

| Slots per shard | 256 | 4096 | 65536 |
| --- | ---: | ---: | ---: |
| ns per record | 71.7 | 80.2 | 45.8 |

Cost does not grow linearly with slots (the previous implementation took 168, 543, 2181 and 7349 ns at 256, 1024, 4096 and 16384 slots). 65536 slots is faster because the filter admits fewer keys and more events hit already tracked hot keys. Recall and cost come from a single-goroutine replay; the drop rate under multi-goroutine contention is reported by `Stats.HotRequestSamplesDropped`, and `cacheload -hot-slots` lets you observe it under load.

Correctness: `tracker_test.go` checks the internal invariants of the lookup table and heap, the `[rank − err, rank]` bound, that the top 100 hot keys are not pushed out under a long-tail flood, window rotation and backward-shift deletion.

## Read path and allocations

Single-goroutine medians, range of three runs (`go test -bench`):

| Path | ns/op | Allocations |
| --- | ---: | ---: |
| `Get` hit (`NewBytes[string]`, one copy) | 110-111 | 1 |
| Borrowed hit (`BeginRequestOn` + `BorrowOn`) | 103-105 | 0 |
| Parallel `Get` hit (reciprocal throughput) | 21-22 | — |
| Parallel miss (reciprocal throughput) | 19-20 | — |
| Gate read and write (heat record of one miss) | 73 | 0 |
| Heat update / saturated skip | 6.4 / 4.1 | 0 |
| epoch pin + unpin | 11.3 | 0 |

Zero allocations on a borrowed hit is asserted by `TestGetHitAllocContract`; zero allocations for a `NewFlat` `Get` by `TestFlatGetHitZeroAlloc`; a borrowed hit stays allocation-free with request hot-key tracking on (`TestObserveRequestZeroAlloc`). Borrowed and `Get` hit costs both include one monotonic clock read (required for the TTL check).

## Reproducing everything

```bash
./bench/reproduce.sh              # about 5-8 minutes; artifacts go to bench/results/ (git-ignored)
DURATION=2s BENCHTIME=100ms ./bench/reproduce.sh  # smoke run; numbers are not comparable with this page
```

The script produces: an environment fingerprint, 7 checks (build, vet, gofmt, test, race, 386 build, 386 vet), micro-benchmarks, allocation and initialization benchmarks, the observe-switch and `HotKeys` benchmarks, 5 load scenarios (pure borrowed, mixed, mixed with a second seed, 99.9% concentrated, owned), the policy replay and search-oracle reports, and a summary. The smoke mode passed end to end on this machine (all 5 scenarios met the objective: about 1.1M QPS, P99 1.26-2.45 ms, 0 errors). The ten-million-entry scale test and the hot-key scale test are not in the script; run them separately with the environment variables above.

## Load

`cacheload` offers 1.1M QPS in millisecond batches. Latency runs from the scheduled arrival to value access and guard release, including backlog, bounded retries and GC. Objective: successful QPS >= 1M, P99 <= 3 ms, zero final errors. Mixed traffic is 95% warm hits / 3% cold lookups / 2% uncacheable, loader wall time 500 µs, 64 shards, 256 clients:

```bash
go run ./cmd/cacheload -duration 20s
# scale flags: -entries -value-min -value-max -hot-slots -prefault -hugepages
```

Same machine, same tool, the pre-change commit and the current version run alternately:

| Version | Runs | P99 range | Failed runs (any final error or P99 > 3 ms) |
| --- | ---: | --- | ---: |
| Before | 53 | 2.37-2.40 ms | 0 |
| Current | 65 | 2.37-2.42 ms (one at 5.7 ms) | 4 |

Both average 1.1M QPS. **The current version has shown intermittent final errors that the pre-change version rarely showed, and the root cause is not located.** What is established:

- Every error is `ErrBusy`: the load test retries one request a bounded number of times, until 512 attempts or 10 ms after arrival, without a decision. Failed runs usually had 1 to 5 errors, a few had dozens.
- **It is not reader slots or the epoch**: the new `Stats.PinSlotsExhausted` and `PinEpochRetriesExhausted` were 0 in the failing runs.
- I temporarily counted every `ErrBusy` return point in `getOrLoad` (since reverted). In every run `ErrBusy` mainly came from "a flight for the same key exists but is not complete yet", about 4k to 10k per 8 seconds, plus about 200 stripe-lock collisions; in two failing runs the former rose to 25k and 52k. Final errors come in two shapes: the first call itself waited 11 to 13 ms before returning, or 512 immediate `ErrBusy` in a row within 0.7 to 5 ms. Both look like flight completion held up by a scheduling stall, not a logic deadlock.
- **GC is involved, but does not explain it fully** (25 runs per row, 6 seconds each):

  | Version | GOGC | Failed runs / runs |
  | --- | --- | --- |
  | Current | 100 | 11 / 150 (in order 7, 3, 1, 0, 0, 0) |
  | Current | 1000 | 2 / 50 |
  | Current | off | 0 / 50 |
  | Current | 30 | 0 / 50 |
  | Before | 100 | 1 / 100 |
  | Before | 30 | 0 / 100 |

  The current Go live heap is about 32 MiB (67-92 MiB before), so at the same allocation rate GOGC=100 collects about 3x as often; but the pre-change version also had no failures at GOGC=30 (even more frequent GC), so "more frequent GC" alone is not a sufficient cause.
- **Failures come in clusters**: successive groups of 25 runs gave 7, 3, 1, 0, and the most recent 200 runs (both versions, two GOGC settings, 50 each) were all 0. Load from other processes on the same machine was not ruled out.

Conclusion: there is no evidence that the library fails in steady state, and no evidence that rules out a small regression; the load tool's 10 ms retry budget is sensitive to scheduling stalls. Re-test under your own load, retry `ErrBusy` a bounded number of times, and set `GOMEMLIMIT` to reduce GC frequency (see the [README](../README.md)). When it reproduces, read the `Pin*` counters in `Stats` to tell whether reader slots are the cause.

## Shutdown

During close the slow sweep retires every resident entry in one pass and the retire chain advances continuously while it makes progress (it no longer waits for the periodic throttle). 300k entries, 16 shards, `NewBytes`: `Close` dropped from 9.1 s to 0.32 s; the 2M-entry scale test dropped from 66 s to 8.9 s in total, and the 10M-entry one takes 36.6 s (324 s before the fix). `TestReclaimContinuesWhileMakingProgress` and `TestReclaimBlockedByHandleDoesNotSpin` assert "continue while making progress" and "do not spin when blocked by a handle".

## Index

The group table (7 slots per cache line, overflow count in the group header, no tombstones), after 300k delete-then-insert cycles at a pinned 70% load: each group's overflow count matches the resident entries group by group; a miss visits about 3.0 groups on average (steady state 2.5-3.4, independent of how many times the table has turned over), about 1.3 groups on a freshly filled table; fewer than 1% of inserts are rejected by the probe limit. At 85% load the same churn gives about a 0.4% rejection rate (simulated) and about 4 groups per miss. The default load factor is 0.85; drop `Memory.LoadFactor` to 0.7 when rejected fills or miss latency matter more than index memory.

## Checks

| Check | Command |
| --- | --- |
| Build / static analysis / format | `go build ./...`, `go vet ./...`, `gofmt -l .` |
| Tests | `go test -count=1 ./...` |
| Race and pointer checks | `go test -race -count=1 .` (a `-race` build keeps the region on the Go heap so checkptr coverage is retained) |
| 32-bit | `GOARCH=386 go vet .`, `GOARCH=386 go test .` |

## Invariant coverage

| Invariant | Test |
| --- | --- |
| Capacity and quota conservation | `TestReplacementWhenFull`, `TestConcurrentMixedStress`, `TestCloseBlockedByOrphanIO` |
| No hit after TTL or invalidation, no resurrection | `TestTTLExpiry`, `TestInvalidateKeyKillsInFlightFill`, `TestNamespaceInvalidateMidFillStale` |
| Solver equals exhaustive search | `solver_test.go` (spec cases + 4000 random) |
| Single terminal state / unique leader / waiter settles once | the full QUEUED→COMMITTING→terminal paths, `TestSingleflightMerge`, `TestWaiterSettleOnce` |
| Safe reclamation | `epoch_test.go`, owned-handle tests, `TestReclaimSkipsPinnedPrefix` |
| Allocator correctness | `arena_test.go` (randomized model check: blocks do not overlap, accounting is conserved, full coalescing after freeing everything, rejection when full) |
| Index | `index_test.go` (overflow counts match group by group, churn steady state, concurrent readers with one writer) |
| Lazy retirement | `lifecycle_test.go` (hint re-validation does not remove live entries, slow-sweep coverage period, soft budget, namespace generation), `TestExpiredEntryRetiredByAccessHint` |
| Replaceable when full | `evict_test.go` |
| Flight timeout cleanup, COMPLETED_PENDING reuse | `TestFlightTimeoutThenLateSourceCleanup`, `TestCompletedPendingReuse` |
| Overload and a full queue still deliver | `TestOverload*`, `TestFillQueueFullStillDelivers` |
| Window history: alignment, skipped windows, incremental collection, columnar round trip | `TestHotKeyHistoryRolls`, `TestHotWindow*`, `TestHistoryColumnarRoundTripOrder` |
| Resident structures are pointer-free | `TestEntryHeaderLayout`, `TestHistoryRecordIsPointerFree` |
| Close, completion pump, size bounds | [review_test.go](../review_test.go), `TestPumpGateFailureDoesNotLoseSignal` |
| Constructor contract (`NewFlat` rejects pointer types, `NewObject` needs `weigh`) | `TestConstructorsValidateValueType` |

## Constructed replay

`replay_test.go` generates six fixed-seed traffic classes (seed 20261001) — Zipf, scan, hot-shift, uniform mixed size/cost, short TTL, namespace invalidation — with 20,000 requests each and a capacity of 24 entries, and compares LRU, SIEVE, S3-FIFO, W-TinyLFU-AV and AHKC with the same resident capacity, TTL and heat eligibility. The reference policies are sequential models, not native baselines under the same total memory. `TestSyntheticPolicyReplay` asserts nothing about which policy wins. By default it replays a short trace through AHKC only and checks the library invariants (resident count and bytes within quota, no I/O left in flight, every load settled); set `AHKC_REPLAY_REPORT=path` to also run the four reference policies at full size and write the JSON.

Hit rate / share of total synthetic origin cost avoided (one run on this machine):

| Scenario | LRU | SIEVE | S3-FIFO | W-TinyLFU-AV | AHKC |
| --- | ---: | ---: | ---: | ---: | ---: |
| zipf | 54.9 / 36.9 | 57.9 / 40.6 | 59.2 / 41.9 | 63.3 / 42.6 | 62.2 / **44.9** |
| scan | 42.5 / 25.3 | 45.9 / 29.4 | 47.2 / 30.5 | 49.6 / 30.9 | 48.8 / **32.7** |
| hot-shift | 56.7 / 39.4 | 61.5 / 43.5 | 61.2 / 44.2 | 71.1 / 47.5 | 68.8 / **47.9** |
| uniform-size-cost | 4.8 / 4.3 | 5.6 / 4.2 | 5.5 / 4.1 | 8.6 / 4.2 | 7.1 / **4.7** |
| short-ttl | 53.3 / 36.1 | 53.8 / 36.4 | 53.7 / 36.2 | 56.2 / **37.1** | 54.8 / 36.8 |
| namespace-invalidation | 56.6 / 39.3 | 61.2 / 43.5 | 61.2 / 43.9 | 70.0 / 47.2 | 64.6 / **49.0** |

Reading: AHKC avoids the most cost in five of the six classes and is slightly behind W-TinyLFU-AV on short-ttl, but its hit rate is below W-TinyLFU-AV's in most of them. It evicts by "heat × cost × remaining lifetime", trading a lower hit rate for more avoided cost; the trade-off depends on the synthetic cost weights and does not imply a production net CPU benefit. The report depends on concurrent ordering, so the ratios shift slightly between runs.
