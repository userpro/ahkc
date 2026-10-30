# Runnable examples

[中文](README.zh.md) | **English**

Run `go run ./example/<directory>` from the repository root. The checks inside each program fail when it does not show the expected behavior; the manual clock is only for deterministic demos, and a real service normally uses the default clock and ticker.

| Reading order | Program | Capability shown |
| --- | --- | --- |
| 1 | [quickstart](quickstart/main.go) | NewBytes, Namespace, Fetch, Get, synchronous single-key invalidation, Stats, Close |
| 2 | [observe-modes](observe-modes/main.go) | The same requests in cache-only / statistics-only / combined mode; request ranking, origin-load table, history and notification |
| 3 | [hot-observe](hot-observe/main.go) | Standalone request statistics without a loader (`ModeObserve`), Heat, request window ranking; zero loads and zero residency |
| 4 | [read-semantics](read-semantics/main.go) | Get, BorrowOn, GetBorrowed, GetOwned, Clone and Release; a borrowed view is valid only until Release, and holding lifetimes |
| 5 | [async-load](async-load/main.go) | FetchAsync, singleflight, Future.Take / Cancel / Wait; cancelling does not affect other waiters |
| 6 | [hot-export](hot-export/main.go) | Hot.Label, Hot.OnWindow signal, HotWindowsSince cursor and lost, JSON logs, fixed-dimension metrics |
| 7 | [hotline](hotline/main.go) | Real clock and concurrent traffic, GetOn→FetchOn, bounded BUSY retry, window hit rate, HotKeys, periodic invalidation |
| 8 | [advanced](advanced/main.go) | NewObject with custom billing, WithResultMetadata (`NoCache`, `HardDeadline`, `Cost`), dynamic namespaces, hard deadlines, asynchronous invalidation, MemoryLimits, manual maintenance and three-phase shutdown |

Shorter runnable API examples are in [example_test.go](../example_test.go); run `go test -run '^Example' .`. Lazy retirement, replacement and overload boundaries are indexed in the [validation document](../docs/VALIDATION.md), so there is no need to copy internal algorithms into business code just to demonstrate them.

`hotline` runs for 12 seconds by default; try `-duration=1s -workers=4 -keys=32` for a quick look, and `-skew` must be greater than 1. `hot-export` finishes in about 3 seconds. The printed heat is a window-weighted count, not directly a per-second rate; sampled rankings must not be used for exact billing.
