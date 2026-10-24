# Runnable examples

[中文](README.zh.md) | **English**

Run `go run ./example/<dir>` from the repository root. The checks inside each program fail when the example does not show the expected behavior; the manual clock is used only for deterministic demonstrations, while real services usually use the default clock and ticker.

| Reading order | Program | Capabilities shown |
| --- | --- | --- |
| 1 | [quickstart](quickstart/main.go) | New, Namespace, Fetch, Get, synchronous single-key invalidation, Stats, Close |
| 2 | [observe-modes](observe-modes/main.go) | The same request set under cache-only / statistics-only / combined modes; request ranking, origin-load table, history, and notification |
| 3 | [hot-observe](hot-observe/main.go) | Standalone request statistics without a loader, Heat, request window ranking; zero loads and zero residency |
| 4 | [read-semantics](read-semantics/main.go) | Get, BorrowOn, GetBorrowed, GetOwned, Clone, and Release; shallow copies and holding lifetimes |
| 5 | [async-load](async-load/main.go) | FetchAsync, singleflight, Future.Take / Cancel / Wait; cancellation does not affect other waiters |
| 6 | [hot-export](hot-export/main.go) | HotKeyLabel, OnHotWindow signals, HotWindowsSince cursor with lost, JSON logging, fixed-dimension metrics |
| 7 | [hotline](hotline/main.go) | Real clock with concurrent traffic, GetOn→FetchOn, bounded BUSY retry, window hit ratio, HotKeys, periodic invalidation |
| 8 | [advanced](advanced/main.go) | WithWeigher, WithResultMetadata, dynamic namespaces, SetPrior / SetModel, cost observation, hard deadlines, asynchronous invalidation, Credits, MemoryLimits, manual maintenance, and three-stage shutdown |

Shorter runnable API examples live in [example_test.go](../example_test.go); run `go test -run '^Example' .`. Timing wheel, cooldown, replacement, and overload boundaries inside the cache are covered by the [test index](../docs/VALIDATION.md), so there is no need to duplicate business code just to demonstrate internal algorithms.

`hotline` runs for 12 seconds by default; use `-duration=1s -workers=4 -keys=32` for a quick run, and `-skew` must be greater than 1. `hot-export` finishes in about 3 seconds. The printed heat is a window-weighted count and is not a direct per-second rate; sampled rankings must not be used for exact billing.
