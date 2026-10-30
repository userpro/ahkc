# AHKC: Caching and Hot-Key Statistics

[中文](README.zh.md) | **English**

AHKC is an in-process Go SDK with zero third-party dependencies, built for caches of hundreds of millions of entries and tens of thousands of tracked hot keys. It admits keys by request heat, evicts by "heat × origin cost × remaining lifetime", merges concurrent origin loads, and provides request hot keys, origin-load hot keys, and window history.

Resident data lives in a memory region reserved once at creation and contains no Go pointers, so GC scan work does not depend on the entry count. The value type is fixed by the constructor, and there is no reflection at run time.

## Choosing a constructor and a mode

| Value type | Constructor | Storage | Read |
| --- | --- | --- | --- |
| `string`, `[]byte` (including named types) | `NewBytes[V]` | raw bytes in the region | borrowed view is zero-copy and zero-alloc; `Get` copies once |
| numbers, arrays, pointer-free structs | `NewFlat[V]` | memory image in the region | value copy, zero allocations |
| any object (pointers, maps, interfaces) | `NewObject[V](cfg, weigh)` | value stays on the Go heap (per-shard side table) and is scanned by the GC | value copy, zero allocations |

`NewFlat` uses reflection once, at construction, to check that the type holds no pointers; a type with pointers returns `ErrInvalidConfig`. Values with pointers cannot be copied safely into memory the GC does not scan, so use `NewObject` and supply `weigh`, which returns `EntryHeaderBytes` + key + all memory the value references.

| Requirement | Configuration | Behavior |
| --- | --- | --- |
| Cache only | default `Mode: ahkc.ModeCache` | Heat admission, benefit-based eviction, TTL, singleflight; no hot-key ranking is produced |
| Statistics only | `Mode: ahkc.ModeObserve`, enable the request ranking and history as needed | Values are not retained, the entry region and index shrink to a minimal block, and only the heat gate keeps its size; `Get` can record request heat, and `Fetch` additionally provides origin-load and merge statistics |
| Cache + statistics | Enable `Hot.LoadSlots` / `Hot.RequestSlots` on a normal cache | Keeps caching and also emits the origin-load ranking and a request ranking that includes cache hits |
| Hit-rate estimate | `Mode: ahkc.ModeShadow` | Simulates admission and eviction without serving values: `Stats.Hits` / `Misses` are virtual, and every request still goes to the origin |

`HotKeys` is the **origin-load ranking** and does not count cache hits. The request ranking that includes hits lives in `Requests` of a window snapshot.

## Quick start

Requires Go 1.23+. The module in this repository is currently named `ahkc`, so you can run it directly in this directory:

```sh
go run ./example/quickstart
go run ./example/observe-modes
```

Other local projects can add `require ahkc v0.0.0` and `replace ahkc => /path/to/ahkc` to their `go.mod`. The repository does not declare a remote module path that can be fetched with `go get`.

```go
package main

import (
    "context"
    "fmt"
    "time"

    "ahkc"
)

func main() {
    ctx := context.Background()
    c, err := ahkc.NewBytes[string](ahkc.Config{
        MaxEntries: 1_000_000,
        MaxBytes:   1 << 30,
        Namespaces: map[string]ahkc.NamespaceConfig{
            "user": {TTL: time.Minute, Tin: 3},
        },
    })
    if err != nil { panic(err) }
    defer c.Close(ctx)

    ns, err := c.Namespace("user")
    if err != nil { panic(err) }
    value, err := ns.Fetch(ctx, "42", func(ctx context.Context, key string) (string, error) {
        return "user:" + key, nil // replace with a real database or RPC call
    })
    if err != nil { panic(err) }
    fmt.Println(value)
}
```

A successful `Fetch` means the result was delivered; filling happens in the background. A value may be retained only once the key's request heat reaches `Tin` and the benefit is positive; with `Tin=1` even the first request may be cached.

When origin cost differs per key (large objects, a slow backend), return a `Cost` in the load result metadata and eviction weights by it; the default is 1:

```go
ahkc.WithResultMetadata[string](func(key, v string) ahkc.ResultMetadata {
    return ahkc.ResultMetadata{Cost: 5}
})
```

To observe existing business requests only, no loader is needed:

```go
c, _ := ahkc.NewBytes[string](ahkc.Config{
    Mode:       ahkc.ModeObserve,
    Namespaces: map[string]ahkc.NamespaceConfig{"user": {}},
    Hot: ahkc.HotConfig{RequestSlots: 64, Windows: 60, Interval: time.Minute},
})
// Call ns.Get(key) once per business request and ignore the returned value; errors must still be handled.
// ns.Heat(key) queries current heat; c.HotKeyHistory(1) returns the request ranking of the most recently completed window.
```

See [observe-only example](example/hot-observe/main.go) for the full program, and [observe-modes](example/observe-modes/main.go) for the same requests across all three modes.

## Key conventions when integrating

- `Get` returns hit / miss / `ErrBusy` / `ErrClosed`; `ErrBusy` means this attempt could not decide, so the caller should retry a bounded number of times or degrade.
- "Get then Fetch" and retries should reuse the same `Request` to avoid duplicate sampling and counting; `Request` and borrowed views must not be copied.
- **A borrowed view is valid only until `Release`.** With `NewBytes` the view points into the region, and the block is reused after it is freed, so do not keep it. To hold the value longer use `Get` (which copies), or `Clone` an owned handle first. Using a borrowed view after `Close` is an error (the region has been returned).
- The `Future` returned by `FetchAsync` must be finalized with `Wait`, `Take` (`ok=true`), or `Cancel`; taking the result again returns `ErrConsumed`. Borrowed views and owned handles must be `Release`d.
- `LoadTimeout` and caller cancellation do not stop the real I/O; the loader sets its own database or RPC deadline. `Close` waits for real I/O, handles, and budget release, so use a sensible timeout.
- `NewObject` values are shallow copies; keep shared values immutable, and make `weigh` cover the full retained storage.
- An expired value is never delivered, but expired entries are not released on time: they retire when read, when the eviction scan meets them, or at the latest within one slow-sweep period (default 5 minutes). Until then they still occupy quota.
- Physical pages are committed on first write. A retired block only returns to a free list for reuse, so process memory stays at its high-water mark until `Close` returns the whole region.
- Hot keys are bounded approximations, affected by sampling, lock contention, slot eviction, and maintenance lag; they are not suitable as an exact billing ledger.

## Configuring for 100M entries and tens of thousands of hot keys

```go
cfg := ahkc.Config{
    Shards:     128,       // a shard region is at most 32GiB; more shards also balance better
    MaxEntries: 100_000_000,
    MaxBytes:   200 << 30, // sized by bytes
    Heat: ahkc.HeatConfig{
        GateCells: 1 << 22, // about 4x the number of distinct missed keys per Heat.Delta
    },
    Hot: ahkc.HotConfig{RequestSlots: 512, RequestSampleP: 1.0 / 16, Windows: 60},
    Memory: ahkc.MemoryConfig{
        Prefault: true, // touch index and gate pages at startup to avoid page-fault jitter online
        HugePages: true, // recommended together with Prefault
    },
}
```

- `MemoryLimits.RegionBytes` is the resident-memory bound (entry region + index + gate, including the `Memory.SlackPercent` margin, default 15%). The mmap region does not count toward Go's GOGC trigger; a good setting is `GOMEMLIMIT = container limit − RegionBytes − other`.
- `MemoryLimits.TotalBytes` also includes `ResponseBytes` (default `MaxConcurrentLoads × MaxResultBytes`, the bound on loads that are running: 256 × 4MiB = 1GiB by default; a finished load only keeps its real size). With a small resident quota it looks large; lower `MaxConcurrentLoads` or `ResponseBytes` as needed.
- To track 10k to 65k hot keys keep a few hundred to a few thousand slots per shard and `Hot.RequestSampleP` at or below 1/16. For the k-th key to get at least n samples per window you need `q ≥ n / (f_k × window seconds)`.

## Documentation and examples

| Entry | Content |
| --- | --- |
| [Usage guide](docs/USAGE.md) | API, configuration, read ownership, invalidation, shutdown, and troubleshooting |
| [Hot-key statistics](docs/OBSERVABILITY.md) | Metric semantics, standalone use, windows, cursors, label mapping, and notification |
| [Example index](example/README.md) | Find runnable programs by capability |
| [Architecture](ARCHITECTURE.md) | Memory model, data flow, admission and replacement, lazy retirement and safe reclamation |
| [Source walkthrough](docs/WALKTHROUGH.md) | Reading path through files and key functions |
| [Validation and performance](docs/VALIDATION.md) | Test entry points, measured data, evidence scope |

```sh
go test ./...
go test -race ./...
go vet ./...
```

Scope: in-process, non-persistent, and no cross-instance consistency. `MemoryLimits` is a retained upper bound derived from the configuration; it is not process RSS.

## License

MIT. See [LICENSE](LICENSE).
