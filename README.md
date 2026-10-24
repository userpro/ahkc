# AHKC: Caching and Hot-Key Statistics

[中文](README.zh.md) | **English**

AHKC is an in-process Go SDK with zero third-party dependencies. It decides which values to cache based on request heat and expected benefit, merges concurrent origin loads, and provides request hot keys, origin-load hot keys, and window history.

## Choosing a usage mode

| Requirement | Configuration | Behavior |
| --- | --- | --- |
| Cache only | Default configuration | Heat admission, benefit-based eviction, TTL, singleflight; no hot-key ranking is produced |
| Statistics only | `ObserveOnly: true`, enable the request ranking and history as needed | Values are not retained; `Get` can record request heat, and `Fetch` additionally provides origin-load and merge statistics |
| Cache + statistics | Enable `ObserveHotKeys` / `ObserveRequestKeys` on a normal cache | Keeps caching and also emits the origin-load ranking and a request ranking that includes cache hits |

`HotKeys` is the **origin-load ranking** and does not count cache hits. The request ranking that includes hits lives in `Requests` of a window snapshot. Observe-only mode may omit resident capacity and TTL; it allocates no resident entry pool, but still validates result size and keeps bounded sampling, loading, and maintenance resources.

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
    c, err := ahkc.New[string](ahkc.Config{
        MaxEntries:       1024,
        MaxResidentBytes: 64 << 20,
        Namespaces: map[string]ahkc.NamespaceConfig{
            "user": {TTL: time.Minute, Tin: 3, Prior: &ahkc.Prior{Lambda: 10}},
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

A successful `Fetch` means the result was delivered; filling happens in the background. A value may be retained only once `Tin` is reached, a valid estimate exists, and net benefit is positive; with `Tin=1` even the first request may be cached. The `Prior` in the example supplies a request-rate prior for cold start and should be set and updated according to production traffic.

To observe existing business requests only, no loader is needed:

```go
// Set ObserveOnly: true in the configuration above, then add:
Advanced: ahkc.Advanced{
    ObserveHotKeys:        -1, // disable the origin-load ranking that observe-only mode enables by default
    ObserveRequestKeys:    64,
    HotHistoryWindows:     60,
    HotHistoryInterval:    time.Minute,
}
// Call ns.Get(key) once per business request and ignore the returned value; errors must still be handled.
// ns.Heat(key) queries current heat; c.HotKeyHistory(1) returns the request ranking of the most recently completed window.
```

See [observe-only example](example/hot-observe/main.go) for the full program, and [observe-modes](example/observe-modes/main.go) for the same requests across all three modes.

## Key conventions when integrating

- `Get` returns hit / miss / `ErrBusy` / `ErrClosed`; `ErrBusy` means this attempt could not decide, so the caller should retry a bounded number of times or degrade.
- "Get then Fetch" and retries should reuse the same `Request` to avoid duplicate sampling and counting; `Request` and borrowed views must not be copied.
- The `Future` returned by `FetchAsync` must be finalized with `Wait`, `Take` (`ok=true`), or `Cancel`; taking the result again returns `ErrConsumed`. Borrowed views and owned handles must be `Release`d.
- `LoadTimeout` and caller cancellation do not stop the real I/O; the loader sets its own database or RPC deadline. `Close` waits for real I/O, handles, and budget release, so use a sensible timeout.
- Returned Go values are shallow copies; keep shared values immutable. Custom reference types must provide a `WithWeigher` that covers the full retained storage.
- Hot keys are bounded approximations, affected by sampling, lock contention, slot eviction, and maintenance lag; they are not suitable as an exact billing ledger.

## Documentation and examples

| Entry | Content |
| --- | --- |
| [Usage guide](docs/USAGE.md) | API, configuration, read ownership, invalidation, shutdown, cost model, and troubleshooting |
| [Hot-key statistics](docs/OBSERVABILITY.md) | Metric semantics, standalone use, windows, cursors, label mapping, and notification |
| [Example index](example/README.md) | Find runnable programs by capability |
| [Architecture](ARCHITECTURE.md) | Data flow, admission and replacement, concurrency and resource bounds |
| [Source walkthrough](docs/WALKTHROUGH.md) | Reading path through files and key functions |
| [Validation and performance](docs/VALIDATION.md) | Test entry points, performance reproduction, evidence scope |
| [Original technical design](docs/TECH.md) | Design derivation and pseudocode; current behavior is defined by the implementation and the usage guide |

```sh
go test ./...
go test -race ./...
go vet ./...
```

Scope: in-process, non-persistent, and no cross-instance consistency. `MemoryLimits` is a retained upper bound derived from the host billing contract; it is not the Go heap or process RSS.

## License

MIT. See [LICENSE](LICENSE).
