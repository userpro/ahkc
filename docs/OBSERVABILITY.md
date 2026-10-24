# Hot-key statistics and export

[中文](OBSERVABILITY.zh.md) | **English**

[Getting started](../README.md) · [Usage guide](USAGE.md) · [Architecture](../ARCHITECTURE.md#hot-key-windows) · [Runnable examples](../example/README.md)

## Choose the metric scope first

| Interface | What it counts | Configuration |
| --- | --- | --- |
| `ns.Heat(key)` | Current request heat: miss gate + resident entry heat | Always available; records no extra accesses |
| `c.HotKeys(n)` | Instant origin-load ranking: real load heat, cumulative loads and merges | `ObserveHotKeys > 0` |
| `snapshot.Top` | Origin loads and merges of a finished window | Origin-load table + history or notification |
| `snapshot.Requests` | Estimated request count of a finished window, including cache hits and Get-only traffic | Request table + history or notification |

A normal cache may enable either table. `ObserveOnly: true` forbids value residency while keeping the other request and load semantics; it enables 128 origin-load slots per shard by default, and a negative value disables them explicitly. To observe existing requests only, call `Get` once per request and supply no loader; the origin-load ranking is then empty. See [hot-observe](../example/hot-observe/main.go).

Observe-only mode may omit `MaxEntries`, `MaxResidentBytes`, and namespace TTL; it allocates no resident entry pool, and an empty index keeps only the probe domain, so increasing `MaxEntries` does not grow this storage. Sampling, loading, waiter, and maintenance resources are still reserved as configured, and `MemoryLimits` includes dynamic budget caps that are not actually occupied, so it is not the allocation size at initialization.

Statistics take one successful `BeginRequest` as the request sampling entry point, including requests that may later not be delivered because of contention or overload. Get→Fetch or retries must reuse the same `Request`, otherwise they are counted more than once.

## Configuration

```go
Advanced: ahkc.Advanced{
    ObserveHotKeys:        128,         // origin-load table, per shard
    ObserveRequestKeys:    64,          // request table, per shard
    ObserveRequestSampleP: 1.0 / 1024,  // request sampling rate q
    HotHistoryWindows:     60,
    HotHistoryInterval:    time.Minute, // independent of HeatDelta
    HotHistoryTopN:        16,          // cap for each ranking
    OnHotWindow: func(ahkc.HotSnapshot) { /* notify consumers without blocking */ },
}
```

| Field | Default and constraints |
| --- | --- |
| `ObserveHotKeys` | 0 for a normal cache; 128 in observe-only mode; negative disables |
| `ObserveRequestKeys` | 0; when greater than 0 either history or notification must be enabled |
| `ObserveRequestSampleP` | Defaults to 1/1024 when the request table is on; range [2^-64, 1] |
| `HotHistoryWindows` | 0, no history retained; notification can be configured on its own |
| `HotHistoryInterval` | Defaults to `HeatDelta`; when different, range [1ms, 24h], and enabling windows requires the incremental collection period validation to pass |
| `HotHistoryTopN` | 16, entries per ranking per window |
| `OnHotWindow` / `HotKeyLabel` | nil; the former receives notifications, the latter maps or drops exported keys |

History or notification requires at least one tracking table. An explicitly independent window has a minimum width of `(⌈total slots/4096⌉ + 1) × PeriodicInterval`. Slots are collected per shard as a whole, and 4096 is the per-cycle collection budget target; a shard larger than that may exceed it. Larger tables increase scanning, lock hold time, and fixed memory cost, so validate the production sampling rate against the real workload.

## How to read the numbers

- `Heat` and `LoadHeat` are both double-bucket weighted counts, `current + (1-φ) × previous`, with width `HeatDelta`. `LoadHeat` is not a direct per-second load rate; divide by the window seconds for a rate approximation.
- `Loads` / `Merged` are cumulative counts since the current slot was occupied and reset after the key is evicted; they are not persistent cumulative metrics. `HotKeys(n)` sorts by `LoadHeat`, `Merged`, `Loads` descending; `n <= 0` returns all tracked keys.
- `WindowLoads` / `WindowMerged` are the events recorded since the key entered a slot within that window. `WindowRequests` is the recorded request sample count multiplied by `1/q`.
- `WindowError` is the error inherited by Space-Saving slots; rankings are sorted by "observed value + error". Without drops or saturation it bounds the actually recorded events or samples; the request ranking still carries sampling error after scaling by `1/q`, so it is not a definite interval for the true request volume.

Gate collisions can overestimate heat, saturation and contention can underestimate it, and insufficient slots evict keys. q=1 suits small-traffic demonstrations, while the default sampling rate keeps overhead under control first. Low-frequency keys may have no samples; absence from a ranking does not mean absence of traffic.

## Windows and quality signals

The built-in clock aligns windows to the Unix offset at creation time and is not adjusted by NTP while running; a custom clock aligns from its own origin. The history window number is `(clock + offset) / HotHistoryInterval`.

Events are assigned to windows by record time. A late event is written back only when its own buffer has not yet been collected and is intact; otherwise it goes to the newest uncollected window. Published history is never rewritten by the maintainer; this is not an event-time replay interface. Only fully finished windows are published, and shutdown does not force export of the current unfinished window.

| Quality field | Meaning |
| --- | --- |
| `Skipped` | Number of windows that were not published between this snapshot and the previous one |
| `Partial` | Data of some shards was already overwritten by a newer window |
| `Totals.Evicted` | Events overwritten or evicted before collection; counted as samples for the request table |
| `Totals.LoadEventsDropped` / `RequestSamplesDropped` | Events dropped due to tracking lock contention, assigned to windows by collection time |
| `Totals.Requests` | Collected samples plus contention-dropped samples scaled by `1/q`; maintenance lag can still lose window data |

A `Seq` gap, `Skipped`, and `Partial` mean different things: a missed consumer read, an unpublished window, and incomplete data in a published window respectively.

## Export and ownership

| Interface | Usage |
| --- | --- |
| `HotKeyHistory(n)` | The most recent n snapshots; `n <= 0` reads all retained history; ascending by time |
| `HotWindowsSince(seq)` | All retained snapshots with `Seq > seq` plus the number of lost coverings `lost`; store the last Seq as the cursor |
| `OnHotWindow` | Serialized, monotonically increasing Seq, at most once, may skip windows; a notification is dropped when the previous callback has not finished or the executor rejects it |
| `HotKeyLabel` | Maps keys for all ranking outputs; `keep=false` drops the entry; a panic drops only that entry and increments a counter |

Returned slices and rankings in callbacks are owned by the consumer; modifications do not affect internal history or other consumers. Each exported batch uses one copy block, and label hooks are applied on the copy. Label mapping does not rewrite the original tracking tables or change totals.

Callbacks and mappings must be bounded and non-blocking; mappings may be called concurrently. Callback panics are isolated and counted in `HotExportPanics`; a busy callback counts `HotWindowBusy` and `HotWindowDropped`, and delivery failures count as Dropped. Do not wait synchronously for `Close` inside a callback, because the callback itself is work that shutdown must wait for.

The recommended pattern is "callback only signals + consumer pulls with a cursor + periodic fallback", see [hot-export](../example/hot-export/main.go). Filling gaps requires `HotHistoryWindows` and consumption before history is overwritten. The library ships no monitoring service; hashing keys does not remove metric cardinality, so dynamic hot keys are better suited to logs and fixed aggregate quantities to metrics.

## Overhead and troubleshooting

No slots are allocated while tracking tables are off. Request tracking writes allocate nothing on the heap and use a short critical section; contention failures increment an atomic drop counter. Origin-load tracking reuses the immutable key already owned by the flight. Memory allocated for window construction and export is bounded by slot count, ranking length, and history length, while output retained by the caller is managed by the caller. Related regressions and benchmarks are in the [validation document](VALIDATION.md).

History is nil: check whether history is retained and whether a window boundary was crossed. No notifications: check Busy / Dropped and the shutdown state. With `DisableTicker`, `MaintenanceStep` must keep being driven; windows that are too short or maintenance that is too slow produce missing windows. `Get` in observe-only mode can still return `ErrBusy` / `ErrClosed`, and those errors must not be ignored.
