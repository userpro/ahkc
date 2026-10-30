# Hot-key statistics and export

[中文](OBSERVABILITY.zh.md) | **English**

[Getting started](../README.md) · [Usage guide](USAGE.md) · [Architecture](../ARCHITECTURE.md#hot-key-windows) · [Runnable examples](../example/README.md)

## Choose the metric scope first

| Interface | What it counts | Configuration |
| --- | --- | --- |
| `ns.Heat(key)` | Current request heat: miss gate + resident entry heat | Always available; records no extra accesses |
| `c.HotKeys(n)` | Instant origin-load ranking: real load heat, cumulative loads and merges | `Hot.LoadSlots > 0` |
| `snapshot.Top` | Origin loads and merges of a finished window | Origin-load table + history or notification |
| `snapshot.Requests` | Estimated request count of a finished window, including cache hits and Get-only traffic | Request table + history or notification |

A normal cache may enable either table. `Mode: ahkc.ModeObserve` forbids value residency while keeping the other request and load semantics; the hot-key tables stay off until you set `Hot.LoadSlots` / `Hot.RequestSlots` (0 disables each, in every mode; a negative value is rejected). To observe existing requests only, call `Get` once per request and supply no loader; the origin-load ranking is then empty. See [hot-observe](../example/hot-observe/main.go).

`ModeObserve` may omit `MaxEntries`, `MaxBytes`, and namespace TTL; the entry region shrinks to 4 KiB and the index to one group, so increasing `MaxEntries` does not grow this storage, while the heat gate keeps its size. Loading, waiter, and maintenance resources are still reserved as configured, and `MemoryLimits` includes dynamic budget caps that are not actually occupied, so it is not the allocation size at initialization.

Statistics take one successful `BeginRequest` as the request sampling entry point, including requests that may later not be delivered because of contention or overload. Get→Fetch or retries must reuse the same `Request`, otherwise they are counted more than once.

## Configuration

```go
Hot: ahkc.HotConfig{
    LoadSlots:      128,         // origin-load table, per shard
    RequestSlots:   64,          // request table, per shard
    RequestSampleP: 1.0 / 1024,  // request sampling rate q
    Windows:        60,
    Interval:       time.Minute, // independent of Heat.Delta
    TopN:           16,          // cap for each ranking
    OnWindow: func(ahkc.HotSnapshot) { /* notify consumers without blocking */ },
},
```

| Field | Default and constraints |
| --- | --- |
| `Hot.LoadSlots` | 0 (disabled) in every mode; negative is rejected |
| `Hot.RequestSlots` | 0; when greater than 0 either history or notification must be enabled |
| `Hot.RequestSampleP` | Defaults to 1/1024 when the request table is on; range [2^-64, 1] |
| `Hot.Windows` | 0, no history retained; notification can be configured on its own |
| `Hot.Interval` | Defaults to `Heat.Delta`; when different, range [1ms, 24h], and enabling windows requires the incremental collection period validation to pass |
| `Hot.TopN` | 16, entries per ranking per window |
| `Hot.OnWindow` / `Hot.Label` | nil; the former receives notifications, the latter maps or drops exported keys |

History or notification requires at least one tracking table. An explicitly independent window has a minimum width of `(⌈total slots/16384⌉ + 1) × PeriodicInterval`. Slots are collected per shard as a whole, and 16384 is the per-cycle collection budget target; a shard larger than that may exceed it.

Recording into a tracking table is O(1): lookup is an open-addressing hash and eviction is an index min-heap. Each shard also keeps a conservative-update Count-Min pre-filter, and a new key whose estimate does not exceed the current minimum count is not admitted to a slot, so a long tail of cold keys cannot push real hot keys out of the ranking. Measurements are in the [validation document](VALIDATION.md#hot-key-tracking).

## How to read the numbers

- `Heat` and `LoadHeat` are both double-bucket weighted counts, `current + (1-φ) × previous`, with width `Heat.Delta`. `LoadHeat` is not a direct per-second load rate; divide by the window seconds for a rate approximation.
- `Loads` / `Merged` are cumulative counts since the current slot was occupied and reset after the key is evicted; they are not persistent cumulative metrics. `HotKeys(n)` sorts by `LoadHeat`, `Merged`, `Loads` descending; `n <= 0` returns all tracked keys.
- `WindowLoads` / `WindowMerged` are the events recorded since the key entered a slot within that window. `WindowRequests` is the recorded request sample count multiplied by `1/q`.
- `WindowError` is the error bound a slot inherits when it is admitted (a key let through by the pre-filter takes its estimate as the error); rankings are sorted by "observed value + error", and the true sampled count lies between the observed value and observed value + error. The request ranking still carries sampling error after scaling by `1/q`, so it is not a definite interval for the true request volume.

Gate collisions can overestimate heat, saturation and contention can underestimate it, and insufficient slots evict keys. q=1 suits small-traffic demonstrations, while the default sampling rate keeps overhead under control first. Low-frequency keys may have no samples; absence from a ranking does not mean absence of traffic.

## Windows and quality signals

The built-in clock aligns windows to the Unix offset at creation time and is not adjusted by NTP while running; a custom clock aligns from its own origin. The history window number is `(clock + offset) / Hot.Interval`.

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
| `Hot.OnWindow` | Serialized, monotonically increasing Seq, at most once, may skip windows; a notification is dropped when the previous callback has not finished or the executor rejects it |
| `Hot.Label` | Maps keys for all ranking outputs; `keep=false` drops the entry; a panic drops only that entry and increments a counter |

Returned slices and rankings in callbacks are owned by the consumer; modifications do not affect internal history or other consumers. Each exported batch uses one copy block, and label hooks are applied on the copy. Label mapping does not rewrite the original tracking tables or change totals.

Callbacks and mappings must be bounded and non-blocking; mappings may be called concurrently. Callback panics are isolated and counted in `HotExportPanics`; a busy callback counts `HotWindowBusy` and `HotWindowDropped`, and delivery failures count as Dropped. Do not wait synchronously for `Close` inside a callback, because the callback itself is work that shutdown must wait for.

The recommended pattern is "callback only signals + consumer pulls with a cursor + periodic fallback", see [hot-export](../example/hot-export/main.go). Filling gaps requires `Hot.Windows` and consumption before history is overwritten. The library ships no monitoring service; hashing keys does not remove metric cardinality, so dynamic hot keys are better suited to logs and fixed aggregate quantities to metrics.

## Overhead and troubleshooting

No slots are allocated while tracking tables are off. Request tracking writes allocate nothing on the heap and use a short critical section; contention failures increment an atomic drop counter. Origin-load tracking reuses the immutable key already owned by the flight. Window history is stored as pointer-free columnar records plus one block of key bytes, so long-retained history adds no GC scan work; `HotKeyStat` values are only materialized on export (`HotKeyHistory`, `HotWindowsSince`, `Hot.OnWindow`), with one slice allocation and one joined key string, and output retained by the caller is managed by the caller. Related regressions and benchmarks are in the [validation document](VALIDATION.md).

History is nil: check whether history is retained and whether a window boundary was crossed. No notifications: check Busy / Dropped and the shutdown state. With `DisableTicker`, `MaintenanceStep` must keep being driven; windows that are too short or maintenance that is too slow produce missing windows. `Get` in observe-only mode can still return `ErrBusy` / `ErrClosed`, and those errors must not be ignored.
