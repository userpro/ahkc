# Adaptive Hot-Key Cache Library (AHKC) Technical Design

[中文](TECH.zh.md) | **English**

> Core design and complete algorithm pseudocode
> Positioning: an in-process cache lib embedded in online services. Targeting the concurrency and lifetime constraints of Rust / C++, this document fixes the architecture, algorithms, semantics, pseudocode, and acceptance methods; it does not cover language-binding implementation or distributed capabilities.

## 1. Conclusion: absorb traffic on a bounded hot path, manage the cache with background benefit decisions

**Adopt "time-based heat admission + unified benefit estimation + SIEVE bounded candidate set + exact local replacement".** A hit performs only a protected lookup, a hard TTL check, and a few bounded policy updates; complex decisions, structural maintenance, and bulk reclamation all move to the maintenance plane. The cache keeps only results that clear the heat threshold and are worth occupying resources, and actively exits once they keep cooling.

The primary goal is **maximizing net service benefit**: the real load cost saved minus all costs of cache lookup, statistics, filling, and reclamation. By default CPU time is the unit; I/O-bound services may choose backend call cost, but a single evaluation must fix its unit. Response P99, memory, load concurrency, and correctness are hard constraints.

### 1.1 How the goals are met

| Goal requirement | Core design and decision | Conditions and acceptance evidence |
|---|---|---|
| Extremely low hit overhead, high concurrency scaling | Fixed key sharding; hits do not move list nodes or replay all events; saturated heat skips writes, benefit uses sparse sampling | Zero heap allocation for borrowed hits after warm-up; no maintenance, I/O, or unbounded policy retries; hot-key skew and multicore stress tests pass |
| Discover hot keys automatically, evict cold objects | Double-bucket gate advanced by real time; resident counters kept separate; entry/exit thresholds form hysteresis | New hot-key discovery, threshold oscillation, traffic-drops-to-zero replay; background makes the agreed scan progress |
| Improve real benefit under limited capacity | Unify frequency, load cost, and remaining TTL; minimize loss over a bounded victim set | Local solve matches the independent solver; benefit on real traces covers policy CPU cost |
| Integrate directly into online services | Async filling decoupled from business responses; constrained origin-load merging; invalidation with completion confirmation | Fill rejection does not alter an already successful load result; overload, invalidation, and late-result intersections tested |
| Correct and resource-bounded | Hard TTL, version isolation, safe holding; entry count, resident bytes, and full retained amount limited separately | Slow readers, paused maintenance, cancelled invalidation, and 72h stress stay within bounds and never resurrect old versions |
| State of the art and theoretically checkable | Low-cost data plane with size/cost-aware decisions; exact optimum within a finite action set | Compared with SIEVE, S3-FIFO, W-TinyLFU-AV; report benefit, CPU, tail latency, and gap to the offline reference |

**Feasibility conclusion: the mechanisms above form a complete design covering the goals and are ready to enter prototyping and validation. Absolute latency, throughput, and superiority over strong baselines must be confirmed by measurement and cannot be derived from the architecture description.** The first version's challenge targets are borrowed-hit P50 < 150ns and P99 < 1μs on specified hardware, plus 50M ops/s on 32 physical cores; full measurement methodology is in section 9.

### 1.2 Core decision boundaries

- **Heat decides eligibility, benefit decides retention.** Saturated heat does not take part in fine-grained value ranking, and SIEVE marks do not directly determine the final eviction.
- **Rejecting a cache fill is allowed; letting a request thread take over background maintenance is not.** Lookup, real load, fill, and invalidation each report their own result.
- **The theoretical promise is exact local optimality within frozen estimates and a finite candidate set.** An unknown future, limited search, and estimation error make overall online global optimality impossible to guarantee directly; validate the gap against the best reference matching the model.
- The first version adds no global LFU ordering, online model inference, extra window / ghost, automatic threshold tuning, or proactive cross-shard eviction. They must prove necessity via net benefit after the current loop passes validation.

**How to read:** section 1 gives conclusions first, sections 2–8 define the architecture and engineering contracts; [Appendix A](#ahkc-pseudocode) fully defines data structures and algorithm pseudocode as the implementation spec. Each core part links directly to the corresponding algorithm, and research basis plus complete validation requirements follow.

## 2. Core architecture: structures, responsibilities, and execution contract

**Every key belongs to exactly one shard; the reference index is a single-writer multi-reader concurrent hash structure with a fixed probe range, entry addresses stay stable for their lifetime, and the shard maintainer is the only mutator of resident structures.** Borrowed reads use shard epoch protection by default, and asynchronous holding uses an independent owned handle. Shard count is not thread count; execution resources are provided by the shared host.

Sections 2–7 together form the implementation contract: section 2 defines structures and reads, sections 3–4 define policy computation, section 5 defines loading and version commit, sections 6–7 define resources and progress. Use the complete reference path in the appendix as the implementation starting point; the concrete containers, memory layout, and atomic widths may be replaced, but the following ordering, state semantics, and failure behavior must not change, and the whole path must not be pre-announced as wait-free.

**Data structures and base algorithms:** [A.1 Structures and primitives](#ahkc-a1), [A.2 Budgets and index](#ahkc-a2), [A.3 Lookup and heat](#ahkc-a3).

```text
external logical request ──sparse sampling──► bounded sample buffer ──► benefit estimation
      │                                          │
      ▼                                          │
shard index + reader protection + current-time TTL check │
      ├─ valid hit: resident heat / SIEVE mark → return view │
      └─ miss: heat gate → bounded origin-load merge → loader │
                                      ├─ return business result
                                      └─ bounded fill candidate
                                                 │
shard maintainer: eligibility check → SIEVE forms Q → exact S* → resource reservation → publish
      ├─ control: invalidation and version fences, shutdown
      ├─ lifetime: timing-wheel expiry, sustained low-heat removal, safe reclamation
      └─ global maintenance plane: idle quota transfer, progress checks, statistics aggregation
```

### 2.1 One request and one fill

1. The external entry point resolves the full key and decides with a uniform probability whether to record a benefit sample. One logical request counts once; internal retries, second lookups, and background operations add no heat or request benefit sample.
2. After the protected lookup, check readability, namespace generation, and the hard deadline against the current monotonic time. A valid hit updates resident heat and the visited mark and returns the protected result.
3. A miss updates the non-resident gate and then joins or starts a bounded load. External requests waiting on the same load each count one heat event as well.
4. A successful load delivers the result per the business contract first; then it checks cacheability, actual retained size, version, TTL, and latest heat, and attempts to commit a fill candidate. A key that was not hot when the load started but is hot when it finishes can still become eligible.
5. The maintainer re-validates the candidate, computes benefit and the replacement set, and publishes only when all eligibility, net-benefit, and resource constraints hold. Rejection reasons are observable, and callers do not wait for publication.

### 2.2 Core structures and required fields

The following are logical fields; no specific language type is required and all fields need not share a cache line. Read-only identity / value, hit-updated fields, and maintenance fields should be laid out separately; all capacities are capped at initialization.

| Structure | State that must be stored | Mutation and lifetime |
|---|---|---|
| Cache / Namespace | Fixed shard table, C/B/M, namespace config plus namespaceGen, shutdown state, global free quota | Modified by controlled configuration / control operations; lifetime covers all unreleased objects |
| RequestContext | Key / shard of this logical request, whether already sampled, whether hit or miss heat was recorded | Temporary state within the call scope, holds no Entry; reused by second lookups, retries, and post-lookup loads to prevent double counting |
| Shard | Index, heat gate, benefit sketch, three input queues, SIEVE queue and hand, independent index slot coldCursor, timing wheel, quota ledger, fillFence, maintenance owner / pending state | Resident structures and committed ledgers are modified only by the maintainer; concurrent reservation credits come from the quota pool, and other interactions are limited to agreed atomic fields and queues |
| Entry | Full key, stable entryId, namespaceGen, immutable value, hardDeadline, billed size b, readable state; heat buckets, visited, graceUntil, lowSince; policy / timer links, retirement mark, owned hold count | Identity / value / deadline are immutable after publication; requests update only heat, visited, and hold count, everything else belongs to the maintainer |
| HeatCell | epoch, current, previous | Requests update a bounded number of times, the maintainer reads / advances; the composite state must be consistently readable |
| ValueStats | Double-bucket weighted counts and sample counts, statistics generation, valid window, dropped-sample / overflow marks; limited prior and cost table, model version | Maintainer-exclusive writes; holds no strong reference to an Entry or value |
| Flight | Full key, flightId, version token, phase, leader result, waiter set, real I/O and response delivery quota, single completion mailbox, resumable Work, completion / cancellation state, individual deadlines | A separate bounded concurrent state machine handles register, join, and complete; the maintainer only handles handoff and cleanup and never runs loaders serially |
| FillCandidate | key, result ownership, size, validity basis / hardDeadline, version token, expectedEntryId, flightId, handoffDeadline, pending budget credit, commit phase and completion cell | Enqueued after successful reservation; a phase transition grants the single right to commit, and the terminal state notifies the flight |
| Control / Retired | Control identity, target and confirmation state; retired object identity, retirement epoch, retained size, hold count, reclamation state | Control must reach a terminal state; retired records are kept until safe release with the ledger returned |

entryId and flightId are not replaced by addresses and stay unique while old messages may still be alive. expectedEntryId is the identity of the old entry a candidate may replace; an empty value means only first publication is allowed. The namespace generation and shard fence prevent resurrection after invalidation, while expectedEntryId additionally prevents a late result in the same generation from overwriting a newer publication.

### 2.3 Input channels and synchronization boundaries

| Channel | Enqueue / acceptance condition | When unsuccessful | Responsibility after success |
|---|---|---|---|
| Benefit sample | Fixed striped buffer, bounded enqueue attempts | Drop the sample and mark the quality window | Process by event time; never affects lookup success |
| Fill candidate | Reserve a pending slot, bytes, and completion handoff state first | Drop caching without altering an already successful load | Exactly one publish / reject terminal state; release or transfer all reservations |
| Invalidation / shutdown control | Independent queue reservation succeeds | Return "not accepted" with no effect | Keep completion responsibility; confirmation means the effect was committed, not merely enqueued |

Index read-only access, flight registration / completion, and maintenance scheduling are three independent synchronization domains. Flights use a key-striped short critical-section state machine: only lookup, state transition, and quota ownership, with a bounded number of ownership attempts and BUSY on contention failure; the critical section never runs loaders, callbacks, weighers, or I/O waits. A flight must be able to end itself from the completion path and cannot depend on a free slot in the fill queue.

**Correctness state must not be lost; policy hints may be approximate.** Entry publication establishes a "initialize first, then visible" synchronization relation; reader protection, validity state, generation, budget ownership, and completion state use synchronization that satisfies the safety proof. Heat and visited may use weaker ordering and give up after bounded attempts. A single maintainer removes structural write-write races but does not exempt synchronization with request reads, flight completion, and namespace invalidation.

Concurrent reservations follow "acquire each quota → accept only if all succeed → return acquired quotas on any failure"; quota may be temporarily underused during multi-item reservation but never oversold. A credit has exactly one terminal transition to used or returned, and releasing quota is not a droppable policy event. Formal changes to resident count / bytes are still committed only by the maintainer.

### 2.4 Fixed order of a hit read

1. Confirm the instance still accepts lookups, otherwise return CLOSED; obtain the shard's epoch protection from the pre-registered context, then read the index, and use an Entry only after the full key comparison succeeds. Insufficient registration / nested protection quota returns BUSY without unbounded temporary allocation.
2. Read the current monotonic time t, then validate the Entry's readable state and current namespaceGen and check `t < hardDeadline`. A published Entry may only go from valid to invalid; republishing requires a new entryId.
3. A valid hit performs at most R resident heat updates; if visited is already set the write is skipped, otherwise the set is bounded; advance heat with t and do not fetch the clock again for policy.
4. Synchronously return a borrowed view bound to the guard; when it must cross threads / await, acquire ownership inside the guard first, then leave the guard. A long-lived borrow must not cross an await.
5. Return MISS only when the index confirms absence, the TTL expired, or the generation mismatches. The reference index uses the same fixed probe domain for insert and lookup, so absence can be confirmed after a full check; other indexes that cannot decide within budget return BUSY and must not disguise it as a definite miss. Logical invalidation and physical cleanup are done by the maintainer.

The index must provide complete lookup, single-entry publication, and entryId-conditional removal semantics, plus collision rejection, absence determination after a full probe, and BUSY behavior when it cannot decide. The first version reserves the index at maximum capacity and performs no online growth or resharding; the concrete layout is decided by prototype comparison and cannot change these contracts.

## 3. Heat admission and cooldown: identify hot keys by time, not by whole-instance QPS

**Algorithm pseudocode:** [A.3 Double bucket and gate](#ahkc-a3), [A.7 Proactive cooldown](#ahkc-a7).

**Use two time buckets to approximate the request volume over the recent Δ; the entry threshold is higher than the exit threshold, and counting only needs to distinguish near the thresholds.** This makes cooldown time independent of total instance traffic and avoids continuous counter writes on extremely hot keys.

### 3.1 Unified counting rules

The counting state is `epoch, current, previous`. Let φ be the elapsed fraction of the current bucket:

```text
H(t) = current + (1 − φ) × previous
T_in > T_out; H_cap > T_in
```

When a bucket boundary is crossed, move current into previous and clear current; when two or more buckets are crossed, clear everything. Progression is lazy on access or scan and never resets the whole table at bucket boundaries. Once the in-bucket count reaches H_cap, writes are skipped until the bucket changes.

A counting update follows the same transition: read a consistent snapshot → normalize the bucket by `e=floor(t/Δ)` → `current=min(current+1,H_cap)` → attempt to commit the new snapshot, retrying at most R times on failure. If the snapshot has already advanced past e, the current request's time is behind, so abandon this count and never roll the epoch back. Bucket advancement and increment must be committed as one consistent state; a new epoch must not be published before the old counters are cleared field by field. Reading heat only needs normalization on a snapshot copy, with no forced write-back for a query.

This is an approximate sliding window. In-bucket traffic distribution, counter saturation, and abandoned updates under contention all add error, so H(t) must not be interpreted as an exact request count. Time and generation fields must define overflow handling, and old state must never be treated as new state after wraparound.

### 3.2 Non-resident gate and resident heat

| Part | Defined behavior | Initial parameters |
|---|---|---|
| Non-resident gate | Fixed d×w cells per shard, the full key maps to d rows; a query takes the minimum H of the rows; updated only on miss | d=4; each cell gets at most R=2 atomic update attempts, 8 in total |
| Resident heat | Independent double-bucket counter per entry, updated on hits; writes skipped at the cap | At most R=2 update attempts each time |
| Thresholds | H≥T_in required for new admission; cooldown only after sustained H<T_out | Δ=1s; T_out=0.5T_in; H_cap=4× the instance's maximum T_in |
| Residency grace | A newly published entry counts from zero and is observed fully first, avoiding immediate exit after entry | grace≥2Δ; the first version observes new Entries from zero and does not enable proactive refresh |

The gate width w is determined by the **non-resident cardinality and traffic in the window**, not by cache entry count alone. Hash collisions may overestimate, and lost updates or saturation may underestimate; the original CMS error guarantee does not apply verbatim to this modified statistic. Width and thresholds are finally fixed by exact window replay. [Count-Min Sketch](https://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf)

### 3.3 The proactive cooldown state machine

The maintainer executes in fair scan order:

```text
already expired → logically invalidate and schedule removal
still in grace → skip this round
H ≥ T_out → clear low_since
H < T_out and low_since empty → record the current time
H < T_out and sustained coldFor → re-read state and heat, remove if still low
```

Cooldown uses the fixed index slot coldCursor and does not depend on the SIEVE hand, which only advances during fills; the timed trigger covers the whole index by slot quota, and inserts and deletes do not move the cursor. New entries are protected by grace; when no valid heat can be read, lowSince is cleared and observation restarts. Hits occurring between the final re-read and logical removal may overlap with the cooldown operation, so per-request exact detection of continuous low heat is not promised.

`coldFor≥Δ` is the starting point. "Sustained low heat" means consecutive maintenance observations satisfy the condition; no claim is made about tracking every instantaneous peak. Between the high and low thresholds the current residency is kept, reducing oscillation around the thresholds.

Let the full scan period be at most P and extra scheduling lag at most L; once an entry finishes grace and stops being accessed, the logical cooldown latency budget is `2Δ + coldFor + 2P + L`. The first-publication case counts the remaining grace separately; physical release also waits for readers to exit. If the host provides no timed maintenance, reads still honor the TTL, but timely removal during idle periods cannot be promised.

For example, with Δ=1s, T_in=20, T_out=10, coldFor=1s: about 20 accesses within the window grant admission eligibility; after residency the entry must drop below 10 and be observed for 1s before cooling. After expiry or full exit the gate heat is re-accumulated from zero by default; short-TTL scenarios must be tested together for discovery latency and origin-load cost.

## 4. Benefit and replacement: unified estimation, exact solve over a finite action set

**Algorithm pseudocode:** [A.4 Benefit estimation](#ahkc-a4), [A.5 Exact selection and commit](#ahkc-a5).

**For each fill, first estimate the candidate's net value over its remaining validity, then find the minimum-loss set that releases the required capacity.** Heat only judges eligibility; a large object must be compared against every object freed for it.

### 4.1 Estimation inputs and behavior when insufficient

Hits, misses, and load waiters are sampled independently at the external entry point using the same basis. A sample carries the key policy fingerprint, namespace generation, event time, and the probability p of that occurrence, and holds no strong reference to an entry or value. The maintainer uses fixed-budget statistical structures, aggregating and aging by event time; a policy fingerprint collision affects only the policy, while the index still compares the full key.

With independent sampling and no transport loss, the window request-count estimate `N̂=Σ_samples 1/p_j` has expectation equal to the true request count; later structural collisions, sample loss, and future prediction do not automatically inherit unbiasedness. Permanent per-key hash sampling decisions are forbidden; when p changes, each sample keeps its original probability.

The first version estimates benefit with a fixed-width frequency sketch independent of the heat gate, written exclusively by the maintainer. Each row stores the double-bucket weighted count and sample count over observation window Δ_v: samples join the corresponding bucket by event time with weighted increment 1/p_j; a query weights by the section 3 double-bucket formula, takes the row minimum, and divides by Δ_v for the request-rate estimate. Counts are not threshold-saturated; insufficient warm-up, lateness beyond the window, and numeric overflow all surface as quality states. Row count starts at 4 for measurement, width is calibrated to the post-sampling window cardinality, and collision error feeds the estimate replay.

| Estimation input | How it is obtained and applied |
|---|---|
| Request rate λ̂ | Queried from the sketch above; after statistical rebuild it warms up again and is used as an independent estimate only after covering a full double-bucket observation period |
| Estimate quality | Records approximate sample volume, window age, and drop state; the initial minimum sample count n_min=8 is only an adjustable engineering gate, not a statistical confidence guarantee |
| Low-sample estimation | By default uses calibrated finite namespace priors λ₀, c₀ for candidates and residents alike; when absent or expired, new fills depending on that estimate are deferred |
| Single avoidable cost c | Measured on the real loader slow path and aged per namespace / limited cost class; no extra origin loads are made to obtain samples |
| Effective origin-load rate r̂ | A calibration trace replays "no cache, singleflight kept", arrivals during a busy period become waiters; a bounded lookup-table model keyed by request rate and load duration is generated per namespace and maintained online |
| Estimation degradation | Any known dropped sample or numeric overflow contaminates the corresponding shard / window; that window is not used for benefit fills and is re-enabled after a complete healthy double bucket. Model expiry separately forbids using that model |

Priors and origin-load models come from calibration traces as namespace configuration and record target workload, version, and validity; when the workload leaves the calibrated range the model is marked invalid, and "zero samples" must not be temporarily treated as zero heat. Model switches must record mode and age to prevent mixing units. Sample drops may correlate with hot keys and cannot simply be corrected by dividing by an average retention rate.

Each estimate yields only three states: a healthy complete window with sufficient samples is MEASURED; healthy but warming up / insufficient samples with a valid prior is PRIOR; everything else is UNAVAILABLE. A lookup-table cost model must be valid at the same time; UNAVAILABLE forbids starting a fill that depends on that value. Known sample drops must not be masked as PRIOR. Each decision freezes the statistics time and an immutable model version and does not reuse the previous G as-is for a new remaining TTL. It also records the consumed sample prefix and maximum lag; a lag beyond maxSampleLag disqualifies the estimate for benefit fills. MEASURED does not mean all concurrent requests have been accounted for.

A sample is written to its bucket when it falls in the maintainer's current or previous bucket, otherwise it is dropped and marked; dequeue time is never treated as a new request time. The cost table uses fixed namespaces / bounded size classes: load CPU and duration are captured on the completion path, and h, k, K are calibrated as bounded lookup parameters in prototype benchmarks, each with a version and maximum age. The first version neither estimates cost nor trains models on the hit path.

At p=1/1024, 50M requests/s yields about 48.8k samples/s, which only indicates cost magnitude; the expected samples per key per window is about pλΔ_v and may not reach n_min. Prototypes must validate prior share, discovery latency, and estimate quality before choosing p within the fixed buffer and CPU budget; extremely low sampling cost and high-precision estimates for all keys cannot be assumed at the same time.

### 4.2 Decision objective

All objects are valued at the same decision time t, the same prediction horizon W, and the same cost unit:

```text
τ_i = hardDeadline_i − t
G_i = ∫[0, min(W, τ_i)] (r̂_i(u) × ĉ_i(u) − ĥ_i(u)) du
L_v = G_v + k_v
```

G_i is the future net benefit expected from retaining the object; r̂ is the **avoidable real load rate**, h is the expected ongoing cost of serving the object, k_v is the removal cost, and K_a is the candidate's not-yet-paid installation cost. A completed loader is a sunk cost and is not deducted again from the candidate's future benefit. The shared cost of lookups and policy is booked to the overall service ledger.

Model inputs r̂, c, h, k, K must be finite non-negative values in the same unit, and G may be negative; objects with τ≤0 are treated as expired first and no negative time integral is computed. Unfinished or invalid estimates must not silently default to 0.

The first version may treat r̂, c, h as constants under a short-term stationary assumption, giving `G_i=(r̂_i×ĉ_i−ĥ_i)×min(W,τ_i)`. Do not multiply every merged request by a full loader cost; if request hit count is chosen as the objective instead, state it uniformly as `G_i=λ̂_i×min(W,τ_i)` and evaluate service CPU separately.

### 4.3 Forming the candidate set and computing the capacity gap

SIEVE only provides a low-cost search order: new entries join the queue head; a hit only attempts to set the visited bit; the scan hand moves from tail to head and wraps at the head. An entry with the bit set has it cleared and is skipped, otherwise it may join Q. Hits do not adjust the list. [SIEVE](https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo)

The total scan limit per fill is `B_scan=64` and the candidate set limit is `m=8`; this is the budget **for the whole fill**. Expired or confirmed-cooled entries are handled first within the lifetime quota, and capacity is then recomputed; this must not become unlimited table clearing. At most one loop over the starting queue may be visited per fill; entryIds within Q are unique and the old entry of the same key being replaced is excluded. The hand position persists after the scan; if no feasible set exists within budget the fill is rejected rather than turning into a full-table search.

When the scan encounters an invalid / expired entry, it cleans it up only if the lifetime and retirement budgets allow, otherwise it skips the entry while still counting it as resident; such an entry is never a Q member with future benefit. visited is a hint: scan reset and hit set may race, and hits after Q is frozen do not retroactively cancel this plan. After candidate formation, N_s, Bytes_s, and the estimation time are frozen so the gap is not carried over from a stale state while cleaning.

Freeze the identity, version, size, estimates, TTL, and resource ledger of the candidate and Q. Let a be the new entry and O the set of same-key old entries that must be replaced; O has at most one element and must not appear in Q; for first publication O is empty. For shard entry quota C_s and byte quota B_s:

```text
ΔN = 1 − |O|; ΔBytes = b_a − Σ[O] b_o
r_n = max(0, N_s + ΔN − C_s)
r_b = max(0, Bytes_s + ΔBytes − B_s)
F(Q) = {S⊆Q: |S|≥r_n, Σ[S] b_v≥r_b, combined resource check passes}
```

The "combined resource check" accounts simultaneously for the whole `O∪S`: retired bytes, retired records / reclamation handoff slots, removal work quota, timing-wheel schedulability, and index installation position. The default conservative assumption is that every removed object must be retired: `retiredUsed+retiredReserved+Σ[O∪S] b_v≤retiredLimit`, plus a check of the final residency against the other classified budgets. Space from future guard exits or destructors must not be pre-spent; objects being individually removable does not mean the group is removable at once. The additive billing basis for b is in section 6.

### 4.4 Exact selection and commit

```text
S* = argmin[S∈F(Q)] Σ[S] L_v
ΔG* = G_a − K_a − Σ[O] L_o − Σ[S*] L_v
publish condition: eligibility still holds, resources still available, ΔG* > 0
```

| Case | Exact solution method |
|---|---|
| No capacity gap, the empty set is resource-feasible, and all L_v≥0 | S*=empty set |
| Victims with equal size b, L_v≥0, and no other resource constraint distinguishing subsets | Take the k smallest losses, k=max(r_n, ceil(r_b/b)); infeasible if fewer than k |
| General case | Enumerate all subsets of the actual Q, at most 256 when m≤8; filter by full constraints then take the minimum loss |

If a required object's estimate is unavailable, F(Q) is empty, or the solve does not finish, reject the candidate; a set found partway must not be labelled exact optimum. Ties are broken by fewer retired bytes, then fewer removed entries, then stable identity. The default benefit threshold is 0; an extra safety margin changes the allowed action set and must be evaluated separately.

**Example:** a new object of size 6, benefit 14, install cost 1; Q contains `(size 6, loss 13)`, `(3,4)`, `(3,5)`. To release 6, choose the latter two for a net gain of `14−1−4−5=4`; taking only the first gives net gain 0, so the replacement must be rejected.

Candidate commit phases are `QUEUED→COMMITTING→terminal`. An expiry / cancellation party may only move QUEUED to a terminal state; the maintainer may modify the resident set only after successfully entering COMMITTING, and it alone produces the terminal state afterwards. A cancellation mark does not mean the payload was released; budget is returned only after the actual hold responsibility ends.

The commit order is fixed as follows, and all fallible preparation must complete before the first irreversible action:

1. **Prepare:** reserve target residency, retirement, index installation, and reclamation handoff resources; finish initializing the new Entry. Ordinary allocation failure, insufficient budget, or no guaranteed index installation position rejects here, and the existing resident set stays readable. Residency / index reservation may rely on net free space created by this locked removal, but those quotas must not be returned early to other plans; physical memory reservation cannot rely on future reclamation.
2. **Re-validate and take commit rights:** check the candidate token, expectedEntryId, hardDeadline, handoffDeadline, plus the identity, accounting, and state of O/S*; after confirming the handoff deadline has not passed, attempt QUEUED→COMMITTING and return the prepared resources on failure. Concurrent changes to heat / visited are allowed to rely on the frozen estimate; identity or resource changes cause rejection without unbounded recomputation.
3. **Remove:** mark O/S* unreadable, unlink them from the index, policy queue, and timing wheel by entryId, register the retirement epoch, and transfer resident billing to the reserved retirement ledger.
4. **Publish:** complete the pending→resident ledger transfer, link the new entry's policy / timer metadata, then make the fully initialized new Entry visible to index reads. Commit is serialized against same-shard invalidation; namespace generation changes are handled per section 5.
5. **Finalize:** release surplus reservations, write the single publish / reject result, and lift the flight's handoff retention; physical reclamation proceeds separately.

After step 3 no user callback or ordinarily fallible allocation is performed. The index must guarantee that a reserved write can complete; multi-key replacement is not required to be atomically visible to readers as a whole. If the namespace is invalidated exactly then or the hard TTL passes within the commit gap, the old generation / expired result cannot pass the read check; an already completed eviction remains valid and finalizes as "commit invalidated" with budget-bounded cleanup, and the local benefit promise does not cover such external state interruptions.

Same-key replacement feeds O uniformly into capacity, benefit, and retirement calculations; the future G of an expired / logically invalidated old entry is 0, but removal and retention cost still count. A candidate produced by the initial load may be installed only while expectedEntryId still matches and never automatically overwrites a newer publication from the interim; proactive refresh is off by default.

**Same-key and memory ledger example:** shard C_s=3, B_s=12, resident A_old=2, B=6, C=4; A_old has expired but is still in the index. The new A has size 6 and matches the identity condition, so O={A_old}, r_n=0, r_b=4. If the optimal S*={C} and net benefit is positive, after commit the residents are the new A and B, 2 entries and 12 bytes; A_old and C additionally occupy 6 bytes of retirement budget. The new A was already counted as pending, so the commit only transfers its ledger. If retirement headroom is below 6 the candidate must be rejected before removal; the final residency being within 12 does not by itself make it committable. The "bytes" in this example use the scaled billing unit.

### 4.5 Optimality conclusion and its validation boundary

For a frozen Q and estimate, the candidate benefit is constant, so minimizing total victim loss is equivalent to maximizing this fill's net benefit; complete enumeration covers all feasible subsets and is then compared with "do nothing, benefit 0", yielding the **exact optimum within that action set**. The fast path must agree with the general solution.

This conclusion does not cover resident objects outside Q and does not remove prediction error. In the theoretical reference, equal-size static selection is Top-K; variable-size selection contains 0/1 knapsack, where greedy by "benefit / byte" cannot guarantee integer optimality. For example with capacity 10 and objects `(6,12), (5,9), (5,9)`, the density greedy gives 12 while the optimum is 18. Solve small-scale optima with an integer model and derive static upper bounds from a linear relaxation; for dynamic policies use an offline model with matching constraints, see section 9.

The set comparison borrows the size-aware admission idea of W-TinyLFU-AV; the bounded subset solve here is additional design whose benefit and cost must be validated independently. [W-TinyLFU-AV](https://arxiv.org/abs/2105.08770), [CacheSack](https://www.usenix.org/conference/atc22/presentation/yang-tzu-wei)

## 5. Online semantics: hard TTL, constrained loading, invalidation without resurrection

**Algorithm pseudocode:** [A.6 Load and response](#ahkc-a6), [A.7 Invalidation and timing wheel](#ahkc-a7), [A.9 Shutdown](#ahkc-a9).

**Lookup correctness is independent of background progress; a business response from a successful load is independent of cache admission; invalidation completion must form an explicit version boundary.**

### 5.1 TTL and entry lifetime

```text
candidate → published readable → logically invalidated → unlinked → retired pending reclamation → released
```

A hit is decided by the **current monotonic time** after the protected read: `now < hardDeadline` and the entry is valid with a matching namespace generation. A stale clock refreshed periodically by the maintenance thread must not be used. The reference implementation times conservatively from the flight registration instant, which is never later than loader start, and takes the earlier value when the business provides an earlier deadline; queueing and publication never extend validity. Optional jitter only shortens the TTL and is off by default.

The TTL guarantee applies at the linearization point of the lookup check; a thread paused afterwards or a caller continuing to hold a view does not revoke an object already obtained. Explicit invalidation likewise does not reclaim memory that is being legitimately held.

Background expiry is scheduled on a bounded hierarchical timing wheel configured with tick, slot count, and maximum span; each processing step re-checks the exact deadline and entry version. Queue marks and timing-wheel membership are each counted in metadata. Catch-up after a pause is processed in budgeted batches. Whether or not the wheel runs in time, reads can never hit an expired object.

The first version uses K fixed slots per level, with level ℓ stepping by `tick×K^ℓ`. Round the deadline up to a base tick number T and select the lowest level that can cover it by remaining ticks, with slot `floor(T/K^ℓ) mod K`; the total number of levels must cover the configured maximum TTL. When the base level wraps, higher levels advance and entries are re-linked downward. Processing an entry compares the real hardDeadline first: expired entries are logically invalidated, and unexpired ones are moved down or re-linked to a future processing point; premature expiry or linking back into an already processed slot is forbidden. Each entry has exactly one effective timer membership; unlinking revokes it at the same time, and cancellation / cascading still validates entryId. Long pauses catch up per slot, entry, and cascade quota with saved cursors, while other maintenance categories keep their own quotas.

### 5.2 Origin-load merging and fill handoff

**At most one actually active leader per `(namespace, full key, namespaceGen, fillFence)`; after a version change old and new loads may run in parallel, but all of them consume real I/O quota.** Delivering business responses to existing waiters and deciding whether to cache are finalized separately. [singleflight semantics reference](https://pkg.go.dev/golang.org/x/sync/singleflight)

| Flight state | Allowed actions | Responsibility when leaving the state |
|---|---|---|
| LOADING | Join within the same version and waiter budget; the leader loads outside locks | Release I/O quota only when the real I/O ends; a successful result first enters the already reserved response delivery responsibility |
| COMPLETING | The source operation finished; deliver existing waiters first, then prepare the cache candidate outside the gate; new joins get a brief BUSY | The completion phase is stored in a fixed Work slot; resume after a pause without consuming the result twice or creating duplicate candidates |
| TIMED_OUT_IO_RUNNING | Already waiting requests finalize as timeout; new requests with the same identity return BUSY and no leader is created | Clean up the orphan record after the underlying operation truly ends; the result is no longer used for filling |
| COMPLETED_PENDING | The candidate passed the initial screen, handoff reservation is enqueued; reuse is allowed only when the token still matches, hardDeadline / handoffDeadline are unmet, and the waiter budget allows | Stop new joins after publish, reject, invalidation, or handoff timeout; never extend validity because waiters increased |
| TERMINAL | No further joins; registered responses are delivered / cancelled individually | Return each quota exactly once according to its actual hold responsibility; late callbacks are ignored by flightId, and records still held stay accounted |

The fixed steps of a first miss are:

1. Update the gate once; read namespaceGen and fillFence, observe the current entry identity under protection, and re-check that the token has not changed to obtain expectedEntryId. If no stable observation can be formed within a bounded number of attempts, return BUSY instead of looping until invalidation stops.
2. Look up the same-identity record in the flight short critical section; if it can be joined, reserve a waiter slot. Before creating a leader, acquire the active record, real I/O, maximum-result response delivery, and first waiter quotas; on failure return everything and do not call the loader.
3. After the leader registers, re-check the cache and the token. On a valid hit, acquire an owned hold inside the guard, then notify waiters after leaving the guard and release unused load quota; if the token expired or the index returns BUSY, finalize to a retryable state and return unused quota. Only after confirming a miss does it leave all reader protection and the critical section and call the loader.
4. When the real load ends, release I/O quota and deliver the successful result to registered waiters per the response contract. At the same time check cacheability, size, deadline, and the latest gate; a unique candidate exists only after the pending slot / byte reservation succeeds.
5. The candidate deadline is fixed to `min(hardDeadline, loadCompletedAt+handoffTimeout)`. The Flight retains the response through Delivery, while the candidate independently holds the Entry to be filled; the immutable underlying result may be shared but retention responsibility is booked separately. Publication or rejection notifies the Flight through the reserved completion state and does not depend on another ordinary queue that may be full.

handoffDeadline bounds the validation instant at which COMMITTING commit rights are acquired; a committer that already started is finished by the maintainer, and a flight timeout must not concurrently publish a contradictory fill terminal state. When the handoff deadline passes without publication, new requests stop reusing completed_pending, but results still held by the committer are not released early; if already in COMMITTING, new same-identity requests return BUSY until this commit finalizes and no competing leader is created. Commit duration is bounded separately by the maintenance budget and must not disguise a scheduling pause as completion; hardDeadline is always guaranteed independently by each read.

The response delivery reservation and the pending reservation are separate: the former guarantees bounded delivery capacity by maxResultBytes before the loader starts, while the latter may fail under cache pressure. Results not yet received by a slow consumer keep counting against the response budget and are not considered delivered when the notification is sent. The host must bound the loader's maximum output and temporary buffers; exceeding the result limit is a load contract error and memory is not promised for arbitrarily large results.

Cancelling a waiter only releases that waiter's own wait. Request cancellation or loader timeout is not the end of the underlying I/O; after the cache load deadline the flight still occupies I/O quota and enters TIMED_OUT_IO_RUNNING until the real completion. Response and waiter cancel / complete races must settle exactly once. The first version does not cache errors automatically, does not fall back to stale values, and does not enable proactive refresh.

### 5.3 Invalidation ordering and shutdown

Ordinary fills may be dropped, but invalidation uses an independent bounded control path: an unaccepted operation returns an explicit state immediately, while an accepted one must eventually complete or report failure and must never vanish silently. A successful confirmation means **logical invalidation is complete**, not that physical memory has been released.

| Operation | Commit rules and external guarantees |
|---|---|
| Single-key invalidation | The shard maintainer increments fillFence, then makes the target unreadable, then confirms; load results from the old fence must not regain readable status |
| Namespace invalidation | Advance namespaceGen; reads check the generation, the old generation is logically hidden immediately, and entries are removed in background batches |
| Late fill / timer | Validate identity, generation, and version together; an old timer cannot delete a new entry, and address reuse never serves as identity evidence |
| Shutdown | Stop accepting new work, finish accepted control and waiters; report stop completion and safe reclamation separately, and timeout never force-releases objects still held |

Fill publication and invalidation must share a consistent commit order; checking the version once at a very early instant and then publishing unconditionally is not allowed. Queries that start after invalidation confirmation must not obtain the old readable version; queries overlapping invalidation may return according to the linearization order. Invalidation does not by default revoke responses of business loads already started, but old results must not re-enter the readable cache.

**The two version kinds take effect separately:** single-key invalidation and fills of that shard are ordered within the same maintainer, and successful confirmation happens after the fence advances and the target is logically invalidated. The linearization point of namespace invalidation is the controlled publication of the new namespaceGen, without waiting for all shards to be scanned. If it races with a fill, an old-generation Entry cannot pass the generation check even if already installed; the maintainer finalizes such a candidate as STALE and schedules budgeted cleanup.

A candidate must also pass a conditional install: an empty expectedEntryId only allows installation while the key is still absent, and a non-empty one only while that old identity is still in the index. If the old identity was already removed normally, the candidate is conservatively rejected and the next miss re-acquires the condition, rather than keeping unbounded history tombstones to relax the rule. The extra fill rejections this causes must be reported separately in short-TTL / slow-load replay.

Example: candidate A observed old entryId=7, then entryId=8 was published for the same key; even with the same namespaceGen and fillFence, A is rejected because expectedEntryId does not match. After single-key invalidation, comparing only entryId is insufficient—the old fence must be rejected as well; the two checks cannot substitute for each other.

fillFence avoids keeping tombstones for unbounded historical keys, at the cost of conservatively rejecting old in-flight fills of other keys in the same shard; their already resident values are unaffected. High-frequency invalidation scenarios must quantify this loss. The total number of namespaces, generations, and shutdown state must also be bounded with overflow handling.

## 6. Capacity and native memory: bound the full retained amount, slow readers affect admission only

**Algorithm pseudocode:** [A.2 Budget credits](#ahkc-a2), [A.8 Safe reclamation](#ahkc-a8), [A.9 Quota transfer](#ahkc-a9).

**Entry count / resident billed bytes decide what fits, while the fully managed retained amount decides the library's resource safety; the two are measured separately.** Values are immutable by default; borrowed views serve low-cost synchronous access and owned handles serve cross-thread or asynchronous holding.

Each Entry's b covers the retained capacity of key and value plus entry metadata and must be additive; allocations shared across Entries are conservatively billed per entry by default. M aggregates all managed retention responsibilities, allowing conservative double billing but forbidding under-counting. If physical deduplication with stable allocation identity is supported, it applies only to M and cannot directly change the resident capacity constraint computed from Σb in section 4.

### 6.1 Three-tier quota and resource ledger

Global entry count C and resident bytes B are granted to shards as quota, always satisfying:

```text
Σ C_s + C_free + C_inTransit = C
Σ B_s + B_free + B_inTransit = B
N_s ≤ C_s; Bytes_s ≤ B_s
```

Each shard also has an initialization-fixed index cap `C_s^max` with `C_s≤C_s^max`, and index slots plus the maximum load factor are reserved accordingly. Quota transfer cannot exceed the receiving shard's physical cap; a full shard can still free space through a locally feasible replacement, otherwise the candidate is rejected and capacity fragmentation surfaces. The first version does not solve this with request-triggered growth.

Quota moves only on the maintenance plane, in-transit quota is held by bounded grant tokens, and ownership transfers exactly once. The first version transfers only **idle quota** beyond actual occupancy and already committed fills: the donor deducts first, the global pool receives, then the receiver is granted; nothing is double-spent temporarily and other shards' hot keys are not evicted to satisfy this fill. If no quota is available the current candidate is rejected and the request never waits for cross-shard rebalancing.

| Retention class | Content that must be counted | Behavior when exhausted |
|---|---|---|
| Fixed M_fixed | Maximum index slots, gate, benefit statistics and sample buffers, timing wheel, solver scratch, reader slots, metric stripes | Validate the budget at initialization; no online growth in the first version |
| Resident M_resident | Keys, values, and entry metadata still retained by the cache in the index; logically invalidated but unremoved entries still count | Reject new additions or perform a budgeted replacement |
| Handoff M_pending | Completed results, pending fill candidates and the objects they actually retain | Reject new handoffs; business results are still delivered |
| Retired M_retired | Objects still held by readers after unlinking, old index buckets, and batches awaiting release | Stop increasing retirement volume and any new fill depending on it |
| Response M_response | Results still held by the library after the loader completes, awaiting a registered consumer; reserved as maxResultBytes before the load starts | Reject new loaders; reserved responses are not lost because a fill was rejected |
| Load and control M_aux | Active / orphan load records, waiters, accepted control, reclamation handoff records | Reject new acceptance per class by count and byte caps |

The requirement is `M_fixed + M_resident + M_pending + M_retired + M_response + M_aux ≤ M`, limiting both used amounts and unfulfilled reservations at the same time. Every retention responsibility has exactly one ledger owner; the same candidate must not be missed during pending→resident→retired transfer. Independent response and cache hold responsibilities may be conservatively billed separately. Slot limits and byte limits exist side by side, and quota is returned exactly once.

A new candidate moving from pending to resident does not mean a new value allocation, and an old entry entering retired does not mean the physical space was released. Therefore extra allocations during commit must be reserved separately, and "space freed after eviction" cannot offset memory not yet released. Removing a flight from the table likewise does not mean the record and response were released; retention still held by callbacks / consumers keeps being accounted.

When the retirement budget is full, TTL / invalidation may still logically hide entries first and keep counting the original occupancy until budget exists to unlink them; confirmation must not release early, and "unreadable" is not a reason to stop accounting.

### 6.2 Holding, reclamation, and byte accounting

A borrow must be bound to a reader protection scope; returning a bare reference with an unconstrained lifetime is forbidden. An owned handle can extend the value lifetime, and its reference counting and single-hot-key contention are measured separately rather than mixed with the borrow path.

The first version chooses a **per-shard independent epoch reclamation domain**: readers enter a bounded protection slot before accessing the index, stay protected during the read, and leave when the borrow ends; the maintainer first stops new readers from acquiring unlinked objects, records the retirement epoch, and then waits for that reclamation protocol to confirm that all protection periods that could observe the old pointer have ended. Objects that must cross an await are converted to owned holds first so the epoch is not held across asynchronous waits for long. The epoch mechanism may follow [Crossbeam Epoch](https://docs.rs/crossbeam-epoch/latest/crossbeam_epoch/), but a library's default collection timing cannot be assumed to satisfy this design.

Releasing an Entry requires all of: removal from the index and all maintenance structures, satisfied epoch safety conditions, and zero owned hold count. Observing "no active readers right now" alone is not a substitute for the full epoch protocol. The implementation must prove the synchronization order between protection publication, pointer reads, unlinking, and safe epoch advancement.

Reclamation runs in batches on the maintenance plane; guard exit and owned handle release perform only bounded state updates without bulk scanning or arbitrary destruction. A retired record carries its own revisitable reclamation state, so the final handle release never needs to post a critical message to an ordinary queue that may be full. When slow readers exhaust the budget, new retention is paused rather than released early; independent per-shard reclamation domains keep one slow reader from directly blocking epoch advancement in every shard.

The byte budget covers the **safe upper bound of actually retained allocations**: buffers by capacity, including key and metadata, never substituting the value's logical length. A generic object that only provides a business weight can only guarantee a weight-based upper bound. After a result is formally handed over, the plain business copy held by the host, loader temporary buffers, allocator fragmentation, and RSS are not equivalent to the library budget; the host must bound those resources. Owned handles held by the cache are still tracked by the library and cannot be moved out of the ledger early as if they were plain business copies.

## 7. Work volume and overload: every maintenance class has a quota, requests never absorb backlog

**Algorithm pseudocode:** [A.9 Maintenance and scheduling](#ahkc-a9).

**A single maintainer keeps structural updates ordered and per-class quotas keep classes from starving each other; beyond finite queues, the actual work per round must also be bounded.**

### 7.1 Scheduling contract

Each round configures non-zero progress quotas for control operations, expiry / cooldown, sample aggregation, fill solving, scanning, and reclamation separately, and limits event count, scan steps, subset checks, removed objects, and released bytes. A soft time budget is supplementary, because bounding only by "at most K events" cannot control the long tail.

Each round serves the bounded control quota first, then the expiry / cooldown, sample, fill, and reclamation quotas; when the soft time budget is reached, work resumes from the unfinished class rather than always restarting at control. Inside each class a cursor-preserving FIFO / round-robin order is used. A pending fill candidate cannot block the queue head indefinitely for lack of resources: this round gives it a reject terminal state and later requests compete again.

The host provides the executor and timed trigger, and the maintenance owner states are `IDLE→QUEUED→RUNNING`: a producer publishes input and pending first, then tries to change IDLE to QUEUED, and the winner submits the task; while QUEUED / RUNNING it only records pending work. When RUNNING completes its quota, the next round is scheduled if work remains; after exiting to IDLE it must re-check input / pending and try to regain the scheduling right, covering the producer-exit intersection. After the owner is handed over, the old task no longer writes resident structures.

When scheduling is rejected it returns to a retryable state, keeping pending and pausing new fills, and the timed trigger retries within bounds; recursive retries and caller-runs are forbidden. The timed trigger does not depend on new requests and drives idle TTL, cooldown, flight deadlines, and reclamation progress. If the executor stalls these completion deadlines cannot be guaranteed, but TTL reads, budgets, and the ban on early release still hold.

Loaders, arbitrary user callbacks, weighers, and uncontrollable destruction never run inside the structural maintenance critical section. The first version reserves the maximum index and fixed structures at initialization; allowing growth later must account separately for old/new index overlap and cannot reuse the first version's fixed-budget conclusions.

### 7.2 Bounded does not mean cheap

| Path | Algorithmic upper bound and main cost |
|---|---|
| Valid hit | One protected lookup, current-time check; resident counting with at most R attempts; visited bit set and one sampling decision; no list movement |
| Miss policy | dR counting attempts; bounded flight register / join; explicit return when resources are insufficient |
| One fill | Total scan ≤B_scan; at most 2^m subsets with a direct upper bound of O(m·2^m); prefer the equivalent exact fast path |
| Maintenance round | Per-class limits on processed items, scan volume, retired / released bytes; progress is retained for the next round |

At 50M requests/s with 5% misses, if every miss triggered 256 subset checks, the checks alone would reach 640M/s. Therefore per-shard fill entry / byte rates and solve budgets are mandatory, and the fast-path share and real per-fill cost must be measured. A budget that is too small lowers hit benefit, so the design cannot be judged by Get throughput alone.

Configure maintenance CPU as `sample rate×processing cost + fill rate×decision cost + index slots/P×scan cost + expiry and reclamation cost`. If actual queue age, scan period, or retirement backlog keeps exceeding limits, maintenance capacity is insufficient and either the admission rate must drop or the host budget must grow.

### 7.3 Deterministic overload behavior

| Pressure / fault | Required behavior |
|---|---|
| Fill queue, quota, solve, or retirement budget exhausted | Reject the cache candidate; a successful loader result is delivered normally |
| Real loader concurrency, active keys, or waiters full | Reject the new load / wait with a distinguishable overload result; never call the loader while bypassing quota |
| Control path full | Refuse new control; accepted operations keep their completion responsibility |
| Sample drops, statistics rebuild, or invalid estimates | Enter PRIOR or UNAVAILABLE per section 4; contaminated windows forbid benefit fills while residency TTL and heat maintenance continue |
| Simultaneous expiry, or recovery after paused maintenance | Reads miss strictly; the background processes in batches and real loads stay bounded |
| Slow readers or a loader ignoring cancellation | Retained objects / I/O keep being accounted and new additions are limited; never release early or restart unboundedly |

## 8. Integration and configuration: one complete default algorithm, every parameter tied to budget and validation

**The host owns business correctness and execution resources; the lib owns cache reuse and its lifetime.** The following contracts must be settled before turning the design into implementation tasks.

### 8.1 Semantics of external operations

| Operation | Result and holding mode | Explicit boundary |
|---|---|---|
| Read-only query | HIT + borrowed view, or MISS / BUSY / CLOSED; owned queries are chosen explicitly | Starts no load; BUSY is not definite absence |
| Query and load | An existing value or loader result returned as an asynchronously deliverable owned result; load errors and overload are reported separately | No epoch guard held across an await; a successful response does not mean cached |
| Conditional fill | Generated by the Flight in this design, with terminal states PUBLISHED / REJECTED / STALE / EXPIRED | Not an external unconditional Put; must carry version, identity conditions, and budget credits |
| Key / namespace invalidation | Not accepted, or a completion receipt after acceptance | Confirmation confirms the logical effect and does not wait for all readers to release |
| Maintenance / shutdown | Bounded progress; shutdown confirms stop-accepting, work-finalized, and safe reclamation separately | Never fake completion while I/O / holders remain, and never occupy a caller thread indefinitely |

These are behavioral contracts; they do not prescribe function signatures, synchronization frameworks, or ABI. "Query then load" must pass the same RequestContext or be performed inside a combined entry point; the lib cannot infer from two unrelated calls that they belong to the same business request.

| Contract that must be made explicit | Content |
|---|---|
| Full key | Business dimensions affecting the result, permission / tenant isolation, and data version; key size and hash / comparison cost are bounded; a policy hash never replaces full equality comparison |
| Result | Immutability, cacheability conditions, actual retained size, maximum result size, borrowed / owned return modes |
| Validity and invalidation | TTL basis, maximum TTL, confirmation semantics for single-key / namespace invalidation |
| Loading | Sync / async mode, timeout and cancellation capability, real concurrency, waiter limits, and overload handling |
| Maintenance | Available CPU, trigger period, maximum acceptable scheduling lag, and shutdown deadline |
| Estimation | Fixed primary objective and W; calibrated priors, cost model and their validity period; fills are deferred when no estimate is available |

### 8.2 Configuration and initialization constraints

The recommended starting parameters below must all be frozen after validation per section 9:

| Parameter group | First-version starting point / required items |
|---|---|
| Capacity and safety | C, B, M, maxEntryBytes, maxResultBytes are required; namespace count, reader / nested guard slots, response and all queue / wait / retirement budgets are explicitly limited |
| Sharding | Start testing at 8 or 16 while comparing 1 / 4 / 32; fix C_s^max, slot count, and maximum load factor; probeLimit starts at 32 and never exceeds the slot count; background transfer moves only idle quota within physical caps |
| Heat | Δ=1s; T_in defined by the business hot set; T_out=0.5T_in; H_cap=4× maximum T_in; grace≥2Δ, coldFor≥Δ |
| Statistics | Gate d=4, R=2, w determined by non-resident working-set replay; benefit sampling starts at p=1/1024, n_min=8 |
| Estimation time | Fixed observation window, prediction horizon W, maximum prior / model age; calibrated jointly with hot-key duration and TTL |
| Replacement | B_scan=64, m≤8, benefit threshold 0; fill entry / byte rates set by maintenance capacity |
| Lifetime | Hard TTL; jitter 0 by default; handoffTimeout significantly shorter than TTL; P, L, and cooldown targets settled together |

Configuration changes must not break existing budgets or state semantics. The first version allows only online changes with clear transition rules, such as sampling adjustment with probabilistic versioning; capacity reduction, statistical rebuild, and generation switches must have a completion state and cannot claim to take effect instantly.

Initialization must also verify: the sum of class budgets and reservations does not exceed M; a single object can pass the necessary response / pending / resident / retired paths; thresholds are representable by the counter bit width; the timing wheel covers the maximum TTL plus allowed lag and rounding margin; all update / scan / maintenance quotas have legal upper bounds. When this cannot be met, instance creation is refused instead of masking an unusable configuration by rejecting fills repeatedly in production.

### 8.3 Implementation modules and delivery boundaries

| Module | Input → output | Local validation required first |
|---|---|---|
| Index and reader protection | Full key / Entry → protected hit, conditional publish, removal by identity | Publication visibility, BUSY semantics, concurrent removal, cross-scope holding |
| Heat and cooldown | External hit / miss, time → admission eligibility, cooldown state | Bucket advancement, saturation, bounded failure, grace and independent full scan |
| Benefit and solve | Samples, cost table, candidate, Q, budget snapshot → optimal feasible plan or rejection | Three estimate states, event time, O/S non-overlap, comparison with independent exact solutions |
| Flight and response | Request, token, loader completion / cancellation → business result, unique candidate | Unique leader per identity, real I/O accounting, response delivery, and handoff terminal states |
| Commit and invalidation | Plan, control operation → readable Entry, invalidation confirmation or rejection | Reserve→re-validate→remove→publish order, expectedEntryId, generation races |
| Resources and maintenance | Quota credits, queues, time, reader exits → bounded progress and safe reclamation | No overselling, single settlement, class fairness, no lost wake-ups, slow-reader recovery |

Modules may be implemented and validated independently but must not redefine key identity, time source, byte accounting, or version tokens on their own. Shared state and result enums must be frozen before coding; the concrete hash table layout, atomic packing, and language containers are implementation choices that must satisfy the contracts here before performance is compared.

**Specification check entry point:** [A.10 Invariants](#ahkc-a10) maps the modules above to assertions that can be written directly into model / property tests.

## 9. Validation and delivery: prove correctness and boundedness first, then performance and policy benefit

**Acceptance completes along three lines at once: state machine invariants, algorithmic benefit, and full-service performance.** A pure-hit microbenchmark or the hit ratio on a single trace cannot show that the design is adequate.

### 9.1 One-to-one mapping between goals and tests

| Validation item | Method | Pass condition / output |
|---|---|---|
| TTL, invalidation, version | Interleaved query / publish / invalidate / old-load-complete / old-timer, with long maintenance pauses | No expired hit at the validation instant; new requests cannot read the old version after invalidation completes; old operations do not damage new entries |
| Hot-key discovery and cooldown | Exact window comparison; threshold edges, bucket boundaries, hot-set shifts, access stops | Report false / missed admissions and new-hot-key discovery time; the cooldown budget holds when P and L conditions hold |
| Local solve | Independent exhaustive / integer solver comparison on small sets, including multiple victims, negative loss, and combined retirement constraints | Enumeration, fast path, and the do-nothing option agree; same-key updates do not miss the old value |
| Estimation and finite search | Prediction versus actual future avoidable benefit; finite Q versus exact solutions on the full resident set at small scale | Report sampling / prior error, candidate-miss loss, and benefit improvement separately to avoid attribution confusion |
| Memory and concurrent reclamation | Slow readers, replacement and release intersections, index at capacity / collisions, retirement full at invalidation | No dangling access, ABA, or double release; all managed budgets always hold |
| Origin load and overload | Same-key bursts, ineffective cancellation, handoff gaps, fills full but responses reserved, slow consumers, interleaved shutdown | Same-identity merging works; real I/O and not-yet-transferred responses are all accounted; no unbounded restarts or waiter leaks |
| Conditional commit | Same-generation old candidate overwriting a new entryId, old identity already removed, namespace invalidation / TTL expiry mid-commit | Reject when conditions mismatch; when rollback is impossible guarantee the old generation is unreadable with bounded retention, reporting interrupted commits separately |
| Long-term progress | Scheduling rejection, recovery, zero-request cooldown, 72h steady state, traffic jitter | Every maintenance class progresses, queues / retention show no runaway trend, and normal admission resumes |

Concurrency-model and native-memory issues are supported during implementation by appropriate tools, for example [Loom](https://github.com/tokio-rs/loom), [Miri](https://github.com/rust-lang/miri), and [ThreadSanitizer](https://clang.llvm.org/docs/ThreadSanitizer.html); tool checks cannot replace state machine tests and real load.

### 9.2 Performance methodology and release requirements

- **Hot path:** zero heap allocation for warm borrowed hits; aim for a median cost no more than 1.5× the pure index baseline with the same key, TTL, reader protection, and return semantics. P50 < 150ns and P99 < 1μs are challenge targets for a specified working set.
- **Scalability:** measure 1 / 2 / 4 / 8 / 16 / 32 physical cores, reporting cross-NUMA separately; the 32-core challenge is 50M ops/s and must satisfy tail latency, maintenance progress, and resource constraints at the same time.
- **Stress matrix:** uniform, Zipf, single key at 90% / 99.9%, scan, and hot-set shifts; actual hit ratios of 100% / 99% / 95% / 50% / 0%; working sets spanning L2, LLC, and beyond LLC; mixed 64B / 1KiB / 16KiB plus large objects, short TTL, and concentrated expiry.
- **Full service:** open-loop replay at a fixed arrival rate counting queueing, rejection, real load duration, and maintenance CPU; normal traffic meets the host's P99 and sustainable throughput requirements, and abnormal traffic stays bounded and recoverable.

Every report fixes hardware, core count / SMT, frequency and NUMA, compiler optimization, allocator, sharding, and parameters. State whether key preparation, value access, and guard / handle release are timed; keep borrowed and owned separate, and microbenchmarks separate from service load tests. Repeat across rounds and keep raw data; never pass off inverse parallel throughput as single-request latency.

The final cost ledger is:

```text
net CPU savings / second
= avoided real load CPU / second
− lookup, heat, sampling, and handle CPU / second
− fill, maintenance, reclamation, and switching CPU / second
```

Selection decisions default to this net benefit plus service P99; for backend cost objectives, report CPU and downstream QPS separately. If set optimization costs more than it gains, adjust m, sampling, and fill budgets first before deciding whether to keep the policy; "locally optimal" never substitutes for benefit evidence.

### 9.3 State of the art and optimal references

Compare the complete design with SIEVE, S3-FIFO, and W-TinyLFU-AV under the same total memory, TTL, heat eligibility, return semantics, and load timing; also choose a native lib baseline matching the final language and record the cost of the semantics it must add. Run ablations on the gate, sparse estimation, set optimization, and idle quota transfer separately to explain where the improvement comes from.

Offline references follow model consistency: basic equal-size, equal-miss-cost experiments may use Belady/MIN; variable-size basic experiments may use the FOO/PFOO miss-count bound; complete small traces with TTL, heat eligibility, and load availability use the corresponding constrained integer model. If eligibility depends on resident state, the oracle must replay it too and must not fix the candidate trajectory produced by this design and then call it global OPT. [FOO/PFOO](https://arxiv.org/abs/1711.03709)

Report raw values for actual benefit, strong-baseline benefit, and the matching upper bound; quantify the impact of estimation error, finite search, and resource limits separately. A static benefit upper bound is not directly a dynamic multi-round policy bound, and a miss-count bound is not directly a net CPU benefit bound.

### 9.4 Delivery gates and required observability

| Stage | Evidence that must be delivered | Gate for the next step |
|---|---|---|
| Algorithm model | Heat / estimation / replacement state definitions, trace simulator, exact local solve comparison, and strong-baseline report | Policy benefit is explainable and the loss from sampling and finite Q is acceptable |
| Native prototype | Index and protection choices, resource ledger, concurrency tests, hot-path and maintenance cost report | Correctness and resource invariants pass and the agreed performance budget is met |
| Service validation | Real load replay, extreme overload and 72h reports, integration, and exit behavior | Full-service net benefit and P99 targets met; advantage over strong baselines or the trade-off is explicit |

Observability uses fixed stripes and low-rate detailed sampling and creates no per-key metrics. At minimum keep: real load and hit benefit, fill rejection reasons, sample drops / prior share, solve check count and gain, gate error, shard quota headroom, queue age, actual scan period, retired bytes / oldest reader, orphan load count, and invalidation completion latency. Alerts are derived from budgets and service deadlines.

## 10. Industry basis: adopt mature mechanisms, own the combined effect independently

| Research / engineering line | Conclusion adopted here | Applicable boundary |
|---|---|---|
| [TinyLFU](https://arxiv.org/abs/1512.00727) / [W-TinyLFU-AV](https://arxiv.org/abs/2105.08770) | Separate admission from residency policy; compare variable-size candidates against the set that frees space | This design uses time-based heat and unified cost-benefit, so the original experimental conclusions do not carry over |
| [SIEVE](https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo) | Hits do not move list nodes; the visited bit and scan hand lower policy maintenance cost | Only borrowed for candidate generation; concurrent performance is re-measured with added heat and the optimizer |
| [S3-FIFO](https://www.cs.cmu.edu/~rvinayak/papers/s3-fifo-sosp-2023-fifo-queues-are-all-you-need-for-cache-eviction.pdf) | The low-cost FIFO / fast-filter line is competitive and serves as a strong baseline | The first version does not layer its multi-queue and ghost on top, avoiding duplicate eligibility mechanisms |
| [Caffeine](https://github.com/ben-manes/caffeine/wiki/Design) / [Ristretto](https://github.com/dgraph-io/ristretto) | Decouple policy maintenance from requests and allow bounded buffers to abandon part of the policy work | TTL, invalidation confirmation, hard resource budgets, and native reclamation are guaranteed here separately |
| [CacheSack](https://www.usenix.org/conference/atc22/presentation/yang-tzu-wei) / [FOO](https://arxiv.org/abs/1711.03709) | Cost and capacity can be modeled explicitly; use optimal solutions or bounds to judge policy gaps | Research setups and constraints differ and are not performance promises for this lib |

The advance of this design is: under strict service semantics and bounded resources, it closes the loop between a low-cost hit path, benefit-aware admission, and exactly verifiable local decisions. Release conclusions must be supported by the evidence in section 9; passing an architecture review means the design is implementable and verifiable, not that performance targets have been measured.

<a id="ahkc-pseudocode"></a>

## Appendix A: Core data structures and algorithm pseudocode (implementation spec)

**This appendix together with the main text forms the spec, giving a complete reference path that can be translated step by step into engineering code.** The main text explains decisions and rationale, while this appendix fixes fields, call order, and failure handling. The algorithms are not production code; array layouts, SIMD, and atomic memory-order optimizations must be validated without changing semantics.

| Module | Entry and algorithms | Main text |
|---|---|---|
| Structures and base primitives | A.1–A.2: fields, budget credits, fixed-probe index | Sections 2, 6 |
| Lookup and heat | A.3: logical request, protected lookup, double bucket, gate | Sections 2–3 |
| Benefit estimation | A.4: event handling, quality states, cost-benefit computation | Sections 4.1–4.2 |
| Admission and replacement | A.5: candidate set, exact subset solve, conditional commit | Sections 4.3–4.5 |
| Load and delivery | A.6: Flight, completion mailbox, response, fill handoff | Section 5.2 |
| Invalidation and expiry | A.7: control commit, cooldown, timing wheel and recovery | Sections 3.3, 5.1, 5.3 |
| Reclamation and progress | A.8–A.9: epoch, destructor handoff, scheduling, quota transfer, shutdown | Sections 6–7 |

<a id="ahkc-a1"></a>

### A.1 Conventions, base primitives, and complete logical structures

Notation: `atomic<T>` is atomic state; `owner<T>` allows writes only by the designated maintainer; `immutable<T>` is immutable after publication; `Ref<T>` holds a bounded-pool object whose slot must not be reused before the last reference ends. All arrays, pools, iteration counts, and work budgets come from the initialization configuration. `NONE`, `BUSY`, `MISS`, and failure results must not be conflated.

Every `key` below is a full key including namespaceId; when prefixes such as `namespace`, `shard`, and `stats` are omitted, they resolve from the owning request / Entry / Flight. `e.namespace` is a read-only shorthand for namespaces[e.namespaceId]. Accessing same-named fields does not imply an extra dictionary or globally shared mutable state.

The correctness reference model uses sequentially consistent atomics; policy counters / visited explicitly marked as such may use weaker synchronization. Changing to memory orders such as acquire/release in engineering must be proven separately, and all atomics must not be mechanically replaced with relaxed. The base primitives below may be provided by mature libraries, and their contracts are part of the algorithm:

| Primitive | Contract that must hold |
|---|---|
| `TRY_GATE(g, R)` | At most R non-blocking attempts; on success holds a short critical section, otherwise BUSY; exit runs no user callback |
| `TRY_PUSH / TRY_POP` | Bounded queue; success holds only once the full message is visible; failure leaves no hole that would block the queue forever; internal publication must still follow the concurrency protocol |
| `TRY_POOL_REF` | Acquire an exclusive slot / reference from a fixed pool, failing when exhausted; a slot must not be reused while an old Ref exists |
| `Ref / ownedPins` | Reference counts must not wrap; check the configured cap before adding a hold and return BUSY when no new reference can be taken; existing holds can still be released |
| `MARK_READY(waiter)` | Only signals the host readiness mechanism and never runs business callbacks inline; wait registration and readiness checks must not lose wake-ups |
| `CAS_OWN_READY_VALUE` | Atomically match a phase with a result and transfer ownership; only the winner may dereference / move the result away, and terminal states never roll back |
| `TRY_SUBMIT(task)` | Runs at most once when accepted; runs nothing when rejected; caller-runs and inline recursive execution are forbidden |
| `HOST_LOAD / DISPOSE` | Runs only on host load / reclaim execution resources; holds no index, Flight, or maintenance critical section; result size is constrained by the contract |
| `CHECKED_NEXT(id)` | Generates unique identities monotonically; refuses acceptance at overflow and requires a rebuild, never wraps |

These primitives do not encapsulate heat, estimation, victim selection, or commit policy; all related algorithms are expanded later. Initial reference values: PIN_RETRIES, TOKEN_RETRIES, RESERVE_RETRIES, and FLIGHT_RETRIES are all 2, PUMP_STEPS_PER_RUN=4, NOTIFY_BATCH=32; all are validated as bounded configuration and must not be replaced with unbounded retries.

```text
Config:
    shardCount, namespaceLimit, maxKeyBytes
    C, B, M                             // entry count, additive resident billing, full managed budget
    shard[i].countMax, slotCount, probeLimit
    heatDelta, Tin[ns], Tout[ns], Hcap, heatRows, heatWidth, heatRetries
    grace, coldFor, valueDelta, valueRows, valueWidth, sampleP[ns], nMin
    maxSampleLag, horizonW, priorMaxAge, costMaxAge
    maxEntryBytes, maxResultBytes, maxConcurrentLoads, loadTimeout, handoffTimeout
    flightStripeCount, flightsPerStripe, maxWaitersPerKey, maxWaitersTotal
    readerSlotsPerShard, maxGuardsPerContext
    sieveScanLimit, victimLimit <= 8
    wheelTick, wheelSlots K, wheelLevels L, maxTTL, maxWheelLag
    perClassWorkLimits, fillRateLimit, periodicInterval
    fixedLimitsForAllByteBudgetsAndObjectPools

Cache:
    atomic state = OPEN                 // OPEN -> CLOSING -> RECLAIMED; stop-accepting is reported separately
    Namespace namespaces[namespaceLimit]
    Shard shards[shardCount]
    owner globalFreeCount, globalFreeBytes
    owner creditTransferState           // single coordinator; must not modify a running shard directly
    fixed namespaceControlQueue

Namespace:
    atomic gen
    immutable key/TTL/costClass rules
    atomic<Ref<ImmutableModel>> versionedPriorAndCostModel
    // bounded size, bounded age, fixed benefit unit; updaters publish atomically, old snapshots reclaimed after use

Shard:
    atomic ownerState = IDLE            // IDLE / QUEUED / RUNNING
    atomic pending, pauseReasons                 // WHEEL_REBASE / EXECUTOR_REJECTED / CLOSING bits
    atomic nextAllowedRunTime, taskSequence
    owner resumeClass
    owner countQuota, byteQuota, residentCount, residentBytes
    atomic fillFence
    owner entrySeq
    Index index
    HeatCell gate[heatRows][heatWidth]
    ValueStats valueStats
    fixed sampleStripes[], fillQueue, controlQueue
    owner policyList, hand, coldCursor   // hand is a queue position; coldCursor is a fixed index slot number
    TimerWheel wheel
    FlightStripe flights[flightStripeCount]
    EpochDomain epoch
    fixed retiredPool, pendingPool, deliveryPool, completionPool
    Budget pendingBytes, responseBytes, retiredBytes
    fixed cleanupJobPools               // every retention class has a settleable reclamation record
    owner workQuota, fillRateTokens

Index:
    atomic<EMPTY | TOMBSTONE | Entry*> slots[slotCount]
    immutable hashSeed, probeLimit      // first version uses fixed linear probing; published entries are never moved

Entry:
    immutable key, keyHash, entryId, namespaceId, namespaceGen
    immutable valueRef, hardDeadline, billedBytes, costClass, domainLifetimeRef
    atomic readable                     // PREPARED is invisible; true -> false after publication
    HeatCell heat
    atomic visited = false, ownedPins = 0
    owner graceUntil, lowSince = NONE, indexSlot
    owner policyPrev, policyNext
    owner timerEpoch, timerLevel, timerSlot, timerPrev, timerNext
    owner residencyReceipt              // billing responsibility of this entry; a transfer settles exactly once

HeatCell:
    atomic consistent {epoch, current, previous}

ValueCell:
    owner epoch, weightedCurrent, weightedPrevious, samplesCurrent, samplesPrevious

ValueStats:
    owner cells[valueRows][valueWidth], builtAt, lastFaultAt, modelVersion
    owner boundedCostTable, aggregateWatermark
    // each sample stripe also has an atomic faultFlag; dropped samples must not vanish silently because of a flag race

RequestContext:
    immutable cache, key, shard, namespace
    sampledOnce = false, heatClass = UNRECORDED   // HIT / MISS is chosen at most once

Token:
    namespaceGen, fillFence

FlightStripe:
    gate
    fixed Ref<Flight> slots[flightsPerStripe]

Flight:
    immutable key, namespaceId, shardId, stripeId, flightId, token, expectedEntryId
    immutable registeredAt, loadDeadline, validityBase
    phase = LOADING                     // protected by the stripe gate; includes the COMPLETING transition
    atomic sourcePending = true         // timeout must not clear this early
    Ref<Delivery> delivery = NONE
    Ref<FillCell> fillCell = NONE
    reuseDeadline = 0                   // protected by the stripe gate like phase
    fixed waiterSlots[maxWaitersPerKey]
    ioReceipt, responseReservation, recordReceipt
    OneShotMailbox sourceOutcome         // completion callbacks need not contend on the gate to store the result
    atomic pumpNeeded, completionOwner
    guardedByCompletionToken work = NONE         // fixed Work union; phase and ownership are recoverable

Waiter:
    atomic state = WAITING               // WAITING -> READY -> TAKEN, or CANCELLED
    Ref<Delivery> delivery = NONE
    slotReceipt, deadline

Delivery:
    immutable outcome                   // immutable business result or error; may also wrap a cache owned handle
    atomic internalRefs                 // holds by the Flight bridge and not-yet-transferred waiters
    responseReceipt, reservedCleanupJob
    atomic cleanupState

FillCell:
    atomic phase = QUEUED                // QUEUED / COMMITTING / terminal
    Ref<Flight> flight                   // may hold the identity record, never implicitly hold the value more than once
    // the terminal reason is published in the same phase value; never announce the terminal state before writing the reason

FillCandidate:
    immutable token, expectedEntryId, handoffDeadline
    Entry* preparedEntry                // unpublished; payload/TTL initialized, entryId filled in by the maintainer
    Ref<FillCell> cell
    pendingReceipt, reservedCleanupJob

Work:
    stage, afterNotify, capturedWaiterRefs[maxWaitersPerKey], notifyCursor
    optional inlineError                // bounded error code; delivered through a Delivery when absent
    reuseDeadline=0, immutable metadata, candidate ownership or late source ownership
    // at most one Work per Flight; each stage advances exactly once

CleanupJob:
    atomic state = RESERVED              // RESERVED -> READY -> SUBMITTING -> RUNNING -> DONE -> ACCOUNTED
    kind = PENDING_ENTRY | DELIVERY | LATE_SOURCE
    ownedPayload or Ref<Delivery>, Receipt, ownerPoolRefs
    // ownership is established before READY; this is not an unbounded callback closure

Control:
    immutable operationId, kind, namespaceId, optional completeKey
    atomic completion = PENDING         // PENDING -> SUCCESS / explicit failure reason
    recordReceipt, reservedCompletionSlot
    // the operation enters the queue only after operationId and the completion slot are reserved; target keys also obey maxKeyBytes

RetiredRecord:
    Entry* entry, entryId, retireEpoch, billedBytes
    retiredReceipt, reservedCleanupJob
    atomic state = WAIT_SAFE            // WAIT_SAFE -> SUBMITTING -> DISPOSING -> DONE -> ACCOUNTED

EpochDomain:
    atomic epoch
    ReaderSlot slots[readerSlotsPerShard] // INACTIVE or ACTIVE(epoch); active slots are never moved
    owner advanceScanCursor, scanEpoch, scanBlocked

TimerWheel:
    owner epoch, cursorTick, rebaseCursor, rebasing
    owner buckets[L][K]                 // intrusive lists; each bucket carries an epoch so old epochs can be cleared lazily
    owner resumableTickAndCascadeState

Budget:
    immutable limit
    atomic available                    // available + used + reserved = limit

Receipt:
    immutable budget, amount
    atomic settled = false              // returned once; transferring ownership is not returning
```

Fixed-pool storage is counted once in M_fixed; slot credits limit occupancy and do not invent a second physical memory. Dynamic retention such as values / keys is billed to its owning responsibility. Sharing across objects is conservatively billed by default. Scheduling, sampling, cost observation, and completion cells must also avoid unbounded per-key dictionaries.

<a id="ahkc-a2"></a>

### A.2 Budget credits and a conditionally reservable index

**Capacity quota may be reused based on the same maintainer's removal plan, but physical retention budget cannot pre-spend reclamation that has not completed.** Ordinary budget credits apply to class limits that already exist; shard residency is settled serially at commit.

```text
TRY_RESERVE(budget, amount):
    repeat at most RESERVE_RETRIES:
        old = budget.available.load()
        if old < amount: return FULL
        if CAS(budget.available, old, old - amount):
            return Receipt(budget, amount)        // the credit slot is reserved here, never allocated after success can still fail
    return BUSY

RETURN_ONCE(receipt):
    if CAS(receipt.settled, false, true):
        receipt.budget.available.fetch_add(receipt.amount)

TRY_BUNDLE(requirements):
    acquired = fixed small list
    for (budget, amount) in requirements:         // fixed item count, no per-key expansion
        r = TRY_RESERVE(budget, amount)
        if r failed:
            RETURN_ONCE(each acquired)
            return r.failure
        acquired.append(r)
    return acquired                              // handed to a single owner afterwards

PROBES(index, keyHash):
    start = MIX(index.hashSeed, keyHash) mod index.slotCount
    return [(start+j) mod slotCount for j in 0 .. probeLimit-1]

INDEX_FIND(index, key, guard):
    require guard is active before first pointer load
    for slot in PROBES(index, HASH(key)):
        p = index.slots[slot].load_acquire()
        if p == EMPTY: return ABSENT
        if p != TOMBSTONE and EXACT_KEY_EQUAL(p.key, key): return FOUND(p, slot)
    return ABSENT                                // absence is never inferred just because the budget ran out; see the proof below

INDEX_INSTALL_SLOT(index, key, removalSet):       // maintainer; does not actually remove
    for slot in PROBES(index, HASH(key)):
        p = index.slots[slot].load()
        if p in {EMPTY, TOMBSTONE} or p belongs to removalSet:
            return slot
    return NO_POSITION

INDEX_REMOVE(entry):                             // sole maintainer
    assert index.slots[entry.indexSlot] == entry
    entry.readable.store(false)
    index.slots[entry.indexSlot].store_release(TOMBSTONE)

INDEX_PUBLISH(entry, reservedSlot):
    assert index.slots[reservedSlot] in {EMPTY, TOMBSTONE}
    entry.indexSlot = reservedSlot
    entry.readable.store(true)
    index.slots[reservedSlot].store_release(entry) // single-entry publication visibility point
```

The reference index requires that **inserts also land only within the key's fixed probeLimit positions** and never move entries, so a full check of those positions can confirm ABSENT. It does not extend lookups indefinitely as tombstones accumulate; when no legal insert position exists the fill is rejected. Replacing it with an index that supports movement / growth requires implementing its own read protocol separately; when a full determination is impossible, return BUSY instead of copying the ABSENT conclusion above. probeLimit may start at 32 for testing, index slots are reserved by load factor, and rejections caused by collisions must be reported separately.

EMPTY is a position never written to; deletion only produces TOMBSTONE and must not be cleared to EMPTY, which would break the probe chain. Publication and removal are performed only by the maintainer; when a query overlaps a replacement it may observe the real gap during replacement. `INDEX_INSTALL_SLOT` never treats "memory that might be reclaimed later" as insert resource and reserves only the slot that this removal definitely frees.

<a id="ahkc-a3"></a>

### A.3 Logical request, hit path, and double-bucket heat

```text
BEGIN_REQUEST(cache, key):
    validate bounded key and namespace
    ctx = RequestContext(cache, key, HASH_TO_SHARD(key))
    p = current namespace sample probability
    if independent BERNOULLI(p):
        g = namespace.gen.load()
        sample = {namespaceId, policyHash(key,g), namespaceGen=g, eventTime=NOW(), p}
        stripe = fixed stripe selected by registered request context
        if not TRY_PUSH(stripe.queue, sample): stripe.faultFlag.store(true)
        else SIGNAL_MAINTENANCE(ctx.shard)
    ctx.sampledOnce = true
    return ctx

NORMALIZE_HEAT(snapshot, time, delta):
    e = floor(time / delta)
    if snapshot.epoch > e: return UNKNOWN         // request resumed after a pause must not roll buckets back
    if e == snapshot.epoch: return snapshot
    if e == snapshot.epoch + 1:
        return {e, current=0, previous=snapshot.current}
    return {e, current=0, previous=0}

HEAT_ADD(cell, time):
    repeat at most heatRetries:
        old = cell.load_consistent()
        x = NORMALIZE_HEAT(old, time, heatDelta)
        if x == UNKNOWN: return SKIPPED
        if x.epoch == old.epoch and x.current == Hcap: return SATURATED
        x.current = min(x.current + 1, Hcap)
        if CAS_CONSISTENT(cell, old, x): return UPDATED
    return SKIPPED

HEAT_READ(cell, time):
    x = NORMALIZE_HEAT(cell.load_consistent(), time, heatDelta)
    if x == UNKNOWN: return UNKNOWN
    phi = (time mod heatDelta) / heatDelta
    return x.current + (1-phi) * x.previous

GATE_ADD(shard, key, time):
    for row in 0 .. heatRows-1:
        HEAT_ADD(shard.gate[row][ROW_HASH(row,key) mod heatWidth], time)

GATE_READ(shard, key, time):
    hs = [HEAT_READ(selected row cell,time) for each fixed row]
    if any UNKNOWN: return UNKNOWN
    return min(hs)

RECORD_HEAT_ONCE(ctx, result, entry, time):
    if ctx.heatClass != UNRECORDED: return
    ctx.heatClass = result
    if result == HIT: HEAT_ADD(entry.heat, time)
    else if result == MISS: GATE_ADD(ctx.shard, ctx.key, time)

READ_INTERNAL(cache, shard, key, returnMode):      // internal calls do not automatically count as a logical request
    if cache.state != OPEN: return CLOSED
    guard = PIN(shard.epoch)                      // A.8; protect before loading pointers
    if guard failed: return BUSY
    found = INDEX_FIND(shard.index, key, guard)
    t = NOW()
    if found == ABSENT:
        UNPIN(guard); return MISS(time=t, expected=NONE)
    e = found.entry
    if not e.readable or e.namespaceGen != namespace.gen or t >= e.hardDeadline:
        id = e.entryId
        UNPIN(guard); return MISS(time=t, expected=id)
    if returnMode == BORROW:
        return HIT(BorrowView(e, guard), time=t)   // the guard lives as long as the view
    if bounded non-overflowing increment of e.ownedPins fails:
        UNPIN(guard); return BUSY
    handle = CacheHandle(e)                      // the Entry itself holds the domain lifetime; no global refcount per hit
    UNPIN(guard)
    return HIT(handle, time=t)

GET(ctx, mode):
    r = READ_INTERNAL(ctx.cache, ctx.shard, ctx.key, mode)
    if r is HIT:
        RECORD_HEAT_ONCE(ctx, HIT, r.entry, r.time)
        if not r.entry.visited.load_relaxed(): r.entry.visited.store_relaxed(true)
    else if r is MISS:
        RECORD_HEAT_ONCE(ctx, MISS, NONE, r.time)
    return r                                     // BUSY is never counted as a definite miss
```

An owned HIT is still protected by the handle after leaving the guard, which allows bounded subsequent heat updates; if the entry was already unlinked, the statistics only affect the old Entry and never change readability. A borrowed guard must not cross an await. A sample's event time is the entry time; only sampled requests take that time extra, while TTL validation still uses the current time at lookup.

<a id="ahkc-a4"></a>

### A.4 Sample processing and benefit computation

**MEASURED is an engineering state meaning "estimated from available samples", not a proof of zero error.** Sample transport lag is also measured; estimation uses the statistics snapshot frozen in the same maintenance round and does not imply that all concurrent requests are already counted.

```text
DRAIN_SAMPLES(shard, quota):
    for each fixed sample stripe:                 // scan cost is charged to the sample quota
        if stripe.faultFlag.exchange(false): stats.lastFaultAt = NOW()
    while quota permits one complete sample update:
        s = TRY_POP_NEXT_SAMPLE_ROUND_ROBIN()
        if none: break
        t = NOW(); epoch = floor(t / valueDelta)
        if s.eventTime > t or floor(s.eventTime/valueDelta) < epoch-1:
            stats.lastFaultAt = t; continue
        if s.namespaceGen != current namespace.gen: continue
        weight = 1 / s.p
        if not finite(weight) or s.p <= 0 or s.p > 1:
            stats.lastFaultAt = t; continue
        for each fixed value row:
            c = stats.cells[row][ROW_HASH(row,s.policyHash) mod valueWidth]
            NORMALIZE_VALUE_CELL_IN_OWNER(c, epoch) // same bucket rotation as the double bucket, counts do not saturate
            bucket = current if event epoch == epoch else previous
            c[bucket].weighted += weight
            c[bucket].samples += 1
            if numeric overflow: stats.lastFaultAt = t
    update bounded queue age / consumed-prefix watermark

ESTIMATE(item, snapshotTime, statsSnapshot):
    if snapshotTime >= item.hardDeadline: return EXPIRED
    if any unconsumed fault flag or any sample prefix lag > maxSampleLag:
        return UNAVAILABLE
    if snapshotTime < stats.lastFaultAt + 2*valueDelta: return UNAVAILABLE
    model = statsSnapshot.model         // immutable Ref frozen for this decision, shared by all candidates
    if model expired, incompatible, or cost class outside calibrated support:
        return UNAVAILABLE
    selectedCells = rows selected by policyHash(item.key,item.namespaceGen)
    weighted = min(DOUBLE_BUCKET_VALUE(c.weighted, snapshotTime) over selectedCells)
    samples  = min(DOUBLE_BUCKET_VALUE(c.samples, snapshotTime) over selectedCells)
    warm = snapshotTime >= stats.builtAt + 2*valueDelta
    if warm and samples >= nMin:
        lambda = weighted / valueDelta; mode = MEASURED
    else if prior exists and its age <= priorMaxAge:
        lambda = prior.lambda[namespace,costClass]; mode = PRIOR
    else: return UNAVAILABLE
    (r,c,h,k,K) = LOOKUP_COST_MODEL(model, lambda, item.costClass)
    if any input is non-finite or negative: return UNAVAILABLE
    duration = min(horizonW, item.hardDeadline-snapshotTime)
    G = (r*c-h) * duration
    return {G, k, removalLoss=G+k, installCost=K, mode, snapshotTime, model.version}
```

`NORMALIZE_VALUE_CELL_IN_OWNER` rotates buckets like `NORMALIZE_HEAT` but maintains a weighted sum and a sample count separately, with no Hcap. `LOOKUP_COST_MODEL` looks up within bounded request-rate / size / load-duration classes, may interpolate a fixed number of times between grid points, returns UNAVAILABLE out of range, and performs no online inference. r comes from a calibration replay that keeps singleflight; the CPU cost in c must not be replaced by loader wall-clock duration. h, k, K come from a fixed-version benchmark; online cost observation is collected only on the completion path and aggregated per fixed class by the maintainer with age updates. The first version runs without any online training.

<a id="ahkc-a5"></a>

### A.5 Candidate generation, exact replacement, and commit

**The solver receives the real executable action set; it must not solve for capacity first and then miss index slots, retired object counts, or commit work.** All computations below run inside the shard's sole maintainer.

```text
COLLECT_Q(shard, oldEntry, t, lifeQuota):
    Q = fixed array[victimLimit]; anchor = NONE
    limit = min(sieveScanLimit, policyList.countAtStart)
    repeat at most limit times:
        if policyList empty: break
        e = hand if hand exists else policyList.tail
        if e == anchor: break                    // do not loop again just because the queue got shorter after removals
        hand = NEXT_TOWARD_HEAD_OR_WRAP(e)
        if not e.readable or t >= e.hardDeadline or e.namespaceGen != namespace.gen:
            e.readable.store(false)
            removed = TRY_RETIRE(e, lifeQuota)    // A.7–A.8; without budget it stays in the index and keeps being accounted
            if not removed and anchor == NONE: anchor = e
            continue
        if anchor == NONE: anchor = e            // this anchor is not removed again in this round
        if e == oldEntry: continue
        if e.visited.exchange(false): continue
        Q.append(e)
        if Q.size == victimLimit: break
    return Q

MATCH_EXPECTED(candidate, current):
    if candidate.expectedEntryId == NONE: return current == ABSENT
    return current is FOUND and current.entryId == candidate.expectedEntryId

FREEZE_INPUT(a, O, Q, shard):
    t = NOW()
    if cache.state != OPEN or a.token != CURRENT_TOKEN(shard,a.namespace): return STALE
    if t >= min(a.entry.hardDeadline,a.handoffDeadline): return EXPIRED
    if GATE_READ(shard,a.key,t) is UNKNOWN or < Tin[a.namespace]: return NOT_HOT
    ss = freeze stats view and one Ref to namespace.versionedPriorAndCostModel
    av = ESTIMATE(a.entry,t,ss)
    if av unavailable: return NO_ESTIMATE
    oldLoss = 0
    for o in O:
        ov = {G=0,k=ss.model.removeCost(o)} if o invalid/expired else ESTIMATE(o,t,ss)
        if ov unavailable: return NO_ESTIMATE
        oldLoss += ov.G + ov.k
    for v in Q:
        if v invalid/expired: exclude v from Q    // no additional search is appended; the actual Q shrinks
        else:
            vv = ESTIMATE(v,t,ss)
            if vv unavailable: return NO_ESTIMATE
            save immutable {entryId,billedBytes,loss=vv.G+vv.k} for v
    freeze N=residentCount, Bytes=residentBytes, countQuota, byteQuota
    freeze retired byte availability, record/job slots, removal-work availability
    probeSlots = PROBES(index,a.keyHash)
    installWithoutQ = any probe slot is EMPTY/TOMBSTONE or contains member of O
    installMask = bitmask of Q members whose current indexSlot is in probeSlots
    return Snapshot(t,a,O,Q,av,oldLoss,all frozen resources)

FEASIBLE(snapshot, mask):
    R = O union Q[mask]                           // O and Q are disjoint
    postN = N + 1 - size(R)
    postB = Bytes + a.billedBytes - SUM(billedBytes of R)
    if postN > countQuota or postB > byteQuota: return false
    if postN > index.countMax: return false
    if SUM(billedBytes of R) > retiredBytesAvailable: return false
    if size(R) > min(retiredRecordSlots, retiredJobSlots, removalWorkLeft): return false
    if not installWithoutQ and (mask & installMask) == 0: return false
    if wheel is rebasing or candidate deadline outside wheel schedulable range: return false
    return true                                  // the candidate's full pending responsibility was guaranteed before enqueue

EXACT_SELECT(snapshot):
    m = Q.size
    if m == 0: return 0 if FEASIBLE(snapshot,0) else INFEASIBLE
    if every Q.loss >= 0 and FEASIBLE(snapshot,0): return mask=0
    if all Q.b equal and all Q.loss >= 0 and installWithoutQ:
        k = max(0, N+1-size(O)-countQuota,
                   ceil(max(0,Bytes+a.b-SUM(O.b)-byteQuota)/Q[0].b))
        if k <= m:
            mask = stable k smallest losses
            if FEASIBLE(snapshot,mask): return mask
        // all same-size, same-count combinations have identical resource requirements; no solution means reject
        return INFEASIBLE
    best = NONE
    sums[0] = {count=0, bytes=0, loss=0}           // at most 256 entries, fixed scratch
    for mask in 0 .. (1<<m)-1:
        if subsetWorkQuota exhausted: return WORK_LIMIT
        if mask != 0:
            bit = LOWEST_SET_BIT(mask); prev = mask without bit
            v = Q[INDEX(bit)]
            sums[mask] = {count=sums[prev].count+1,
                          bytes=sums[prev].bytes+v.billedBytes,
                          loss=sums[prev].loss+v.loss}
        if FEASIBLE(snapshot,mask):
            rank = (sums[mask].loss, sums[mask].bytes, sums[mask].count, stableIds(mask))
            if best == NONE or rank < best.rank: best = (mask,rank)
    return best.mask if best exists else INFEASIBLE

PLAN_FILL(a, shard):
    if a.cell.phase != QUEUED: return ALREADY_TERMINAL
    current = INDEX_FIND_IN_OWNER(index,a.key)
    if not MATCH_EXPECTED(a,current): return STALE
    O = {current.entry} if current is FOUND else {}
    if O-only replacement fits local count/bytes, retired resources and index position:
        Q = []                                   // the idle fast path explicitly chooses an empty Q, no pointless scan
    else: Q = COLLECT_Q(shard,O.singleOrNone,NOW(),lifeQuota)
    // COLLECT_Q may perform lifetime cleanup, so re-check old identity and occupancy after it
    if not MATCH_EXPECTED(a,INDEX_FIND_IN_OWNER(index,a.key)): return STALE
    x = FREEZE_INPUT(a,O,Q,shard)
    if x failed: return x.reason
    selected = EXACT_SELECT(x)
    if selected failed: return selected.reason
    gain = x.av.G - x.av.installCost - x.oldLoss - SUM(Q[selected].loss)
    if gain <= 0: return NON_POSITIVE_GAIN
    return Plan(x, removals=O union Q[selected], gain)

PREPARE_COMMIT(a,plan):
    require shard owner retained until commit or rejection completes
    if identities, countQuota, byteQuota or required work resources no longer match:
        return STALE_PLAN
    lease = TRY_BUNDLE(retired bytes, retired record slots, retired cleanup job slots)
    if lease failed: return RESOURCE_LIMIT
    slot = INDEX_INSTALL_SLOT(index,a.key,plan.removals)
    if slot == NO_POSITION: RETURN_ONCE(lease); return INDEX_LIMIT
    a.entry.entryId = (shard.id, CHECKED_NEXT(shard.entrySeq))
    return Prepared(plan,lease,slot)              // no ordinary allocation is needed any more; planned freed quota is not donated early

COMMIT_FILL(a,prepared):
    t = NOW()
    if cache.state != OPEN or a.token != CURRENT_TOKEN(shard,a.namespace): reject STALE
    if not MATCH_EXPECTED(a,INDEX_FIND_IN_OWNER(index,a.key)): reject STALE
    if t >= min(a.entry.hardDeadline,a.handoffDeadline): reject EXPIRED
    if any planned entry identity/resource differs: reject STALE_PLAN
    if not CAS(a.cell.phase,QUEUED,COMMITTING): reject ALREADY_TERMINAL
    // after this point ordinary timeouts may no longer change the fill terminal state; no user callbacks, no fallible allocation
    for e in prepared.removals:
        INDEX_REMOVE(e)
        UNLINK_POLICY_AND_TIMER_FIX_HAND(e)
        residentCount -= 1; residentBytes -= e.billedBytes
        TRANSFER_RESIDENT_TO_RETIRED(e, prepared.preReservedReceiptFor(e))
        REGISTER_RETIRED(e, epoch.current)        // the record was already reserved by prepared
    if cache.state != OPEN or a.token != CURRENT_TOKEN(shard,a.namespace)
       or NOW() >= a.entry.hardDeadline:
        DISPOSE_PENDING_USING_RESERVED_JOB(a)    // an unpublished object keeps counting as pending until truly released
        FINISH_FILL_AS_OWNER(a.cell,STALE_OR_EXPIRED)
        RELEASE_UNUSED_PREPARED_RESOURCES()
        return
    e = MOVE(a.preparedEntry)
    residentCount += 1; residentBytes += e.billedBytes
    TRANSFER_PENDING_TO_RESIDENT_RECEIPT(a,e)     // the new ledger is established first, then the old pending credit is returned
    e.graceUntil = NOW()+grace; e.lowSince = NONE
    LINK_POLICY_AT_HEAD(e); WHEEL_ADD(e)
    INDEX_PUBLISH(e,prepared.slot)
    // namespace invalidation / time passing may still interleave with publication; the read check is the final guarantee
    if e.namespaceGen != namespace.gen or NOW() >= e.hardDeadline or cache.state != OPEN:
        e.readable.store(false)                  // keep resident billing; retire later when budget allows
        outcome = STALE_OR_EXPIRED
    else: outcome = PUBLISHED
    FINISH_FILL_AS_OWNER(a.cell,outcome)
    RELEASE_UNUSED_PREPARED_RESOURCES()
    SIGNAL_FLIGHT_PUMP(a.cell.flight)
    return
  reject reason:
    RELEASE_UNUSED_PREPARED_RESOURCES()
    REJECT_QUEUED_IF_STILL_OWNER(a,reason)        // the resident set was not modified

PROCESS_FILL(a):
    if a.cell.phase is terminal:
        DISPOSE_PENDING_USING_RESERVED_JOB(a); return
    plan = PLAN_FILL(a,shard)
    if plan failed: REJECT_QUEUED_IF_STILL_OWNER(a,plan.reason); return
    prepared = PREPARE_COMMIT(a,plan)
    if prepared failed: REJECT_QUEUED_IF_STILL_OWNER(a,prepared.reason); return
    COMMIT_FILL(a,prepared)

CANCEL_FILL_CELL(cell,reason):                    // a timeout / shutdown party only changes the phase
    CAS(cell.phase,QUEUED,TERMINAL(reason))
    SIGNAL_FLIGHT_PUMP(cell.flight)               // must not release a payload the queue consumer is still using

REJECT_QUEUED_IF_STILL_OWNER(a,reason):
    CAS(a.cell.phase,QUEUED,TERMINAL(reason))      // keep the original terminal state if already cancelled
    assert a.cell.phase is terminal              // the current exclusive consumer never handed over to another committer
    DISPOSE_PENDING_USING_RESERVED_JOB(a)
    SIGNAL_FLIGHT_PUMP(a.cell.flight)

REJECT_UNENQUEUED_CANDIDATE(a,reason):
    REJECT_QUEUED_IF_STILL_OWNER(a,reason)         // a failed enqueue never transferred candidate ownership

FINISH_FILL_AS_OWNER(cell,reason):
    assert CAS(cell.phase,COMMITTING,TERMINAL(reason)) succeeds
    SIGNAL_FLIGHT_PUMP(cell.flight)
```

`INDEX_FIND_IN_OWNER` uses the same probe rule; the maintainer owns the structures and needs no borrowed guard. `UNLINK_POLICY_AND_TIMER_FIX_HAND` is a fixed-count list re-link: fix head/tail, take the successor before deleting the hand, set hand to NONE after deleting the only element, and revoke the current timer; coldCursor is an index slot number and does not move on removal. `TRANSFER_*` only moves existing retention responsibility; actual space release happens only after the reclamation in A.8 completes. The `a.entry/key/namespace` and `Q.b` in the pseudocode are read-only shorthands for the corresponding structure fields, not another undefined state.

The equal-size fast path additionally relies on the combined resources listed in this spec being determined only by count / bytes and the already-resolved index position; adding tenant limits, different retirement classes, or other combined constraints requires disabling this fast path or re-proving it. When m=0 the empty set is evaluated separately and Q[0] is never touched. Exceeding the work quota mid-enumeration rejects the whole fill; the current tentative best must not be committed.

<a id="ahkc-a6"></a>

### A.6 Load merging, response delivery, and fill handshake

**The Flight uses a short critical section to manage registration, and real completion is handed over through a pre-allocated one-shot completion mailbox.** A completion callback that temporarily cannot get the Flight gate still cannot lose the result or leak I/O quota. COMPLETING is an explicit transition state: existing waiters can complete, new requests briefly get BUSY, and completed_pending reuse is allowed only after the initial screen passes and the enqueue succeeds.

```text
READ_STABLE_TOKEN_AND_EXPECTED(ctx):
    repeat at most TOKEN_RETRIES:
        t1 = {namespace.gen.load(), shard.fillFence.load()}
        guard = PIN(shard.epoch)
        if failed: return BUSY
        x = INDEX_FIND(index,ctx.key,guard)
        expected = x.entryId if FOUND else NONE
        UNPIN(guard)
        t2 = {namespace.gen.load(), shard.fillFence.load()}
        if t1 == t2: return (t1,expected)
    return BUSY

GET_OR_LOAD(ctx):
    hit = GET(ctx,OWNED)
    if hit != MISS: return hit
    (token,expected) = READ_STABLE_TOKEN_AND_EXPECTED(ctx)
    if failed: return BUSY
    stripe = FLIGHT_STRIPE(ctx.key)
    if TRY_GATE(stripe.gate,FLIGHT_RETRIES) failed: return BUSY
    f = find matching (complete key,token) in fixed stripe slots
    if f exists:
        if f.phase == LOADING:
            w = TRY_ATTACH_WAITER(f)              // also reserves per-key / total waiter slots
        else if f.phase == COMPLETED_PENDING and f.fillCell not terminal
                and token == CURRENT_TOKEN() and NOW() < f.reuseDeadline:
            w = TRY_ATTACH_WAITER(f)
            if success: BIND_READY(w,f.delivery)
        else:
            unlock; SIGNAL_FLIGHT_PUMP(f); return BUSY
        unlock; return Future(w) if attached else OVERLOADED
    bundle = TRY_BUNDLE(flight record/pool slot, I/O permit,
                        maxResultBytes response reservation + delivery cleanup record,
                        first waiter slot)
    if failed: unlock; return OVERLOADED
    f = initialize Flight(token,expected,unique flightId,bundle)
    f.registeredAt = NOW(); f.loadDeadline = f.registeredAt + loadTimeout
    f.validityBase = f.registeredAt               // earlier than the actual call, serving as a conservative TTL basis
    register f before unlocking                   // later requests of the same identity can only join
    w = f.firstWaiter
    unlock
    r = READ_INTERNAL(cache,shard,key,OWNED)       // second check, no extra external counting
    if r == HIT: FINISH_SOURCE(f,CACHED(r.handle)); return Future(w)
    if r in {BUSY,CLOSED} or token != CURRENT_TOKEN() or NOW() >= f.loadDeadline:
        FINISH_SOURCE(f,ERROR(RETRY_OR_CLOSED)); return Future(w)
    HOST_LOAD(key, completion=(outcome => FINISH_SOURCE(f,LOADED(outcome))))
    return Future(w)

TRY_ATTACH_WAITER(f):                            // stripe gate already held
    drop registry Refs of TAKEN/CANCELLED waiters within fixed waiterSlots
    i = first empty slot in f.waiterSlots
    if none: return OVERLOADED
    bundle = TRY_BUNDLE(global waiter permit, waiter pool slot)
    if failed: return OVERLOADED
    w = initialize Waiter(WAITING,bundle,deadline=logical request deadline)
    f.waiterSlots[i] = Ref(w)
    return w                                    // an old pool slot cannot be reused while a previous Work snapshot still holds a Ref

FINISH_SOURCE(f,outcome):                         // real I/O completed, or the leader confirmed no I/O was started
    attach library NOW() as source completion time; retain immutable kind/metadata
    if not f.sourceOutcome.COMPLETE_ONCE(outcome): return DUPLICATE_COMPLETION
    RETURN_ONCE(f.ioReceipt)                      // the timeout path must not perform this return
    f.sourcePending.store(false)
    SIGNAL_FLIGHT_PUMP(f)
    TRY_PUMP_FLIGHT(f)                            // on failure the dedicated completion retry / periodic trigger continues

TRY_PUMP_FLIGHT(f):
    if CAS(f.completionOwner,false,true) failed: return
    f.pumpNeeded.exchange(false)                 // claim existing notifications; new notifications during execution are kept
    repeat at most PUMP_STEPS_PER_RUN:
        if f.work == NONE:
            if TRY_GATE(f.stripe.gate,FLIGHT_RETRIES) failed: break
            PICK_FLIGHT_WORK_UNDER_GATE(f)        // defined below; creates at most one fixed work record
            unlock
            if f.work == NONE: break
        w = f.work
        if w.stage == NOTIFY:
            for next at most NOTIFY_BATCH registered waiters:
                if w.inlineError exists: READY_INLINE_ERROR(waiter,w.inlineError)
                else: BIND_READY(waiter,f.delivery)
            if all captured waiters processed: w.stage=w.afterNotify
        else if w.stage == CHECK_LATE_SOURCE:
            WORK_CHECK_LATE_SOURCE_STEP(f,w)
        else if w.stage == PREPARE_FILL:
            w.candidate = TRY_PREPARE_CANDIDATE(f,f.delivery,w.metadata,w.reuseDeadline)
            w.stage = SET_TERMINAL if candidate failed else ATTACH_CELL
        else if w.stage == ATTACH_CELL:
            if TRY_GATE(f.stripe.gate,FLIGHT_RETRIES) failed: break
            f.fillCell = w.candidate.cell         // the completion relation is established before the enqueue
            unlock
            w.stage=ENQUEUE
        else if w.stage == ENQUEUE:
            if not TRY_PUSH(shard.fillQueue,w.candidate):
                REJECT_UNENQUEUED_CANDIDATE(w.candidate,QUEUE_FULL)
            else:
                MOVE candidate ownership to queue; SIGNAL_MAINTENANCE(shard)
            w.stage=SET_PENDING_OR_TERMINAL       // never overwrites the terminal state even if the maintainer finished instantly
        else if w.stage in {SET_TERMINAL,SET_PENDING_OR_TERMINAL}:
            if TRY_GATE(f.stripe.gate,FLIGHT_RETRIES) failed: break
            terminal = w.stage==SET_TERMINAL or f.fillCell==NONE or f.fillCell terminal
            f.phase = TERMINAL if terminal else COMPLETED_PENDING
            f.reuseDeadline=w.reuseDeadline
            if terminal: CLEAR_BRIDGE_REF(f)
            f.work=NONE
            REMOVE_FLIGHT_IF_SETTLED_UNDER_GATE(f)
            unlock
        else if w.stage == DISPOSE_LATE:
            MOVE late source outcome and response receipt to preReserved cleanup job
            mark job READY; w.stage=SET_TERMINAL
    needRetry = f.work != NONE or atomic mailbox/fill flags need handling
    if needRetry: f.pumpNeeded=true              // read Work only while completionOwner is still held
    f.completionOwner=false
    // after releasing, do not read Work / phase that another executor may modify; the fixed periodic scan compensates concurrent signals
    if f.pumpNeeded: schedule bounded completion retry, or wait periodic tick

PICK_FLIGHT_WORK_UNDER_GATE(f):
    require f.work==NONE
    now=NOW()
    if f.phase==LOADING and (now>=f.loadDeadline or cache.state!=OPEN):
        f.phase=TIMED_OUT_IO_RUNNING
        f.work=Work(NOTIFY, inlineError=TIMEOUT_OR_CLOSED,
                    capturedWaiters=fixed snapshot, afterNotify=CHECK_LATE_SOURCE)
        // CHECK_LATE_SOURCE: if the source is still unfinished, clear work but keep the orphan state;
        // if finished, take the mailbox once and move to DISPOSE_LATE, releasing the empty response reservation when there is no payload.
        return
    if f.sourceOutcome has unconsumed result:
        source=TAKE_SOURCE_ONCE(f.sourceOutcome)
        if f.phase==TIMED_OUT_IO_RUNNING:
            f.work=Work(DISPOSE_LATE,source); return
        if source is ERROR without retained payload:
            RELEASE_EMPTY_RESPONSE_RESERVATION(f)
            f.phase=COMPLETING                    // no new waiter may join after the snapshot, avoiding missed notifications
            f.work=Work(NOTIFY,inlineError=source.error,
                        capturedWaiters=fixed snapshot,afterNotify=SET_TERMINAL)
            return
        sourceKind=source.kind                   // save the scalar discriminant before MOVE; never touch the moved source again
        f.delivery=BUILD_DELIVERY_IN_RESERVED_SLOT(MOVE(source),MOVE(f.responseReservation))
        f.phase=COMPLETING                        // new joins briefly get BUSY, existing waiters are delivered first
        if sourceKind in {CACHED,ERROR_WITH_PAYLOAD}:
            f.work=Work(NOTIFY,capturedWaiters=fixed snapshot,
                        afterNotify=SET_TERMINAL,reuseDeadline=0)
            return
        metadata=f.delivery.outcome.cacheMetadata // bounded read-only fields, no additional payload retention
        deadline=min(CALCULATE_HARD_DEADLINE(f,metadata),
                     metadata.realCompletedAt+handoffTimeout)
        f.work=Work(NOTIFY,capturedWaiters=fixed snapshot,
                    afterNotify=PREPARE_FILL,metadata,reuseDeadline=deadline)
        return
    if f.phase==COMPLETED_PENDING:
        if f.fillCell terminal:
            f.phase=TERMINAL; CLEAR_BRIDGE_REF(f)
        else if now>=f.reuseDeadline or f.token!=CURRENT_TOKEN() or cache.state!=OPEN:
            CANCEL_FILL_CELL(f.fillCell,EXPIRED_OR_STALE)
            CLEAR_BRIDGE_REF(f)
            if f.fillCell terminal: f.phase=TERMINAL
            // COMMITTING is never terminated by a timeout party; the record stays and same-identity joins return BUSY
    REMOVE_FLIGHT_IF_SETTLED_UNDER_GATE(f)

WORK_CHECK_LATE_SOURCE_STEP(f,w):
    if f.sourcePending:
        f.work=NONE                              // wait for the real completion mailbox; I/O/response responsibility is still held
    else:
        source=TAKE_SOURCE_ONCE(f.sourceOutcome)
        if source has payload: w.source=source; w.stage=DISPOSE_LATE
        else: RELEASE_EMPTY_RESPONSE_RESERVATION(f); w.stage=SET_TERMINAL

REMOVE_FLIGHT_IF_SETTLED_UNDER_GATE(f):
    if f.phase!=TERMINAL or f.sourcePending or f.work!=NONE: return
    if f.fillCell exists and not terminal: return
    CLEAR_BRIDGE_REF(f)
    f.fillCell=NONE                               // actively break the Flight <-> FillCell reference cycle
    remove stripe registry slot only if it still names this flightId
    // ready waiters / callback Refs may still hold the Flight; the pool slot and record quota are returned after the last Ref ends

CLEAR_BRIDGE_REF(f):
    d=MOVE(f.delivery)
    if d exists: DROP_INTERNAL_REF(d)             // state update only, no payload destruction

TRY_PREPARE_CANDIDATE(f,d,metadata,deadline):
    if cache.state != OPEN or f.token != CURRENT_TOKEN(): return STALE
    if NOW() >= deadline or not metadata.cacheable: return INELIGIBLE
    if GATE_READ(shard,key,NOW()) is UNKNOWN or < Tin[namespace]: return NOT_HOT
    b = BOUNDED_MEASURE_FULL_ENTRY(key,d.outcome.value) // outside the gate; includes key, capacity, metadata
    if b > maxEntryBytes or measure invalid: return TOO_LARGE
    bundle = TRY_BUNDLE(pending bytes=b, candidate/pool slot,
                        pending cleanup job, FillCell/pool slot)
    if failed: return RESOURCE_LIMIT
    e = TRY_ALLOCATE_UNPUBLISHED_ENTRY_WITHIN_RESERVATION()
    if allocation failed:
        dispose partial allocation through reserved job; return ALLOCATION_FAILED
    e.key=bounded key copy; e.keyHash=HASH(key)
    e.namespaceId=f.namespaceId; e.namespaceGen=f.token.namespaceGen
    e.entryId=UNASSIGNED; e.valueRef=immutable shared reference to d.outcome.value
    e.hardDeadline=CALCULATE_HARD_DEADLINE(f,metadata); e.billedBytes=b
    e.costClass=metadata.validatedCostClass; e.domainLifetimeRef=shard lifetime reference
    e.readable=false; e.visited=false; e.ownedPins=0
    e.heat={epoch=floor(NOW()/heatDelta),current=0,previous=0}
    initialize all policy/timer links to NONE
    cell = FillCell(QUEUED,Ref(f))
    return FillCandidate(e,cell,f.token,f.expectedEntryId,deadline,bundle)

BIND_READY(w,d):
    acquire one internal delivery Ref
    if CAS(w.state,WAITING,READY(d)) fails: DROP_INTERNAL_REF(d); return
    MARK_READY(w)                                 // the READY transition hands this Ref to the waiter

READY_INLINE_ERROR(w,error):
    if CAS(w.state,WAITING,READY_INLINE(bounded error code)) succeeds:
        MARK_READY(w)

TAKE_RESULT(w):
    if CAS_OWN_READY_VALUE(w.state,READY_INLINE(error),TAKEN) succeeds:
        RETURN_ONCE(w.slotReceipt); return ERROR(error)
    if CAS_OWN_READY_VALUE(w.state,READY(d),TAKEN) fails: return NOT_READY_OR_CANCELLED
    result = TRANSFER_OR_CLONE_IMMUTABLE_VALUE_TO_HOST(d.outcome)
    DROP_INTERNAL_REF(d)
    RETURN_ONCE(w.slotReceipt)
    return result

CANCEL_WAITER(w):
    old = ATOMIC_TRANSITION_TO_CANCELLED_IF_WAITING_OR_READY(w.state)
    if old was READY(d): DROP_INTERNAL_REF(d)
    if old was WAITING or READY or READY_INLINE: RETURN_ONCE(w.slotReceipt)
    // TAKEN is never returned; cancelling never cancels other waiters and never returns real I/O quota early

DROP_INTERNAL_REF(d):
    if d.internalRefs.fetch_sub(1) == 1:
        mark d.reservedCleanupJob READY           // never destroy arbitrary values on the calling thread

EXPIRE_WAITERS_STEP(f,quota):
    if TRY_GATE(f.stripe.gate,FLIGHT_RETRIES) failed: return RETRY_LATER
    ws = next bounded batch of Ref<Waiter> from f's fixed waiter slots
    unlock
    for w in ws:
        if NOW() >= w.deadline: READY_INLINE_ERROR(w,TIMEOUT)
    // only changes results that are still WAITING; a successful READY result is not revoked by a later timeout
```

Work is a tagged record in the Flight's pre-reserved work slot: it stores stage, afterNotify, waiter scan position, read-only result metadata, reuseDeadline (0 by default), and exclusive ownership of the candidate or late source. Work stages are modified only by completionOwner; if acquiring the gate fails the current stage is kept and the pump yields. Re-entry resumes from that stage and must not consume the mailbox again, notify the same waiter twice, or create another candidate. The completion mailbox, work slot, and periodic scan keep this path from depending on a free slot in the ordinary fill queue. Errors with a payload go through Delivery; only a bounded error code without retained objects may use READY_INLINE and release the empty response reservation.

`CURRENT_TOKEN` returns the current namespace generation and shard fence; registration uses the stable double read above, commit is re-checked by the sole maintainer, and concurrent namespace advancement is still caught by the generation check on reads. `CALCULATE_HARD_DEADLINE` takes the conservative validity basis plus TTL, and an earlier deadline declared by the business only shortens it; jitter only shortens too, and invalid result size / CPU metadata is rejected per the contract.

Delivery and the candidate may share the underlying immutable result with zero copy, but **response retention and cache retention are covered by M_response and M_pending respectively**. completed_pending reads the Delivery and never dereferences the preparedEntry whose ownership the maintainer is moving. Clearing the bridge only drops one internal Ref; the response is still billed while a slow waiter has not taken it. It settles only after the response is truly released or transferred—removing the Flight from the registry does not mean its pool record can be reused immediately.

<a id="ahkc-a7"></a>

### A.7 Invalidation, fixed-slot cooldown scan, and hierarchical timing wheel

**The reference implementation runs cooldown with a fixed index-slot cursor so scan progress cannot be changed by continuous queue inserts; the SIEVE hand still serves victim selection only.** Read-side expiry checks are independent of the maintenance work below.

```text
SUBMIT_CONTROL(cache,operation):
    if cache.state != OPEN: return CLOSED
    r = TRY_BUNDLE(control record, reserved completion slot)
    if failed: return BUSY                       // not accepted, so no pending-effect promise was created
    queue = fixed namespace or shard control queue selected by operation
    if not TRY_PUSH(queue,Control(operation,r)):
        RETURN_ONCE(r); return BUSY
    SIGNAL_CONTROL_OWNER(queue)
    return ACCEPTED(completionHandle)            // means only "received"; the effect is determined by the completion handle

COMPLETE_CONTROL_ONCE(control,result):
    publish result by single terminal CAS in preReserved completion slot
    notify host readiness without inline callback
    // the record slot is returned after both the owner Ref and completionHandle Ref end; no dependency on the ordinary fill queue

INVALIDATE_KEY_IN_OWNER(control):
    shard.fillFence = CHECKED_NEXT(shard.fillFence)
    e = INDEX_FIND_IN_OWNER(index,control.completeKey)
    if e found:
        e.readable.store(false)                  // complete the logical effect first
        TRY_RETIRE(e,controlCleanupQuota)         // no retirement budget does not affect the logical confirmation
    COMPLETE_CONTROL_ONCE(control,SUCCESS)

INVALIDATE_NAMESPACE_IN_CONTROL_OWNER(control):
    namespace.gen = CHECKED_NEXT(namespace.gen)   // linearization point of namespace invalidation
    mark namespace cleanup pending in fixed namespace bitset
    COMPLETE_CONTROL_ONCE(control,SUCCESS)        // does not wait for all shards to be scanned

COLD_SWEEP(shard,quota):
    repeat at most quota.indexSlots times:
        i = shard.coldCursor
        shard.coldCursor = (i+1) mod index.slotCount
        e = index.slots[i]
        if e in {EMPTY,TOMBSTONE}: continue
        t = NOW()
        if not e.readable or e.namespaceGen != namespace.gen or t >= e.hardDeadline:
            e.readable.store(false); TRY_RETIRE(e,quota); continue
        if t < e.graceUntil: continue
        h = HEAT_READ(e.heat,t)
        if h == UNKNOWN: e.lowSince=NONE; continue
        if h >= Tout[e.namespace]: e.lowSince = NONE; continue
        if e.lowSince == NONE: e.lowSince=t; continue
        if t-e.lowSince >= coldFor:
            h2 = HEAT_READ(e.heat,NOW())
            if h2 == UNKNOWN: e.lowSince=NONE; continue
            if h2 < Tout[e.namespace]:
                e.readable.store(false); TRY_RETIRE(e,quota)
            else if h2 >= Tout[e.namespace]: e.lowSince=NONE

TRY_RETIRE(e,quota):
    if quota cannot pay one remove: return false
    lease = TRY_BUNDLE(retired bytes=e.billedBytes, retired record slot, cleanup job slot)
    if failed: return false                       // index / resident ledger is still retained
    INDEX_REMOVE(e)
    UNLINK_POLICY_AND_TIMER_FIX_HAND(e)
    residentCount -= 1; residentBytes -= e.billedBytes
    TRANSFER_RESIDENT_TO_RETIRED(e,lease)
    REGISTER_RETIRED(e,epoch.current)
    consume one removal quota
    return true

WHEEL_ADD(e):
    require candidate already passed wheel capacity/range check before COMMITTING
    T = ceil(e.hardDeadline / wheelTick)
    base = wheel.tickInProgress if present else wheel.cursorTick
    delta = max(1,T-base)
    level = smallest ell in [0,L-1] with delta < K^(ell+1)
    slot = floor(T/K^level) mod K
    bucket = wheel.buckets[level][slot]
    if bucket.epoch != wheel.epoch: reset bucket head/tail; bucket.epoch=wheel.epoch
    append e to bucket; write e.timerEpoch/level/slot/links

WHEEL_REMOVE(e):
    if e.timerEpoch == wheel.epoch and e has timer member:
        unlink e using its bucket and intrusive links
    clear e timer membership                     // residual pointers from an old epoch must not be used to re-link

START_WHEEL_REBASE(targetTick):
    wheel.epoch = CHECKED_NEXT(wheel.epoch)
    wheel.cursorTick = targetTick
    clear fixed pending tick/cascade task descriptors
    wheel.rebasing=true; wheel.rebaseCursor=0
    shard.pauseReasons.add(WHEEL_REBASE)
    // no full traversal to clear all nodes; old timer members are logically invalidated by epoch

WHEEL_REBASE_STEP(quota):
    while quota permits one index slot and rebaseCursor < index.slotCount:
        e = index.slots[rebaseCursor++]
        if e in {EMPTY,TOMBSTONE}: continue
        WHEEL_REMOVE(e)                           // the old epoch only clears this entry's membership mark
        if e invalid or expired or stale namespace:
            e.readable=false; TRY_RETIRE(e,quota)
        else: WHEEL_ADD(e)
    if rebaseCursor == index.slotCount:
        wheel.rebasing=false
        shard.pauseReasons.remove(WHEEL_REBASE)   // other pause reasons are not cleared

ADVANCE_WHEEL(now,quota):
    target = floor(now/wheelTick)
    if not wheel.rebasing and target-wheel.cursorTick > maxWheelLag:
        START_WHEEL_REBASE(target)
    if wheel.rebasing: WHEEL_REBASE_STEP(quota); return
    while quota permits a tick/task/item:
        if no tick task in progress:
            if wheel.cursorTick >= target: return
            k = wheel.cursorTick+1
            make fixed tasks for each ell with k mod K^ell == 0,
                ordered highest ell down to 0; each task names (wheelEpoch,ell,slot)
            wheel.tickInProgress=k
        task = first unfinished task
        if task bucket epoch stale or bucket empty:
            mark task done
        else:
            e = remove first timer member from bucket
            if e invalid or namespace stale or NOW() >= e.hardDeadline:
                e.readable=false; TRY_RETIRE(e,quota)
            else: WHEEL_ADD(e)                    // move down a level; exact TTL prevents premature invalidation
        if all tasks for k done:
            wheel.cursorTick=k; clear in-progress tasks
```

The total timing-wheel span must exceed `maxTTL + maxWheelLag×wheelTick + rounding margin`; the rebase scan also has a progress budget, during which new fills stop while control, reads, and Flight responses continue. If the rebase is too slow and the lag exceeds the allowed bound again, bounded recovery re-enters and exposes insufficient maintenance capacity. All due/cascade tasks store only fixed bucket identity and epoch, never raw list cursors that may become invalid. An invalidated entry whose retirement budget was insufficient at timer removal is retried by the independent coldCursor.

<a id="ahkc-a8"></a>

### A.8 Epoch protection, owned handles, and actual reclamation

**PIN must announce protection before reading the index; physical release waits for both epoch safety and the owned count reaching zero.** The sequentially consistent reference protocol below separates old readers with two epoch advancements and never substitutes "nobody seems to be reading right now" for the safety condition.

```text
PIN(domain):
    slot = TAKE_PREASSIGNED_GUARD_SLOT_FROM_CONTEXT(domain)
    if no slot: return BUSY
    repeat at most PIN_RETRIES:
        g = domain.epoch.load_SC()
        slot.store_SC(ACTIVE(g))
        if domain.epoch.load_SC() == g: return Guard(domain,slot,g)
        slot.store_SC(INACTIVE)
    RETURN_CONTEXT_GUARD_SLOT(slot)
    return BUSY                                  // the slot stays INACTIVE, leaking no nested protection quota

UNPIN(guard):
    guard.slot.store_SC(INACTIVE)
    RETURN_CONTEXT_GUARD_SLOT(guard.slot)          // runs no reclamation scan and no destructor

CLONE_CACHE_HANDLE(handle):
    require an existing owning handle remains alive during clone
    if bounded non-overflowing increment of handle.entry.ownedPins fails: return BUSY
    return another owning handle

DROP_CACHE_HANDLE(handle):
    handle.entry.ownedPins.fetch_sub_SC(1)
    // the retired record already lives in a bounded pool; reclamation never depends on another possibly full notification queue

ADVANCE_EPOCH_STEP(domain,quota):
    if no scan active:
        scanEpoch=domain.epoch; advanceScanCursor=0; scanBlocked=false
    while quota permits one reader slot and cursor < readerSlotsPerShard:
        s = domain.slots[cursor++].load_SC()
        if s is ACTIVE and s.epoch < scanEpoch: scanBlocked=true
    if cursor == readerSlotsPerShard:
        if not scanBlocked and domain.epoch == scanEpoch:
            domain.epoch.store_SC(CHECKED_NEXT(scanEpoch))
        clear scan state                         // each complete scan advances at most one epoch

REGISTER_RETIRED(e,g):
    r = preReservedRetiredRecord
    r.entry=e; r.entryId=e.entryId; r.retireEpoch=g
    r.retiredReceipt=retired receipt; r.state=WAIT_SAFE
    keep r in fixed retired pool until disposal really completes

RECLAIM_STEP(quota):
    ADVANCE_EPOCH_STEP(epoch,quota.readerScan)
    for next fixed retired records within quota.recordScan:
        if r.state == WAIT_SAFE:
            if epoch.current < r.retireEpoch+2: continue
            if r.entry.ownedPins.load_SC() != 0: continue
            if quota cannot pay a disposal handoff: continue
            // at this point no guard can obtain this Entry from the removed index and no owning handle can be cloned
            if CAS(r.state,WAIT_SAFE,SUBMITTING):
                if not TRY_SUBMIT(r.reservedCleanupJob(DISPOSE_ENTRY_AND_MARK_DONE,r)):
                    CAS(r.state,SUBMITTING,WAIT_SAFE)
                // after acceptance the submitter never overwrites the phase; the task may already be done
        if CAS(r.state,DONE,ACCOUNTED):
            RETURN_ONCE(r.retiredReceipt)
            drop pool owner's record Ref once; slots reusable only after all Ref lifetimes end

DISPOSE_ENTRY_AND_MARK_DONE(r):                    // reclaim executor; no index/Flight/maintenance critical section
    assert CAS(r.state,SUBMITTING,DISPOSING) succeeds
    DISPOSE(r.entry.valueRef, r.entry.key, r.entry)
    r.state.store_release(DONE)                   // r is a separate pre-allocated record, never embedded in the just-released Entry

DISPOSE_PENDING_USING_RESERVED_JOB(a):
    move a.preparedEntry to a.reservedCleanupJob if not already moved
    mark cleanup job READY
    // the pendingReceipt is held by that job and RETURN_ONCEd after real release; a terminal cell alone never returns it

PROGRESS_CLEANUP_JOBS(pool,quota):                 // pending and Delivery share this phase protocol
    for next fixed records within quota.recordScan:
        if CAS(job.state,READY,SUBMITTING):
            if not TRY_SUBMIT(RUN_CLEANUP_JOB(job)):
                CAS(job.state,SUBMITTING,READY)
        if CAS(job.state,DONE,ACCOUNTED):
            RETURN_ONCE(job.receipt)
            drop pool owner Ref once; reuse the slot only after remaining metadata Refs end

RUN_CLEANUP_JOB(job):
    assert CAS(job.state,SUBMITTING,RUNNING) succeeds
    if job.kind == PENDING_ENTRY: DISPOSE(owned entry/value/key)
    if job.kind == DELIVERY:
        assert job.delivery.internalRefs == 0; DISPOSE(job.delivery.outcome)
    if job.kind == LATE_SOURCE: DISPOSE(job.sourceOutcome)
    job.state.store_release(DONE)
```

Reclamation tasks may be extremely fast, so the submitter must not overwrite DONE after completion. Pending / Delivery cleanup jobs use the same handshake: `READY→SUBMITTING→RUNNING→DONE`, and SUBMITTING is restored to READY only when the executor definitely rejected the task; once accepted, the terminal state is published only by the task.

Pending / Delivery records awaiting release are also scanned by fixed cursors within budget. All classes release their held objects uniformly through RUN_CLEANUP_JOB and settle budget after DONE→ACCOUNTED. Their jobs are pre-reserved, so a scheduling rejection only affects progress and never loses release responsibility; byte quota is not returned before the real end. Fixed index / reader slots are kept until the cache object is actually destroyed; fixed structures still used by registered contexts or in-flight calls are not released merely because the business object was reclaimed.

<a id="ahkc-a9"></a>

### A.9 Maintenance scheduling, quota transfer, and shutdown

**Maintenance uses a unique task identity, per-class quotas, and timed compensation; work waiting for time to advance or for readers to exit must not trigger endless immediate rescheduling.** Flight completion work uses an independent fixed work slot and readiness mechanism and never queues behind a possibly full ordinary fill queue.

```text
SIGNAL_FLIGHT_PUMP(f):
    f.pumpNeeded.store(true)
    mark f's preallocated completion record ready // fixed records aggregate notifications as bits/state, no per-event allocation
    request bounded dispatch from completion executor
    // a scheduling rejection keeps ready and the periodic scan retries; TRY_PUMP_FLIGHT excludes parallel execution via completionOwner

SIGNAL_MAINTENANCE(shard):
    shard.pending.store(true)
    ENSURE_MAINTENANCE_TASK(shard)

ENSURE_MAINTENANCE_TASK(shard):
    if before shard.nextAllowedRunTime: return    // the periodic trigger owns the deferred retry
    ticket = next task identity from preallocated task record
    if not CAS(shard.ownerState,IDLE,QUEUED(ticket)): return
    if not TRY_SUBMIT(task=RUN_MAINTENANCE(shard,ticket)):
        CAS(shard.ownerState,QUEUED(ticket),IDLE)
        shard.pauseReasons.add(EXECUTOR_REJECTED)
        shard.pending=true                       // no recursive retry, and the caller is never asked to drain

RUN_MAINTENANCE(shard,ticket):
    if not CAS(shard.ownerState,QUEUED(ticket),RUNNING(ticket)): return
    shard.pauseReasons.remove(EXECUTOR_REJECTED)
    quota = bounded round allowance
    classes = [CONTROL, LIFECYCLE, SAMPLES, FILL, RECLAIM]
    while quota permits one complete bounded work unit:
        cls = next class using saved resumeClass and per-class allowance
        if cls == CONTROL: process at most allocated accepted control operations
        if cls == LIFECYCLE:
            ADVANCE_WHEEL(NOW(),classQuota)
            COLD_SWEEP(shard,classQuota)           // fixed index-slot coverage, still progressing while requests are idle
        if cls == SAMPLES: DRAIN_SAMPLES(shard,classQuota)
        if cls == FILL and pauseReasons empty:
            if fill rate tokens and classQuota allow complete worst-case decision:
                a = TRY_POP(fillQueue)
                if a exists: PROCESS_FILL(a)
        if cls == FILL and admission paused:
            discard bounded queued candidates with explicit terminal reasons
        if cls == RECLAIM:
            RECLAIM_STEP(classQuota)
            PROGRESS_CLEANUP_JOBS(pendingJobs,classQuota)
            PROGRESS_CLEANUP_JOBS(deliveryJobs,classQuota)
        save next resumeClass
        if all classes used allowance or soft time reached: break
    shard.pending=false
    CAS(shard.ownerState,RUNNING(ticket),IDLE)
    // after this point the old task never writes resident structures; re-check to cover the enqueue-exit intersection
    if shard.pending or IMMEDIATE_WORK_REMAINS(shard):
        if round CPU/rate allowance remains: ENSURE_MAINTENANCE_TASK(shard)
        else: retain pending and set nextAllowedRunTime for periodic refill

PERIODIC_TICK(cache):
    for bounded batch of fixed shards:
        refill time-based maintenance/fill allowances
        SIGNAL_MAINTENANCE(shard)
        for bounded Flight/Delivery pool records:
            EXPIRE_WAITERS_STEP(record.flight,waiterScanQuota)
            if mailbox, deadline, fill terminal, or retained continuation needs progress:
                SIGNAL_FLIGHT_PUMP(record.flight)
    schedule next bounded tick through host scheduler
```

`IMMEDIATE_WORK_REMAINS` covers published queue input, unfinished round / timing-wheel catch-up, and runnable completion work; when only a future timer, a retired object waiting for guards, or unfinished I/O remains, it waits for the periodic trigger / real event instead of busy polling. Each period processes only a fixed batch of shards / records and preserves a global cursor; the actual maximum scan interval feeds into P, L, and the load-timeout service level.

Quota transfer may move only idle quota, keeps at most a fixed number of transfer records, and gives every grant token a single owner. Transfers include in-transit state; quota loss must not be inferred from non-atomically collected shard values.

```text
DONOR_TAKE_IDLE_IN_OWNER(request):
    freeN = max(0,countQuota-residentCount-localKeepReserveN)
    freeB = max(0,byteQuota-residentBytes-localKeepReserveB)
    amount = componentwise min(request.wanted,(freeN,freeB))
    // no commit is currently suspended across rounds; a commit occupying the owner must finish first and cannot donate its resources
    countQuota -= amount.count; byteQuota -= amount.bytes
    complete request once with UNIQUE_GRANT_TOKEN(amount,owner=IN_TRANSIT)

COORDINATOR_RECEIVE_IDLE(grant):
    if transfer grant ownership IN_TRANSIT -> GLOBAL succeeds once:
        globalFreeCount += grant.count; globalFreeBytes += grant.bytes

COORDINATOR_ISSUE(receiver,amount):
    if globalFree insufficient: return
    globalFree -= amount
    token = UNIQUE_GRANT_TOKEN(amount,owner=IN_TRANSIT)
    if receiver control queue does not accept token:
        COORDINATOR_RECEIVE_IDLE(token)           // returned once; delivery cannot continue in parallel

RECEIVER_APPLY_IN_OWNER(token):
    if countQuota+token.count > index.countMax:
        return token to coordinator through its preallocated completion record
    else if transfer token ownership IN_TRANSIT -> RECEIVER succeeds once:
        countQuota += token.count; byteQuota += token.bytes
        acknowledge applied                      // observing completion twice must not add quota again
```

The global conservation ledger is `Σ shard quota + global free + in-transit quota = configured quota`, separately for entries and bytes. State transitions follow grant ownership, and the execution order guarantees that quota is never simultaneously usable by both parties; multi-shard metric snapshots do not offer a one-shot transaction view across atomic fields. The coordinator neither modifies shard quota directly nor synchronously evicts entries from other shards for the current candidate.

```text
BEGIN_CLOSE(cache):
    if CAS(cache.state,OPEN,CLOSING) succeeds:
        stop accepting new request/registration/control admission
        mark fixed per-shard close work pending
    return CloseHandle with distinct stopped/work/reclaimed completion states

CLOSE_SHARD_STEP(shard,quota):
    finish or explicitly fail already accepted control operations within quota
    CANCEL_FILL_CELL(each queued candidate within quota,CLOSED)
    // COMMITTING is settled by the original maintainer; the same shard owner never runs another committer in parallel
    for bounded occupied index slots:
        entry.readable=false; TRY_RETIRE(entry,quota)
    for bounded Flight records:
        stop new joins; make current waiters terminal according to close contract
        retain source mailbox, real I/O permit and response responsibility until source ends
        schedule completion pump / candidate cleanup
    keep epoch, timer and cleanup progress active

POLL_CLOSE(cache):
    stopped = no new admission can succeed
    workDone = all accepted controls terminal and all fill work settled
               and all real sources finished and all waiter outcomes settled
    reclaimed = workDone and no indexed/retired/pending/Delivery retention remains
                and no borrowed guard/cache owning handle/dependent callback Ref remains
    if stopped: report STOPPED_ADMISSION
    if workDone: report WORK_DRAINED
    if reclaimed: set cache.state=RECLAIMED; report RECLAIMED
    if user deadline reached before completion: report remaining categories and return
    // no timeout is equivalent to force-ending I/O, returning retention budget, or releasing referenced memory
```

<a id="ahkc-a10"></a>

### A.10 Pseudocode-level invariants and implementation validation entry points

Every implementation module must hold at least the following assertions; they are more stable than function names, bit widths, or container choices:

```text
I1  For each shard: residentCount <= countQuota <= index.countMax; residentBytes <= byteQuota.
I2  For each budget, available >= 0; used + reserved + available == limit; a Receipt is returned at most once.
I3  Every index-readable Entry is fully initialized; the full key matches; namespaceGen, readable, and TTL are valid at read time.
I4  Every published Entry appears only within its own fixed probe range; no deletion clears a TOMBSTONE to EMPTY.
I5  O and Q are disjoint; every victim is removed / billed once; old and new identities differ; no irreversible commit without reservation.
I6  A candidate has exactly one COMMITTING owner; terminal states never roll back; timeout and commit never produce contradictory terminal states.
I7  At most one real source operation per key+token; a timeout never returns the I/O credit of a source operation that has not finished.
I8  Each waiter gets at most one result delivery / slot return; after readiness the host's take or cancel settles it, and business delivery never depends on filling.
I9  All index reads PIN first; owned references are acquired inside the protection period; physical reclamation satisfies two epoch generations and ownedPins=0.
I10 An entry has at most one active timer membership; timer epoch/entryId match; expiry and generation invalidation do not depend on timer progress.
I11 Each logical request samples at most once and records hit/miss heat at most once; internal lookups add no extra samples.
I12 Every accepted control, completion mailbox, candidate, and reclamation responsibility can be traced to a terminal state from bounded structures.
```

Acceptance order: first validate the state / mathematical results of A.2–A.5 with a deterministic clock and small capacities, then test the ownership, completion, and budget return of A.6–A.9 with controllable interleavings, and finally measure the cost and service benefit under real load. Even if the sequentially consistent pseudocode passes model checking, that does not automatically mean a weak-memory-order implementation is correct; every optimization must preserve the same invariants and concurrency history semantics.

The design self-check verified 6,000 fixed-probe index mutations, 3,500 subset solves against independent combined enumeration, 250 timing-wheel scenarios, and heat-boundary, terminal-order, and epoch-lifetime examples against an independent sequential model; all passed. That result validates only these algorithmic relations and does not mean native concurrency, weak memory ordering, or performance targets have been validated.
