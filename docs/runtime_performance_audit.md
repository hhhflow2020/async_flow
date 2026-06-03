# Runtime Performance Audit

Date: 2026-06-02

## Scope

This audit reviews the current `af` runtime implementation for lock usage,
cache behavior, branch behavior, false-sharing risk, remaining performance
headroom, and mechanism-level comparison with adjacent runtimes.

This pass is a source-level audit plus existing benchmark evidence. It does not
claim a definitive performance ranking because no new apples-to-apples benchmark
was run against Boost.Asio, libuv, Seastar, or Go in this pass.

## Current Strengths

- Hot-path framework scheduling avoids ordinary mutexes. The only framework
  mutex in active headers is the optional task registry path in
  `runtime_task_lifecycle.hpp`; it is disabled by default through
  `RuntimeTraitsConfig::task_registry_enabled == false`.
- Runtime-thread self posts are explicitly routed to the executor local queue.
  Runtime cross-thread posts use per-source/per-target SPSC queues, and external
  posts use one MPSC queue per target thread. This is the right topology for
  avoiding unnecessary contention in the common fixed-thread case.
- `BoundedSpscQueue` separates producer and consumer positions/caches with
  cache-line alignment and uses acquire/release only at the publication points.
- `BoundedMpscQueue` and `BoundedMpmcQueue` use per-cell sequence counters and
  cache-line-aligned cells, which avoids producer/consumer false sharing at the
  cost of larger queue memory footprint.
- Runtime global counters that are touched from multiple threads use
  `CacheLineAtomic`, including runtime status, unfinished-task count, generation,
  external post counters, executor wake flags, and stop/sleep flags.
- External post admission checks the runtime generation after entering the
  active-post counter, so a delayed external producer cannot enqueue work into a
  restarted runtime generation.
- Runtime-bound async logging control flags now use `CacheLineAtomic` for
  consumer wake/task/shutdown/finish state and file/TCP/UDP backend shutdown
  state, keeping cross-thread wake and shutdown traffic off adjacent cold fields.
- `AsyncLogger` keeps the separate `ready_` and `pending_` counters for correct
  publication/flush semantics, but its producer-side pending increment and
  consumer-side counter decrements now avoid unnecessary acquire fences.
- Ordered async logging keeps one global MPSC enqueue cursor for strict
  producer order, but shards producer record pools and accepted/dropped counters
  onto cache-line-isolated producer shards so allocation and stats updates do not
  create additional high-fan-in cache-line bouncing.
- Runtime-bound file/TCP/UDP log backend batch counters keep acquire loads for
  flush waiters, but their enqueue/complete RMW operations now avoid unnecessary
  acquire fences.
- Runtime file-log flush requests use a relaxed sequence counter; the flush
  completion edge is carried by `completed_flushes` release/acquire, not by the
  request counter.
- `ReadySourceSet` stores each 64-bit ready-source word on its own cache line.
  The executor also rotates multi-word scans, so thread counts above 64 do not
  permanently bias ready-source word zero. Ready-source bit clearing is a
  relaxed hint update; cross-thread task publication is synchronized by the SPSC
  queue tail acquire/release pair, and the scheduler rechecks the queue after
  clearing to cover concurrent producer marks.
- `ObjectPool` uses a batched thread-local slot cache and cache-line-aligned
  slots. Small objects can use a compact single-cache-line slot layout, while
  larger and over-aligned objects keep explicit cache-line storage separation.
  Each block now uses a tagged free stack instead of a general MPMC free queue.
  TLS refill/flush can move same-block batches with one CAS, pure cross-thread
  destroyers return directly to the owning block, repeated pure remote releases
  use a separate four-entry thread-local direct-return set, and cold refill/flush
  helpers are split out of the hot create/destroy path. Its TLS cache is now
  primary/overflow shaped with eight same-type pool entries, avoiding the severe
  cache-thrash case where one thread alternates between multiple
  `ObjectPool<T>` instances. Single-entry object pools use a dedicated
  primary-cache-only TLS shape, so static task/group pools and executor-owned IO
  pools do not carry overflow-cache state. Direct-return set entries are also
  cleared when that same thread later reaches the local-cache slow path for the
  pool, avoiding stale-marker effects without adding hot-path work. Static task
  and parallel-group pools opt into a 64-slot remote-release batch to reduce
  cross-thread free-stack CAS traffic, and remote-batched pools skip the
  direct-return marker TLS set entirely. Allocated slot-index caching is
  available as an explicit task-pool trait, but it remains disabled by default
  because longer runtime task-hop A/B runs did not support it. The same runtime
  task/group pool configuration path now also forwards local-cache set size,
  direct-release set size, local-cache capacity, and chunk size, so these measured
  object-pool policies are available to runtime workloads. Runtime task/group
  pools now default to a one-entry local cache set and retain an explicit
  256-slot chunk default, while generic `ObjectPool<T>` now uses a 512-slot
  chunk default and keeps its eight-entry cache-set default for same-type
  multi-pool workloads.
- Object-pool tagged-stack head, block-list pointers, and slot-link atomics are
  now compile-time required to be lock-free, preventing a target from silently
  replacing the intended lock-free path with library-level atomic locks.
- Object-pool free-list indexing is intentionally capped at 16 bits so the
  tagged stack keeps 48 version bits. Very large chunk sizes are rejected because
  shrinking the ABA tag to support them would weaken the correctness margin for
  the runtime's actual small fixed chunks.
- Executor IO backend initialization now pre-reserves object-pool storage for
  native wait registrations and io_uring operation/message/address objects,
  using the existing `io_wait_reserve` and `io_uring_entries` capacities. This
  moves unavoidable block allocation out of the first IO submission burst.
- Executor-owned IO object pools now use a one-entry local-cache set. These are
  single-pool instances in the normal executor IO path, so they avoid the
  generic eight-entry same-type multi-pool TLS footprint while preserving the
  generic `ObjectPool<T>` default for multi-pool workloads.
- Existing perf canaries show low branch miss rates: the recorded runtime cases
  are below 1%, and queue/object-pool microbenchmarks are below 2% in
  `docs/remote_performance_report.md`.

## Lock And Contention Assessment

The framework is already close to minimum practical lock usage for this design:

- No hot task scheduling path takes `std::mutex`.
- No ready-queue path uses a central global queue.
- Runtime-thread posts do not contend on an MPSC queue unless they originate
  outside the runtime.
- Shutdown uses atomics and `atomic::wait` to wait for active external posts and
  unfinished tasks.

The main contention points that remain are intentional:

- External submissions to the same target thread contend on that target's MPSC
  enqueue position.
- Cross-thread runtime fan-in contends on the target executor's ready-source
  bitmap words when many source threads mark readiness at once.
- `ObjectPool` block free stacks can contend when TLS caches miss under high
  cross-thread allocate/free churn, although batch refill/flush, direct return
  from generic pure destroyer threads, and bounded remote-release batches for
  static task/group pools reduce the common shared-atomic pressure.
- IO object-pool block allocation should no longer appear on the first steady IO
  registration/submission path when the configured reserve is representative of
  expected concurrency.
- Optional task registry locking can become a bottleneck if enabled in a
  high-rate production runtime. It should remain a debug/shutdown-cancellation
  feature, not a default fast-path feature.

## Cache And False-Sharing Assessment

The implementation deliberately avoids several common false-sharing traps:

- Per-target external post counters are cache-line padded.
- Queue head/tail counters are cache-line separated.
- MPSC/MPMC queue cells are cache-line aligned.
- Executor wake/sleep/stop flags are cache-line padded.
- Runtime-bound async log consumer/backend wake, started, finished, and shutdown
  flags are cache-line padded.
- Ready-source words are cache-line padded.
- Executors are held behind `std::unique_ptr`, so adjacent executor objects are
  not packed contiguously in one vector allocation.

Remaining cache risks:

- `hardware_cache_line_size` is fixed at 64. That is correct for mainstream
  x86_64, but it is not ideal for every target. Consider a config override or
  `std::hardware_destructive_interference_size` fallback where available.
- `Executor` keeps hot scheduler state and cold IO resource state in one large
  object. The padded atomics protect the worst false-sharing cases, but a future
  nested `HotSchedulerState` / `ColdIoState` layout could improve instruction
  locality and auditability without changing public behavior.
- Cache-line-sized MPSC/MPMC cells trade false-sharing avoidance for memory
  bandwidth and footprint. This is usually good for contended queues, but it
  should be validated under large queue counts and many producers.
- `ObjectPool` still stores owner metadata per slot. The compact small-object
  slot layout, tagged free stack, and batched stack operations reduce the worst
  tiny-object footprint and shared atomic traffic, but the per-block free-stack
  head can still become the main object-pool contention point under high
  cross-thread fan-in. The primary/overflow TLS cache avoids same-thread
  multi-pool thrash without adding extra shared state; the direct-return set
  avoids repeated foreign-thread cache scans without occupying a local slot cache
  entry; remote-release batching is intentionally opt-in so non-static pools do
  not hold foreign-thread slots by default.

Recent batch-size evidence:

- Object-pool cross-thread destroy sweep favored a 64-slot remote-release batch
  for every tested medium-payload burst size, reaching 65.832 M/s on
  `BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch/16384`.
- A follow-up 128-slot remote-release canary was added for
  `RemoteReleaseBatchSize=128` with `LocalCacheCapacity=128`. It slightly
  improved the medium single-destroyer `/16384` object-pool row in one run
  (66.881 M/s -> 68.323 M/s), but fan-in was flat and tiny rows were mixed. This
  supports keeping it as explicit benchmark coverage rather than a default.
- A shuffled remote-release canary was added because the original cross-thread
  destroy benchmark releases objects in creation order and therefore often
  returns adjacent same-block slots. The shuffled `/16384` rows are much harder:
  512-slot chunks measured 69.491 M/s ordered but 31.425 M/s shuffled in the
  baseline run. A grouped remote-flush prototype was rejected because the extra
  grouping work regressed the large shuffled row to 26.639 M/s. This keeps
  shuffled remote release as a known remaining specialization target rather than
  hiding it behind an unconditional generic grouping path.
- Runtime-level `BM_RuntimeCrossThreadHopTaskPoolBatch` A/B did not show a
  regression for large bursts: batch 8 reached 638.910 k/s at `/8192`, batch 32
  reached 646.037 k/s, and batch 64 reached 648.161 k/s. The `/1024` case still
  favored batch 8, so this should remain a runtime trait rather than a hidden
  constant.
- Runtime-level 128-slot task-pool cache validation did not support changing the
  default. A longer random-interleaved run measured the current default at
  600.472 k/s for `/1024` and 651.139 k/s for `/8192`; the 128-slot cache with
  the same 64-slot remote-release batch measured 599.200 k/s and 628.420 k/s.
  `RemoteReleaseBatchSize=128` with a 128-slot cache was also worse in the
  preceding runtime run, especially at `/8192`.
- Runtime task-pool allocated-slot-index caching is not enabled by default. A
  short run looked favorable, but a longer A/B on the same Linux host favored
  the default `false` policy: `/1024` measured 676.345 k/s with caching disabled
  and 647.274 k/s enabled, while `/8192` measured 663.154 k/s disabled and
  638.343 k/s enabled. Generic `ObjectPool<T>` also leaves the policy disabled
  because object-pool-only remote-batch microbenchmarks show a small-burst
  tradeoff.
- Generic `ObjectPool<T>` chunk-size validation favored moving the generic
  default from 256 to 512. In the longer random-interleaved run, create/destroy
  `/16384` was effectively tied (406.800 M/s at 256 vs 406.707 M/s at 512), but
  batch `/16384` improved from 96.166 M/s to 101.715 M/s and remote-batched
  cross-thread `/16384` from 67.829 M/s to 70.792 M/s. The 1024-slot chunk was
  slightly better on large batch/cross-thread rows but worse on core
  create/destroy and has a larger first-block footprint, so it remains an
  explicit tuning option.
- Runtime task-pool chunk-size validation did not support the same default
  change for task/group pools. With remote-release batch fixed at 64, the
  `/1024` runtime hop row favored the existing 256-slot chunk
  (645.839 k/s vs 620.995 k/s for 512 and 620.380 k/s for 1024). The `/8192`
  512-slot row looked better but had about 9.25% coefficient of variation, so
  `task_pool_chunk_size` remains an explicit runtime trait with a 256 default.
- Increasing the object-pool TLS cache set from four to eight same-type pool
  entries fixed a pathological same-thread 8-pool alternation case. The
  `BM_ObjectPoolAlternatingPoolSet<8>/16384` row improved from 5.531 M/s to
  248.430 M/s, while 2/4-pool rows stayed in the same broad band. The cost is a
  larger TLS cache-set footprint per object-pool template instantiation.
- Adding a next-overflow hint to that TLS cache set improved the same
  8-pool alternation canary again, from 248.430 M/s to 290.009 M/s. The
  release path checks the pure-remote direct-return marker before using the hint,
  so remote-only destroyers keep their O(1) direct-return fast path.
- The TLS cache-set size is now an explicit object-pool template policy. The
  default stays at eight entries, but a tuned 16-entry instantiation restored a
  pathological 16-pool alternation case from 3.874 M/s to 291.265 M/s without
  imposing the larger TLS footprint on normal pools.
- `ObjectPool` now also supports a specialized one-entry local cache set.
  Same-template single-pool validation favored the smaller shape after the
  specialization: `<1>` reached 439.154 M/s at `/1024` and 442.322 M/s at
  `/16384`, while `<8>` reached 364.981 M/s and 370.093 M/s in the same sweep.
  The generic default remains eight because the multi-pool canaries need
  overflow entries, but runtime task/group pools now default to one entry
  because each task/group pool is a static single-pool instantiation in normal
  use.
- Increasing the thread-local direct-return set from four to eight entries was
  rejected as a new default. It was unstable: one run improved the 8-pool
  round-robin row but regressed 2/4-pool rows, and a follow-up run regressed the
  8-pool `/16384` row to 27.391 M/s. The kept four-entry default is smaller and
  more stable for the measured 2/4-pool cases.
- The direct-return set size is now an explicit object-pool template policy.
  Workloads that really do rotate pure remote destroyers through more same-type
  pools can opt in: a tuned 8-entry set improved the 8-pool `/16384` round-robin
  remote destroy canary from 31.263 M/s to 35.987 M/s, and a tuned 16-entry set
  improved the 16-pool `/16384` row from 28.077 M/s to 29.109 M/s.
- The local-cache capacity is now also an explicit object-pool template policy.
  The default stays at 64 because larger caches regress the balanced common
  cases, but a 128-slot cache improved the batch `/65` row from 197.042 M/s to
  293.169 M/s and `/96` from 239.326 M/s to 312.200 M/s; a 256-slot cache
  improved `/128` from 264.662 M/s to 382.197 M/s.
- Runtime task and parallel-group pools now receive the configured
  remote-release batch size, slot-index caching policy, local-cache set size,
  direct-release set size, and local-cache capacity through the actual
  `ObjectPool` template instantiation. This closes the previous gap where some
  object-pool tuning policies existed only at the generic pool layer.
- Runtime task-pool A/B after the one-entry specialization did not prove a
  scheduler-level throughput win, but also did not show a meaningful regression.
  A longer random-interleaved `BM_RuntimeCrossThreadHopTaskPoolBatch<64, false>`
  run measured 599.888 k/s at `/1024` and 649.366 k/s at `/8192` with the
  one-entry default, versus 607.135 k/s and 653.017 k/s with an explicit
  8-entry cache set. The run had high variance, so the kept argument for the
  one-entry runtime default is the clearly faster object-pool single-pool path
  plus smaller TLS footprint, not a claimed scheduler throughput gain.
- A longer runtime task-pool A/B did not support raising the default local-cache
  capacity to 128 after that pass-through. With
  `BM_RuntimeCrossThreadHopTaskPoolBatch<64, false>`, the default 64-slot cache
  reached 621.378 k/s at `/1024` and 651.574 k/s at `/8192`, while the 128-slot
  cache reached 605.192 k/s and 641.907 k/s. The larger value remains explicit
  tuning only.
- A slot-header index cache variant was rejected despite improving the
  object-pool microbenchmark shape, because it failed the runtime-level A/B:
  `BM_RuntimeCrossThreadHopTaskPoolBatch<64, true>` measured 612.650 k/s at
  `/1024` and 651.976 k/s at `/8192`, both below the corresponding `false`
  rows.
- High fan-in object-pool validation showed remote-release batching still helps
  when several destroyer threads return to one pool: `16384/4` improved from
  66.614 M/s to 76.473 M/s, and `16384/8` improved from 61.056 M/s to
  68.826 M/s.
- Cold-burst object-pool validation showed `reserve_slots()` has a large impact
  when it removes block allocation from the timed burst: at `/16384`, throughput
  improved from 33.397 M/s to 99.100 M/s. Smaller bursts still improved by
  13-16%.
- A bulk block-chain reserve prototype was tested and reverted. It reduced
  publication CAS count but did not improve measured reserve time, confirming
  that cold reserve cost is primarily block allocation/initialization.
- A temporary per-block free-stack sharding prototype was tested and reverted.
  It regressed the same high fan-in canary: for `16384/4`, unsharded
  remote-batch throughput was 71.685 M/s, while shard counts 2, 4, and 8 reached
  69.380 M/s, 65.915 M/s, and 63.753 M/s. The likely cause is extra refill
  scanning and cache footprint exceeding the saved head contention.
- Full-cache flush-count experiments with 64 and 48 slots were tested and
  rejected. The existing 32-slot half flush preserved better small-burst locality
  on the new `/64`, `/65`, `/96`, and `/128` batch canaries; the alternatives did
  not produce a robust large-burst win.
- A TLS-local object-pool hot-block hint was tested and rejected. It improved
  some local large-refill canaries but expanded each TLS cache entry and regressed
  small-burst/cross-thread rows; runtime hop evidence was not strong enough to
  justify keeping it.
- A default direct-return create-path branch-elimination variant was tested and
  rejected. It was logically valid under Debug/TSAN `PoolTests`, but remote gcc
  Release microbenchmarks did not show a stable win and regressed
  `BM_ObjectPoolCreateDestroy/16384` in the measured runs.
- A lazy overflow-cache layout for `MultiLocalCacheSet` was tested and rejected.
  It tried to shrink the generic eight-entry default's single-pool TLS footprint,
  but it did not improve the default single-pool benchmark and regressed the
  8-pool same-type alternation canary to about 106 M/s at `/16384`, far below the
  kept inline primary/overflow layout.
- The default direct-return marker was expanded from one pointer to a separate
  four-entry set. This improved the new round-robin pure-remote destroy canary
  for two and four same-type pools while keeping the primary local-cache hot path
  unchanged.
- Host `perf stat -a` around containerized object-pool benchmarks showed branch
  miss rates below 0.4% on hot local and cross-thread object-pool paths. The
  cross-thread rows had much higher L1D miss rates than the local row, matching
  the expected shared-cacheline/atomic-publication cost rather than a branch
  layout problem.

## Branch Behavior

Current branch behavior looks acceptable:

- The hot IO helper cancellation path is marked `[[unlikely]]`.
- Existing perf snapshots report low branch miss rates.
- The scheduling result switch is small and stable.
- Queue full and empty checks are structured as simple fast-path branches.

Potential branch improvements:

- Keep collecting branch-miss counters after every scheduler or IO dispatch
  change. Current object-pool counters are good canaries, but not enough for
  long steady-state networking conclusions.
- Consider adding `[[likely]]` to the common `TaskResult::Pending` and `Again`
  paths only after perf confirms it helps. Blind hinting can make code layout
  worse.
- Avoid adding abstraction layers that hide the local/SPSC/MPSC route decision
  behind virtual dispatch or dynamic allocation.

## Remaining Performance Headroom

Highest-value next work:

1. Add steady-state benchmark suites.
   Current benchmarks are useful canaries, but many include runtime startup,
   thread creation, first-touch page faults, or synthetic zero-byte/invalid IO.
   Add long-running benchmarks for:
   - TCP echo throughput and latency.
   - one-connection-per-thread shard affinity.
   - many external producers to one target MPSC.
   - many runtime producers to one target through SPSC fan-in.
   - many remote task releases to one executor-local pool.
   - io_uring read/write/accept under real payload sizes.
   - Go, Boost.Asio, libuv, and optionally Seastar baselines on the same host.

2. Tune io_uring setup per workload.
   The runtime already exposes SQPOLL, SUBMIT_ALL, COOP_TASKRUN, SINGLE_ISSUER,
   and DEFER_TASKRUN via traits. Defaults are conservative. For Linux-only
   high-performance profiles, benchmark trait presets with these enabled.

3. Tune CQE processing budget.
   `poll_io_uring_completions()` can stop after a completion that should yield
   to its task. This is good for latency/fairness, but can reduce completion
   batching under heavy CQ load. A configurable completion budget may improve
   throughput while preserving tail latency.

4. Add thread affinity and NUMA-aware allocation options.
   The fixed-thread model benefits strongly from pinning and first-touch
   locality. A runtime config for executor CPU affinity and NUMA-local queue
   allocation would make performance more deterministic on large machines.

5. Add backpressure telemetry.
   Queue-full loops currently yield according to policy. Counters for local
   queue full, SPSC full, MPSC full, eventfd wake count, and io_uring submit
   flush count would make production tuning much easier.

6. Add benchmark coverage above 64/128/256 runtime threads.
   The implementation now supports thread counts above 64, but large-thread
   cache and ready-source behavior still needs perf-counter evidence.

## Mechanism-Level Comparison

### Boost.Asio

Boost.Asio is much more general and portable. Multiple threads can run one
`io_context`, and handlers may be invoked by any thread in that pool unless the
application uses strands or explicit executor binding.

Compared with Asio, `af` has stronger fixed-thread affinity and cheaper
same-thread/cross-thread routing in the cases it is designed for. It may win in
sharded, affinity-heavy workloads where users avoid shared state and keep fd
ownership stable. Asio remains more mature, portable, and feature-rich.

### libuv

libuv uses a single-thread event-loop model per loop. Network IO is handled by
the loop thread, while filesystem and DNS-style blocking operations use a thread
pool.

Compared with libuv, `af` can expose multiple fixed IO executors and can use
io_uring for both socket and file-style async operations on Linux. `af` should
have more headroom for Linux-specific sharded server workloads. libuv has a
broader portability and ecosystem advantage.

### Seastar

Seastar is closest architecturally: shared-nothing, per-core sharding, message
passing, and high-performance Linux networking options such as DPDK.

Compared with Seastar, `af` is smaller and simpler, but not yet in the same
category for end-to-end high-performance networking. Seastar likely wins peak
throughput on carefully tuned servers, especially when DPDK/userspace networking
is used. `af` can still be competitive for embedded C++ runtimes, explicit
task-state machines, and io_uring-centric workloads.

### Go

Go uses an M:N scheduler with goroutines, worker threads, processors, a global
runtime, integrated network poller, stacks, GC, and work distribution.

`af` has lower potential overhead when the workload maps cleanly to fixed
threads: no GC, no goroutine stack management, no general work stealing, no
implicit migration, explicit fd ownership, and local/SPSC queues. This can be
better for predictable low-latency C++ services.

Go can win in general server workloads because goroutines are extremely easy to
create, the scheduler dynamically balances work, the standard library integrates
network polling deeply, and application code is simpler. Go also handles
blocking syscalls and mixed workloads more transparently.

The honest conclusion is workload-dependent:

- `af` is structurally positioned to beat Go in fixed-thread, cache-affine,
  C++/io_uring hot paths.
- Go is structurally positioned to beat `af` in developer productivity,
  dynamic load balancing, operational maturity, and mixed blocking/nonblocking
  workloads.
- A real claim requires identical echo/RPC/file benchmarks on the same host,
  with p50/p99 latency, throughput, CPU, context switches, cache misses, branch
  misses, allocations, and memory footprint.

## Priority Recommendation

Do not spend the next pass removing more locks; the remaining lock is not on the
default hot path. The best next performance work is measurement infrastructure:
steady-state real IO benchmarks and cross-runtime baselines. After that, tune
io_uring profiles, CQE completion budget, affinity/NUMA policy, and telemetry
based on measured bottlenecks.

## External References

- Go runtime scheduler and netpoll source: `https://go.dev/src/runtime/proc.go`,
  `https://go.dev/src/runtime/netpoll.go`.
- Boost.Asio thread model documentation:
  `https://www.boost.org/doc/libs/latest/doc/html/boost_asio/overview/core/threads.html`.
- libuv design overview:
  `https://docs.libuv.org/en/v1.x/design.html`.
- Seastar architecture overview: `https://seastar.io/`.
