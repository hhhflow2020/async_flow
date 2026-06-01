# ObjectPool Performance Review

Date: 2026-06-02

## Summary

`af::detail::ObjectPool` is now a much stronger internal pool than the original
implementation: the hot same-thread path is mutex-free and TLS-only after
warmup, the shared free path no longer uses a general MPMC queue, small objects
have a compact cache-line layout, and block refill/flush now moves batches with
one tagged-stack CAS. The TLS cache also now keeps a primary cache plus overflow
entries, so same-thread use of multiple `ObjectPool<T>` instances does not flush
and steal each other's cache. Overflow lookup also carries a next-entry hint, so
round-robin use of several same-type pools avoids repeated linear scans after
warmup. Cross-thread release batching is available as an explicit template
policy and is enabled for the runtime's static task/group pools, while the
generic `ObjectPool<T>` default still returns foreign releases directly.
Repeated pure remote releases now use a separate thread-local direct-release
set, so they avoid both foreign slot hoarding and repeated TLS cache-set scans
even when a destroyer alternates among a few same-type pools, without changing
the hot local-cache layout. The cache-set implementation is also specialized:
single-entry pools use a dedicated primary-cache-only TLS object, while
multi-entry pools keep the primary/overflow structure. Remote-batched pools skip
the direct-release TLS set entirely. An allocated-slot-index cache is available
as an explicit tuning policy, but it remains disabled by default because longer
runtime task-hop A/B runs did not support making it the runtime default. The
runtime task and parallel-group pools now forward all object-pool tuning traits
that affect the measured hot paths: remote-release batch size, slot-index
caching, local-cache set size, direct-release set size, and local-cache
capacity. The generic object-pool cache-set default remains eight entries for
same-type multi-pool workloads, but runtime task/group pools now default to a
single local-cache entry after runtime hop A/B favored that smaller static-pool
shape.

I would still not call it mathematically "ultimate". It is close for the
framework's current task/IO-object usage, but the remaining hard problems are
workload-specific: pool lifetime across foreign TLS caches, high fan-in
cross-thread release pressure, and per-object-type tuning.

## Implemented In This Pass

Source: `include/af/detail/object_pool.hpp`.

- Increased per-thread local cache capacity from 32 to 64 slots.
- Added `reserve_slots()` and `reserve_blocks()` for cold-start control.
- Replaced each block's `BoundedMpmcQueue` free queue with a pool-specific
  tagged free stack.
- Stored free links as atomic slot indices, avoiding non-atomic next-link races
  while keeping the head versioned against practical ABA.
- Capped chunk indexing to a 16-bit slot index so the 64-bit tagged free-stack
  head always keeps 48 version bits. The previous conditional 32-bit index path
  would have supported very large chunks but reduced the ABA tag to 32 bits,
  which is a poor tradeoff for this framework's small fixed chunks.
- Raised the generic `ObjectPool<T>` default chunk size from 256 to 512 after a
  Linux gcc chunk-size sweep showed the 512-slot block shape was the best
  balanced default for single create/destroy, same-thread batch, and
  cross-thread release canaries. The runtime task/group pool keeps an explicit
  `task_pool_chunk_size` default of 256 because runtime hop validation did not
  support moving that higher.
- Added compile-time lock-free atomic requirements for the tagged free-stack
  head, block-list pointers, and slot-link indices. If a target platform would
  lower those atomics to hidden locks, this pool now fails to build instead of
  silently violating the no-lock performance assumption.
- Added true batched `try_pop_many()` and `push_many()` paths so a TLS refill or
  same-block flush uses one CAS for many slots.
- Added a default release policy where a pure cross-thread destroyer does not
  hoard returned slots in its TLS cache; it returns them directly to the owning
  block.
- Added a separate four-entry thread-local direct-release set for the default
  release policy. A pure remote destroyer that repeatedly releases to one pool,
  or alternates among a few same-type pools, now bypasses repeated overflow-cache
  scans while still returning slots directly to the owning block.
- Added `DirectReleaseSetSize` as an explicit template policy with the existing
  four-entry default. Larger direct-release sets are available for workloads
  whose pure remote destroyers rotate through more same-type pools, without
  increasing the default TLS marker footprint.
- Added `LocalCacheCapacity` as an explicit template policy with the existing
  64-slot default. Larger local caches are available for bursty batch
  create/destroy workloads that would otherwise sit just above the default cache
  capacity, without changing normal pool footprint or locality.
- Added 128-slot remote-release benchmark canaries. These cover the explicit
  `RemoteReleaseBatchSize=128` plus `LocalCacheCapacity=128` shape without
  changing the generic or runtime defaults.
- Wired `task_pool_local_cache_set_size`,
  `task_pool_direct_release_set_size`, and
  `task_pool_local_cache_capacity` through runtime traits/config into the static
  runtime task and parallel-group pool instantiations. Added
  `task_pool_chunk_size` for the same reason. The lower-level `ObjectPool`
  policies are now actually selectable for runtime task allocation, not just
  generic pool microbenchmarks.
- Cleared that direct-release set entry when a thread later enters the
  local-cache slow path for the same pool. This handles rare
  remote-only-to-local transitions, including address reuse cases, without
  touching the hot primary-cache path.
- Added `RemoteReleaseBatchSize` as an explicit template policy. Static runtime
  task pools and the parallel-group pool use a batch of 64 to reduce remote
  release CAS traffic; general `ObjectPool<T>` keeps the safer direct-return
  default.
- Added `CacheAllocatedSlotIndex` as an explicit template policy. When enabled,
  slots popped from a block cache their own index in the existing free-link
  field until release, avoiding pointer-difference index computation on
  release. Runtime traits expose it as `task_pool_cache_slot_index`, but the
  default remains disabled after the longer runtime A/B noted below.
- Added conditional compact slot layout. Small payloads can share the first
  cache line with owner/free metadata while adjacent slots still never share a
  cache line.
- Kept larger and over-aligned payloads on an explicit cache-line/over-aligned
  storage offset, guarded by static layout assertions.
- Split cold acquire/refill and full-cache flush code out of the hot path with
  `AF_DETAIL_NOINLINE`, and avoided rewriting the TLS "locally acquired" flag
  on every hot create.
- Replaced the single per-thread cache with a primary/overflow TLS cache set.
  The common single-pool path still checks only the primary owner, while a thread
  that alternates between multiple same-type pools keeps separate cached slots.
- Raised that TLS cache set from four to eight same-type pool entries after the
  8-pool alternating canary showed the four-entry set collapsing to about
  5.5 M/s. The larger set restores 8-pool throughput to about 248 M/s while
  preserving the single-pool primary fast path.
- Added a next-overflow hint for the TLS cache set. This keeps the primary
  single-pool fast path unchanged, but improves round-robin same-thread use of
  overflow entries by checking the next likely cache before falling back to a
  full overflow scan.
- Added `LocalCacheSetSize` as an explicit template policy with the existing
  eight-entry default. This preserves the current TLS footprint for normal use
  while allowing workloads that rotate through more same-type pool instances to
  opt into a larger cache set.
- Extended `LocalCacheSetSize` down to one entry. This gives static single-pool
  workloads a true primary-cache-only TLS shape with no overflow cache entries
  or overflow lookup state. The implementation uses a dedicated
  `SingleLocalCacheSet` for this case and keeps `MultiLocalCacheSet` for
  cache-set sizes above one. The runtime task and parallel-group pools now use
  that one-entry default, while generic `ObjectPool<T>` keeps eight entries to
  avoid the measured same-type multi-pool collapse.
- Avoided touching the direct-release TLS set for `RemoteReleaseBatchSize > 1`
  pools. That set only serves the default direct-return policy; runtime task
  pools batch remote releases and therefore do not need the extra TLS lookup.
- Switched executor-owned IO object pools (`IoWaitRegistration`,
  kqueue timeout registrations, io_uring messages, socket addresses, and
  operations) to the same one-entry local-cache shape. These pools are
  executor-owned single-pool instances in the normal IO path, so they do not
  need the generic eight-entry same-type multi-pool protection.
- Applied the new reserve API in executor IO backend initialization. Native
  `io_wait_pool_` now reserves `io_wait_reserve` slots, kqueue timeout storage
  reserves the same on kqueue builds, and io_uring executors reserve
  `io_uring_entries` slots for operation, message, and socket-address pools so
  the first burst of IO submissions does not pay object-pool block allocation.

## Current Strengths

- No `std::mutex` on `create()` / `destroy()`.
- Compile-time checks reject targets where the object-pool atomics are not
  always lock-free.
- Same-thread allocate/free is TLS-only after warmup.
- Slots are cache-line aligned and adjacent slots cannot false-share.
- Small payloads no longer always pay the old two-cache-line slot footprint.
- Same-thread alternation between multiple same-type pools no longer collapses
  throughput by flushing one pool's TLS cache to use another.
- Cross-thread destroy is correct through the owning block's tagged free stack.
- Pure remote destroyers do not consume object-cache entries, but repeated
  releases to one or a few same-type pools still get O(1) direct-return markers.
- Static task/group pools can batch remote releases in bounded 64-slot bursts,
  reducing cross-thread free-stack contention without changing IO member pools.
- Runtime task/group pools expose the object-pool cache-set size,
  direct-release set size, and local-cache capacity as traits, so pathological
  workloads can opt into larger TLS structures after measurement without
  changing generic object-pool defaults. The runtime task/group default
  cache-set size is one entry because those pools are static single-pool
  instantiations in the normal runtime path.
- Allocated slot-index caching is available as an explicit policy for workloads
  where release-side index computation dominates, but it is not enabled by
  default for generic or runtime pools.
- Batch refill/flush greatly reduces shared atomic traffic when the TLS cache
  misses or overflows.
- Constructor failure returns the slot before rethrowing.
- Cold-start allocation can be controlled explicitly with reserve APIs.
- Runtime IO backends now use those reserve APIs for wait registrations and
  io_uring operation/message/address objects during backend initialization.
- Executor-owned IO object pools use a one-entry local cache set, reducing TLS
  cache-set footprint for the common single-pool IO path while keeping generic
  `ObjectPool<T>` conservative.

## Correctness Coverage

Tests now cover:

- same-thread storage reuse;
- constructor exception recovery;
- cross-thread destroy;
- default cross-thread destroy visibility before the destroying thread exits;
- default cross-thread destroy visibility across alternating same-type pools
  before the destroying thread exits;
- remote-release batch flushing at its threshold;
- remote-release batch flushing at its threshold with a single-entry local cache
  set, matching the runtime task-pool shape;
- remote-release batch flushing when a remote thread exits before reaching the
  threshold;
- cached allocated-slot-index remote-release batch flushing at its threshold;
- repeated cross-thread batch destroy;
- multiple pool instances with the same `T`;
- single-entry local cache sets;
- `reserve_slots()` and `reserve_blocks()`;
- over-aligned payloads;
- concurrent create/destroy from eight threads.

Validation snapshot:

- Local Debug/TSAN build of `asyncflow_runtime_tests`: passed.
- Local Debug/TSAN `PoolTests`: 19/19 passed, no TSAN report.
- Local Debug/TSAN targeted
  `PoolTests|RuntimeConfigTests|IoRuntimeEpollFixture|RuntimeIo`: 44/44 passed
  or skipped as appropriate, no TSAN report.
- Local Release build of `asyncflow_runtime_benchmarks`: passed.
- Remote Linux gcc Release build of `asyncflow_runtime_tests` and
  `asyncflow_runtime_benchmarks`: passed.
- Remote Linux gcc full runtime tests: 161 registered tests, 0 failed; 25
  skipped because io_uring/kqueue capabilities are unavailable in the container.

## Linux GCC Container Benchmark

Primary run: remote Linux host,
`ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.0`, Release build,
main object-pool benchmark filter, `--benchmark_min_time=0.1s
--benchmark_repetitions=5`. ASLR was enabled, but most rows had low variance.

| Benchmark | Mean real time | Mean throughput |
| --- | ---: | ---: |
| `BM_ObjectPoolCreateDestroy/1024` | 2,397 ns | 427.452 M/s |
| `BM_ObjectPoolCreateDestroy/16384` | 38,461 ns | 426.328 M/s |
| `BM_ObjectPoolBatchCreateDestroy/1024` | 7,357 ns | 139.332 M/s |
| `BM_ObjectPoolBatchCreateDestroy/16384` | 166,385 ns | 98.564 M/s |
| `BM_ObjectPoolCrossThreadDestroyBatch/1024` | 24,365 ns | 42.070 M/s |
| `BM_ObjectPoolCrossThreadDestroyBatch/16384` | 356,992 ns | 45.997 M/s |
| `BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch/1024` | 19,858 ns | 51.631 M/s |
| `BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch/16384` | 249,187 ns | 65.832 M/s |
| `BM_ObjectPoolTinyCreateDestroy/1024` | 2,184 ns | 469.295 M/s |
| `BM_ObjectPoolTinyCreateDestroy/16384` | 34,868 ns | 470.231 M/s |
| `BM_ObjectPoolTinyBatchCreateDestroy/1024` | 5,878 ns | 174.330 M/s |
| `BM_ObjectPoolTinyBatchCreateDestroy/16384` | 126,053 ns | 130.105 M/s |
| `BM_ObjectPoolTinyCrossThreadDestroyBatch/1024` | 18,934 ns | 54.189 M/s |
| `BM_ObjectPoolTinyCrossThreadDestroyBatch/16384` | 230,531 ns | 71.153 M/s |
| `BM_ObjectPoolTinyRemoteBatchCrossThreadDestroyBatch/1024` | 17,769 ns | 57.681 M/s |
| `BM_ObjectPoolTinyRemoteBatchCrossThreadDestroyBatch/16384` | 162,226 ns | 101.106 M/s |
| `BM_ObjectPoolAlternatingPools/1024` | 4,127 ns | 496.575 M/s |
| `BM_ObjectPoolAlternatingPools/16384` | 65,955 ns | 497.162 M/s |

Compared with the previous tagged-stack plus release-policy run:

- medium batch `/16384`: 329,236 ns -> 166,385 ns, about 1.98x faster;
- tiny batch `/16384`: 301,254 ns -> 126,053 ns, about 2.39x faster;
- default medium cross-thread batch `/16384`: 419,505 ns -> 356,992 ns, about
  18% faster;
- remote-batched medium cross-thread batch `/16384`: 419,505 ns -> 249,187 ns,
  about 1.68x faster;
- default tiny cross-thread batch `/16384`: 313,609 ns -> 230,531 ns, about 36%
  faster;
- remote-batched tiny cross-thread batch `/16384`: 313,609 ns -> 162,226 ns,
  about 1.93x faster;
- medium single create/destroy `/16384`: now 38,461 ns / 426.328 M/s, so the
  primary/overflow TLS cache shape kept the single-pool hot path competitive;
- alternating between two same-type pools `/16384`: 5,363,232 ns / 6.114 M/s
  before the TLS cache set -> 65,955 ns / 497.162 M/s now, roughly 81x higher
  throughput.

The important conclusion is that the multi-pool fix no longer costs the common
single-pool path: a primary cache preserves the old fast check, and overflow
caches absorb adjacent same-type pools.

Chunk-size sweep, same remote Linux gcc Release host. The final decision uses
the longer random-interleaved run (`--benchmark_min_time=1.0s`,
`--benchmark_repetitions=7`, `--benchmark_enable_random_interleaving=true`):

| Benchmark | 256-slot chunk | 512-slot chunk | 1024-slot chunk |
| --- | ---: | ---: | ---: |
| create/destroy `/1024` | 399.977 M/s | 398.355 M/s | 364.798 M/s |
| create/destroy `/16384` | 406.800 M/s | 406.707 M/s | 369.291 M/s |
| batch `/1024` | 140.500 M/s | 145.618 M/s | 146.075 M/s |
| batch `/16384` | 96.166 M/s | 101.715 M/s | 103.202 M/s |
| default cross-thread `/1024` | 41.360 M/s | 39.647 M/s | 43.450 M/s |
| default cross-thread `/16384` | 46.963 M/s | 49.579 M/s | 49.485 M/s |
| remote-batched cross-thread `/1024` | 56.033 M/s | 58.281 M/s | 58.224 M/s |
| remote-batched cross-thread `/16384` | 67.829 M/s | 70.792 M/s | 71.589 M/s |

The longer run shows 256 and 512 are effectively tied on core create/destroy,
while 512 improves the batch and large cross-thread rows. The 1024-slot chunk
wins several bursty/cross-thread rows, but it regresses core create/destroy by
about 9% and increases first-block footprint more aggressively. The generic
default therefore moves to 512 as the best balanced performance default, with
256 and 1024 kept as explicit benchmark canaries. Runtime task/group pools do
not inherit that generic default: they route through
`RuntimeTraitsConfig::task_pool_chunk_size`, which remains 256 by default after
runtime hop validation.

Runtime hop chunk-size canary with the task-pool remote-release batch fixed at
64:

| Runtime task-pool chunk | `/1024` throughput | `/8192` throughput |
| --- | ---: | ---: |
| 256 | 645.839 k/s | 590.121 k/s |
| 512 | 620.995 k/s | 615.046 k/s |
| 1024 | 620.380 k/s | 590.788 k/s |

The `/1024` runtime row favored the existing 256-slot chunk. The `/8192` 512-row
looked better, but its coefficient of variation was about 9.25%, so it is too
weak to justify changing the runtime task-pool default. `task_pool_chunk_size`
is kept as an explicit trait for measured workload tuning.

Follow-up same-thread multi-pool validation after increasing the TLS cache set
to eight entries:

| Benchmark | Four-entry set | Eight-entry set |
| --- | ---: | ---: |
| `BM_ObjectPoolAlternatingPoolSet<2>/16384` | 440.434 M/s | 438.790 M/s |
| `BM_ObjectPoolAlternatingPoolSet<4>/16384` | 276.632 M/s | 272.376 M/s |
| `BM_ObjectPoolAlternatingPoolSet<8>/16384` | 5.531 M/s | 248.430 M/s |

The 2/4-pool rows were effectively in the same performance band, while the
8-pool row improved by about 45x. The tradeoff is a larger TLS cache-set
footprint per `ObjectPool` template instantiation, which is accepted because the
primary single-pool path remains unchanged and the previous 8-pool behavior was
pathologically bad.

Follow-up validation after adding the next-overflow hint:

| Benchmark | Eight-entry set | Eight-entry set + next hint |
| --- | ---: | ---: |
| `BM_ObjectPoolAlternatingPoolSet<2>/16384` | 438.790 M/s | 443.430 M/s |
| `BM_ObjectPoolAlternatingPoolSet<4>/16384` | 272.376 M/s | 271.200 M/s |
| `BM_ObjectPoolAlternatingPoolSet<8>/16384` | 248.430 M/s | 290.009 M/s |

The 8-pool canary improved by another 16.7% without moving the 2/4-pool rows out
of their previous performance band. The release-side lookup keeps the
direct-release set check before the next-overflow hint, so pure remote destroyers
still hit the O(1) direct-return marker instead of paying an extra overflow hint
check.

The cache-set size is now also an explicit template policy. The generic
`ObjectPool<T>` default remains eight entries, but a larger value can be
selected for workloads that really do rotate through more same-type pool
instances:

| Benchmark | Default 8-entry set | Tuned 16-entry set |
| --- | ---: | ---: |
| `BM_ObjectPoolAlternatingPoolSet<16>/16384` | 3.874 M/s | N/A |
| `BM_ObjectPoolTunedCacheSetAlternatingPoolSet<16, 16>/16384` | N/A | 291.265 M/s |

This keeps the common case conservative while giving the pathological 16-pool
case an explicit escape hatch. It is intentionally not made the default because
doubling the cache-set size also doubles the per-thread TLS slot-cache footprint
for each `ObjectPool` instantiation.

The lower bound is now one cache entry. A one-entry cache set is useful for
static single-pool workloads because it removes overflow cache storage and
overflow lookup state. After specializing the implementation into
`SingleLocalCacheSet` and `MultiLocalCacheSet`, same-template single-pool
validation measured:

| Cache-set size | `/1024` throughput | `/16384` throughput |
| --- | ---: | ---: |
| 1 entry | 439.154 M/s | 442.322 M/s |
| 2 entries | 388.227 M/s | 399.191 M/s |
| 4 entries | 383.333 M/s | 386.957 M/s |
| 8 entries | 364.981 M/s | 370.093 M/s |

The generic default is still eight entries because the same document's
multi-pool canaries showed that smaller sets can collapse when a thread rotates
through many same-type pool instances. Runtime task/group pools are different:
they are static single-pool instantiations for each task/group type, so
`RuntimeTraitsConfig::task_pool_local_cache_set_size` now defaults to one.

Runtime hop validation did not invalidate that runtime-specific default. A
longer random-interleaved run after the one-entry specialization
(`--benchmark_min_time=2.0s --benchmark_repetitions=7`) measured:

| Runtime task-pool cache set | `/1024` throughput | `/8192` throughput |
| --- | ---: | ---: |
| default 1 entry | 599.888 k/s | 649.366 k/s |
| explicit 8 entries | 607.135 k/s | 653.017 k/s |

This runtime-level run is noisy: the measured coefficient of variation was
about 8% for the default `/1024` row and 8-11% for the `/8192` rows. It does not
prove a runtime scheduler throughput win for the one-entry default, but it also
does not show a meaningful runtime regression. The one-entry default is kept for
runtime task/group pools because the object-pool single-pool path is clearly
faster and the TLS footprint is smaller.

Cold-burst reserve benchmark, measured on the same remote Linux gcc Release
build with pool construction/destruction and `reserve_slots()` outside the timed
region. This models runtime IO backend initialization paying the allocation cost
before the first steady IO submission burst:

| Benchmark | Mean real time | Mean throughput |
| --- | ---: | ---: |
| `BM_ObjectPoolColdBurstCreateDestroy<false>/256` | 2,401 ns | 105.924 M/s |
| `BM_ObjectPoolColdBurstCreateDestroy<true>/256` | 2,081 ns | 122.574 M/s |
| `BM_ObjectPoolColdBurstCreateDestroy<false>/1024` | 9,202 ns | 111.025 M/s |
| `BM_ObjectPoolColdBurstCreateDestroy<true>/1024` | 8,129 ns | 126.073 M/s |
| `BM_ObjectPoolColdBurstCreateDestroy<false>/16384` | 497,823 ns | 33.397 M/s |
| `BM_ObjectPoolColdBurstCreateDestroy<true>/16384` | 167,927 ns | 99.100 M/s |

The reserve path improved measured first-burst throughput by about 15.7% at
`/256`, 13.6% at `/1024`, and 3.0x at `/16384`. This is why executor startup now
pre-reserves wait-registration and io_uring operation/message/address pools.

A bulk reserve implementation that allocated a chain of blocks and published the
whole chain with one CAS was tested and reverted. The remote Linux canary did not
support keeping it: compared with the existing incremental `reserve_blocks()`
path, bulk reserve was slightly slower at `/16384` (485,741 ns / 34.401 M/s
incremental vs 506,619 ns / 32.997 M/s bulk) and essentially flat-to-slower at
`/65536` (2,164,506 ns / 31.021 M/s incremental vs 2,178,481 ns / 30.817 M/s
bulk). Reserve cost is dominated by block allocation and initialization, not the
number of free-list publication CAS operations.

Remote-release batch sweep after the direct-return marker was separated from
the TLS cache set:

| Payload | Arg | Best batch | Best throughput | Batch 8 throughput |
| --- | ---: | ---: | ---: | ---: |
| medium | 8 | 64 | 17.991 M/s | 16.600 M/s |
| medium | 32 | 64 | 31.360 M/s | 30.942 M/s |
| medium | 128 | 64 | 42.501 M/s | 41.652 M/s |
| medium | 1024 | 64 | 56.098 M/s | 52.448 M/s |
| medium | 16384 | 64 | 64.984 M/s | 62.340 M/s |
| tiny | 8 | 64 | 18.476 M/s | 15.308 M/s |
| tiny | 32 | 64 | 32.666 M/s | 32.107 M/s |
| tiny | 128 | 4 | 50.021 M/s | 48.534 M/s |
| tiny | 1024 | 64 | 59.839 M/s | 57.821 M/s |
| tiny | 16384 | 32 | 105.185 M/s | 99.857 M/s |

The static runtime task pool is closer to the medium-payload case than the tiny
payload case, and the sweep favored 64 for every medium burst size tested. The
default runtime task/group pool batch was therefore raised from 8 to 64. This is
not the generic `ObjectPool<T>` default: it is a runtime-static-pool tuning that
can temporarily keep up to 63 remote slots per thread/type before flushing.

Follow-up 128-slot remote-release canary, using
`ObjectPool<..., RemoteReleaseBatchSize=128, LocalCacheCapacity=128>`:

| Payload/shape | Arg | Batch 64 / cache 64 | Batch 128 / cache 128 |
| --- | ---: | ---: | ---: |
| medium single destroyer | 8 | 17.702 M/s | 17.717 M/s |
| medium single destroyer | 32 | 31.134 M/s | 31.979 M/s |
| medium single destroyer | 128 | 41.684 M/s | 41.697 M/s |
| medium single destroyer | 1024 | 55.972 M/s | 56.710 M/s |
| medium single destroyer | 16384 | 66.881 M/s | 68.323 M/s |
| tiny single destroyer | 8 | 17.950 M/s | 18.342 M/s |
| tiny single destroyer | 32 | 33.279 M/s | 33.780 M/s |
| tiny single destroyer | 128 | 44.931 M/s | 45.904 M/s |
| tiny single destroyer | 1024 | 61.726 M/s | 61.089 M/s |
| tiny single destroyer | 16384 | 103.604 M/s | 104.023 M/s |
| medium fan-in | 4096/4 | 61.124 M/s | 61.204 M/s |
| medium fan-in | 16384/4 | 73.810 M/s | 73.827 M/s |
| medium fan-in | 16384/8 | 67.905 M/s | 67.413 M/s |

The 128/128 shape is not a new default. It is a useful explicit canary for
single remote-destroyer medium payloads, where it was up to about 2% faster in
this run, but fan-in was flat and tiny large-burst rows were mixed. The generic
default still avoids holding larger foreign batches, and the runtime default
stays at a 64-slot remote-release batch.

High fan-in validation, with multiple destroyer threads returning slots to one
pool:

| Benchmark | Mean real time | Mean throughput |
| --- | ---: | ---: |
| `BM_ObjectPoolFanInCrossThreadDestroyBatch/4096/4` | 67,325 ns | 60.909 M/s |
| `BM_ObjectPoolFanInCrossThreadDestroyBatch/16384/4` | 246,269 ns | 66.614 M/s |
| `BM_ObjectPoolFanInCrossThreadDestroyBatch/16384/8` | 268,700 ns | 61.056 M/s |
| `BM_ObjectPoolRemoteBatchFanInCrossThreadDestroyBatch/4096/4` | 61,523 ns | 66.639 M/s |
| `BM_ObjectPoolRemoteBatchFanInCrossThreadDestroyBatch/16384/4` | 214,453 ns | 76.473 M/s |
| `BM_ObjectPoolRemoteBatchFanInCrossThreadDestroyBatch/16384/8` | 238,361 ns | 68.826 M/s |

Remote-release batching improved this high fan-in case by about 9.4% at
`4096/4`, 14.8% at `16384/4`, and 12.7% at `16384/8`. This confirms the batch
policy is useful beyond the single remote-destroyer benchmark.

A temporary per-block free-stack sharding implementation was also tested and
then reverted. It made the high fan-in case slower because refill had to scan
more heads and the extra cache footprint outweighed the reduced head contention:

| Variant | Mean real time | Mean throughput |
| --- | ---: | ---: |
| unsharded remote batch `16384/4` | 228,893 ns | 71.685 M/s |
| shard 2 `16384/4` | 236,412 ns | 69.380 M/s |
| shard 4 `16384/4` | 248,815 ns | 65.915 M/s |
| shard 8 `16384/4` | 257,271 ns | 63.753 M/s |
| unsharded remote batch `16384/8` | 245,877 ns | 66.732 M/s |
| shard 2 `16384/8` | 271,348 ns | 60.460 M/s |
| shard 4 `16384/8` | 296,382 ns | 55.348 M/s |
| shard 8 `16384/8` | 294,508 ns | 55.701 M/s |

The current conclusion is that generic per-block sharding is not justified. If
high fan-in remains a bottleneck in real runtime benchmarks, the next candidate
should be executor-local pool specialization or producer-aware remote-return
aggregation, not another unconditional shard count inside `ObjectPool`.

Runtime task-pool batch A/B on `BM_RuntimeCrossThreadHopTaskPoolBatch`:

| Batch | Arg | Mean real time | Mean throughput |
| ---: | ---: | ---: | ---: |
| 8 | 1024 | 1.59 ms | 647.447 k/s |
| 32 | 1024 | 1.63 ms | 633.821 k/s |
| 64 | 1024 | 1.65 ms | 622.661 k/s |
| 8 | 8192 | 12.9 ms | 638.910 k/s |
| 32 | 8192 | 12.7 ms | 646.037 k/s |
| 64 | 8192 | 12.7 ms | 648.161 k/s |

This does not prove 64 is universally optimal, but it removes the main concern
that the object-pool microbenchmark win obviously regresses the runtime
cross-thread hop path. The default remains configurable through
`TraitsT::task_pool_remote_release_batch_size`.

Allocated-slot-index cache A/B, measured in the same remote Linux gcc Release
build after making it an explicit runtime task-pool trait. The first shorter run
looked favorable:

| Benchmark | Cache index | Mean throughput |
| --- | ---: | ---: |
| `BM_RuntimeCrossThreadHopTaskPoolBatch<64, ...>/1024` | false | 602.725 k/s |
| `BM_RuntimeCrossThreadHopTaskPoolBatch<64, ...>/1024` | true | 631.154 k/s |
| `BM_RuntimeCrossThreadHopTaskPoolBatch<64, ...>/8192` | false | 647.196 k/s |
| `BM_RuntimeCrossThreadHopTaskPoolBatch<64, ...>/8192` | true | 666.184 k/s |

The same policy is deliberately not the generic `ObjectPool<T>` default.
The object-pool microbenchmark was mixed: cached index improved the large
remote-batch row (`/16384`: 64.976 M/s -> 71.183 M/s) but regressed the smaller
remote-batch row (`/1024`: 54.319 M/s -> 44.007 M/s).

A longer follow-up run contradicted enabling it by default for runtime task
pools:

| Benchmark | Cache index | Mean throughput |
| --- | ---: | ---: |
| `BM_RuntimeCrossThreadHopTaskPoolBatch<64, ...>/1024` | false | 676.345 k/s |
| `BM_RuntimeCrossThreadHopTaskPoolBatch<64, ...>/1024` | true | 647.274 k/s |
| `BM_RuntimeCrossThreadHopTaskPoolBatch<64, ...>/8192` | false | 663.154 k/s |
| `BM_RuntimeCrossThreadHopTaskPoolBatch<64, ...>/8192` | true | 638.343 k/s |

The kept conclusion is conservative: `CacheAllocatedSlotIndex` remains available
for explicit workload tuning, but `TraitsT::task_pool_cache_slot_index` defaults
to `false`.

A follow-up experiment stored the cached index in a slot header field whenever
that did not increase the total slot size, falling back to free-link reuse only
when necessary. It improved the object-pool micro shape, but it also failed the
runtime task-hop A/B: in the same remote gcc container,
`BM_RuntimeCrossThreadHopTaskPoolBatch<64, true>` measured 612.650 k/s at
`/1024` and 651.976 k/s at `/8192`, both below the corresponding `false` rows
at 619.169 k/s and 661.196 k/s. That variant was rejected; the kept
implementation uses the free-link reuse strategy for the explicit
`CacheAllocatedSlotIndex` policy.

Perf-counter snapshot, measured with host `perf stat -a` around the container
benchmark. These are system-wide counters during the benchmark run, so they are
directional rather than perfectly process-isolated:

| Benchmark | IPC | Branch miss rate | L1D miss rate | LLC load miss rate |
| --- | ---: | ---: | ---: | ---: |
| `BM_ObjectPoolCreateDestroy/16384` | 3.19 | 0.093% | 0.183% | 16.5% |
| `BM_ObjectPoolCrossThreadDestroyBatch/16384` | 1.20 | 0.311% | 5.87% | 2.38% |
| `BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch/16384` | 1.35 | 0.347% | 7.72% | 0.56% |
| `BM_ObjectPoolAlternatingPools/16384` | 3.69 | 0.072% | not sampled | not sampled |

This supports the current diagnosis: the local hot path is branch-stable and
mostly L1-resident; the cross-thread paths are dominated by shared cache-line
movement and atomic publication, not branch misprediction.

Rejected default local-cache capacity experiment: increasing
`local_cache_capacity` from 64 to 128 was tested on the same Linux host. It
regressed the main `/16384` rows: medium batch throughput fell from
98.564 M/s to 97.455 M/s, default cross-thread fell from 45.997 M/s to
42.376 M/s, remote-batched cross-thread fell from 65.832 M/s to 63.077 M/s, and
alternating-pool throughput fell from 497.162 M/s to 490.530 M/s. The default
therefore remains 64.

`LocalCacheCapacity` is now available as an explicit template policy for bursty
batch workloads. Follow-up validation showed why this should be opt-in rather
than default:

| Benchmark | Default 64-slot cache | 128-slot cache | 256-slot cache |
| --- | ---: | ---: | ---: |
| `BM_ObjectPoolBatchCreateDestroy/64` | 392.313 M/s | 286.739 M/s | N/A |
| `BM_ObjectPoolBatchCreateDestroy/65` | 197.042 M/s | 293.169 M/s | N/A |
| `BM_ObjectPoolBatchCreateDestroy/96` | 239.326 M/s | 312.200 M/s | N/A |
| `BM_ObjectPoolBatchCreateDestroy/128` | 264.662 M/s | 323.398 M/s | 382.197 M/s |
| `BM_ObjectPoolBatchCreateDestroy/1024` | 135.940 M/s | 133.854 M/s | 146.037 M/s |
| `BM_ObjectPoolBatchCreateDestroy/16384` | 99.385 M/s | 97.790 M/s | 98.474 M/s |

The larger capacities fix the cliff just above 64 cached slots, but they regress
the exact 64-slot case and do not improve the huge `/16384` row. Keeping 64 as
the default preserves the balanced hot path, while the explicit policy lets
batch-heavy users choose a larger cache when the burst shape justifies the TLS
footprint.

Runtime task-pool validation after wiring `task_pool_local_cache_capacity`
through the actual `ObjectPool` instantiation did not justify changing the
default from 64 to 128. A longer remote Linux gcc run
(`--benchmark_min_time=1.5s --benchmark_repetitions=5`) measured:

| Runtime task pool cache | `/1024` throughput | `/8192` throughput |
| --- | ---: | ---: |
| default 64-slot cache | 621.378 k/s | 651.574 k/s |
| tuned 128-slot cache | 605.192 k/s | 641.907 k/s |

The tuned 128-slot row remains useful as a runtime benchmark canary, but it is
not a new default.

Rejected full-cache flush-count experiments: the normal cache-full path keeps
flushing half of the 64-slot TLS cache. A full-cache flush of 64 slots was tested
against new small-burst benchmark arguments. It slightly helped one noisy large
batch run, but it was worse or flat on the locality-sensitive `/64`, `/65`,
`/96`, and `/128` cases. A 48-slot flush was worse overall, especially at
`/128`, `/1024`, and `/16384`. The benchmark now includes `/64`, `/65`, `/96`,
and `/128` canaries so future tuning does not optimize only the huge-burst case.

Rejected TLS-local hot-block experiment: adding a per-cache `Block*` hint before
the shared `hot_block_` read improved some local large-refill canaries
(`BM_ObjectPoolCreateDestroy/16384` reached 443.280 M/s and
`BM_ObjectPoolAlternatingPools/16384` reached 503.717 M/s in one run), but it
regressed the small-burst batch canaries and the cross-thread destroy rows. The
default runtime hop benchmark was also too noisy to justify keeping the larger
TLS cache entry. The implementation therefore keeps one shared `hot_block_`
hint plus the existing primary/overflow TLS slot caches.

Rejected default-path acquired-flag branch elimination: a variant removed the
`locally_acquired` check from default direct-return pool cache hits and kept it
only for `RemoteReleaseBatchSize > 1`. The invariant held under Debug/TSAN
`PoolTests`, but remote gcc Release microbenchmarks did not support keeping the
change. Baseline `BM_ObjectPoolCreateDestroy` measured 388.184 M/s at `/1024`
and 391.972 M/s at `/16384`; the variant measured 381.119-383.657 M/s at
`/1024` and 373.103-378.984 M/s at `/16384` across two runs. It was reverted.

Rejected release-side acquired-flag branch removal: a narrower follow-up removed
the `cache->caches_releases()` condition only from the default
`RemoteReleaseBatchSize == 1` release path after `find_release_cache()` returned
a cache, leaving a Debug assertion for the state-machine invariant. The
invariant held under local Debug/TSAN `PoolTests` and remote Release
`PoolTests`, but same-host Linux gcc A/B did not support the change. With the
condition removed, `BM_ObjectPoolCreateDestroy` measured 385.074 M/s at `/1024`
and 382.292 M/s at `/16384`, and `BM_ObjectPoolAlternatingPools` measured
217.877 M/s at `/1024` and 217.663 M/s at `/16384`. Restoring the condition
measured 405.552 M/s, 410.000 M/s, 392.715 M/s, and 393.030 M/s on the same
rows. Default cross-thread rows were roughly flat or noisy. The release-side
condition is therefore kept.

Rejected lazy overflow-cache experiment: moving `MultiLocalCacheSet` overflow
entries into a lazily constructed separate `thread_local OverflowState` was
intended to give the generic eight-entry default a single-pool-sized primary TLS
object. The benchmark did not support it: default single-pool throughput stayed
around 382.865 M/s at `/1024` and 382.968 M/s at `/16384`, while the important
8-pool canary regressed to 104.467 M/s at `/1024` and 106.223 M/s at `/16384`.
The tuned 16-entry canary still worked, but the generic 8-entry default lost too
much locality. The inline primary/overflow cache-set layout is therefore kept.

Direct-release marker set validation: the old default release marker remembered
one pure remote pool per thread. A destroyer alternating among same-type pools
therefore paid repeated overflow-cache scans. Replacing it with a separate
four-entry direct-release set improved the new round-robin remote-destroy canary:

| Benchmark | Single marker | Four-entry set |
| --- | ---: | ---: |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<2>/1024` | 47.451 M/s | 47.738 M/s |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<2>/16384` | 49.943 M/s | 52.463 M/s |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<4>/1024` | 52.598 M/s | 54.167 M/s |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<4>/16384` | 48.277 M/s | 49.969 M/s |

The normal single-pool default and remote-batched canaries did not show a
systematic regression in the same validation run, so the set is kept outside the
hot primary local-cache layout.

Embedding the direct-release set into `LocalCacheSet` was also tested and
rejected. It removed a second TLS object lookup, but made the local cache-set
layout worse: `BM_ObjectPoolAlternatingPoolSet<4>/16384` dropped to
248.343 M/s and `<8>/16384` dropped to 232.968 M/s in the validation run. The
direct-release set therefore remains separate from the primary/overflow local
caches.

An eight-entry direct-release set was tested after adding an
`BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<8>` canary. The current
four-entry set measured:

| Benchmark | Four-entry set |
| --- | ---: |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<2>/1024` | 49.090 M/s |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<2>/16384` | 52.382 M/s |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<4>/1024` | 54.024 M/s |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<4>/16384` | 51.255 M/s |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<8>/1024` | 52.324 M/s |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<8>/16384` | 32.007 M/s |

The eight-entry variant was unstable when evaluated as a new default. One run
improved the 8-pool rows (`/1024`: 55.415 M/s, `/16384`: 34.362 M/s) but
regressed the 2/4-pool rows; a follow-up run regressed the 8-pool `/16384` row
to 27.391 M/s. The implementation therefore keeps four entries as the default
instead of increasing TLS footprint for an unstable general-purpose win.

The direct-release set size is now an explicit template policy, so workloads
with many pure remote pools can opt in. Longer validation for 8/16-pool
round-robin remote destroyers measured:

| Benchmark | Default 4-entry set | Tuned direct set |
| --- | ---: | ---: |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<8>/1024` | 49.401 M/s | 55.872 M/s (`<8, 8>`) |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<8>/16384` | 31.263 M/s | 35.987 M/s (`<8, 8>`) |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<16>/1024` | 49.907 M/s | 53.527 M/s (`<16, 16>`) |
| `BM_ObjectPoolRoundRobinCrossThreadDestroyBatch<16>/16384` | 28.077 M/s | 29.109 M/s (`<16, 16>`) |

The 8-pool `/16384` default row was noisy in this run, but the tuned rows had low
variance and the direction matched the shorter validation. This supports the
explicit policy while still rejecting a larger default.

Direct-release marker cleanup validation, after clearing stale markers only on
the local-cache slow path:

| Benchmark | Mean real time | Mean throughput |
| --- | ---: | ---: |
| `BM_ObjectPoolCreateDestroy/16384` | 39,253 ns | 418.103 M/s |
| `BM_ObjectPoolCrossThreadDestroyBatch/16384` | 334,421 ns | 49.087 M/s |
| `BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch/16384` | 240,868 ns | 68.098 M/s |
| `BM_ObjectPoolAlternatingPools/16384` | 65,836 ns | 498.065 M/s |

The change is intentionally outside the primary hot path. The validation above
shows the local create/destroy and alternating-pool canaries remained in the
same performance band while closing a stale-marker edge case.

## Remaining Non-Extreme Areas

### Pool Lifetime Contract

The destructor can only discard the current thread's TLS cache. Other threads
must have exited or stopped using the pool before destruction. This matches the
runtime ownership model, but it remains the main correctness contract to
document if `ObjectPool` becomes a wider public allocator.

### Cross-Thread Fan-In

Generic pure cross-thread destroyers return directly to the owning block, which
prevents foreign TLS hoarding. Static task/group pools opt into bounded
remote-release batches to reduce CAS traffic. Heavy fan-in can still contend on
a block's single tagged stack head. A generic per-block sharding prototype was
tested and was slower on the high fan-in canary, so the next step should be
executor-local pool specialization or producer-aware remote-return aggregation,
not another unconditional queue or shard swap.

### Per-Type Tuning

One cache capacity and flush policy may not be optimal for every object type.
Task pools, IO operation pools, and tiny helper objects may want different
cache capacities, reserve sizes, or remote-release batch sizes.

### Perf Counters

Host `perf stat -a` can collect system-wide counters while the containerized
benchmark runs, and this pass used it for the object-pool canaries above. It is
good enough to catch obvious branch/cache regressions on an otherwise idle host,
but it is still not as clean as running the benchmark binary directly against
matching host libraries or inside an image with matching perf tooling.

## Verdict

For current framework usage, the object pool is now high-performance and
structurally sound: no locks, no generic MPMC free queue, cache-line-safe slots,
true batched shared-stack operations, reserve APIs, and TSAN-backed concurrent
coverage.

It is still not "perfect" in the absolute sense. The remaining path to extreme
performance is specialization: per-runtime-thread pools for task/IO objects,
producer-aware remote returns for high fan-in workloads, and real perf-counter
baselines on the target Linux host.
