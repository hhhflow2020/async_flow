# Remote Performance And Pressure Report

Date: 2026-05-31

## Scope

This report records the remote Linux performance and pressure-test evidence for the fixed-thread async runtime after the socket lifecycle modularity split.

The measurements are intended as a directional validation snapshot for cache, branch, syscall/context-switch, and pressure behavior. They are not yet a final microarchitectural baseline because each perf run used a fixed single benchmark iteration to avoid benchmark time-mode amplification under `perf`.

## Environment

- Remote host: `Linux frappuccino 5.14.0-611.41.1.el9_7.x86_64`.
- Host perf: `perf version 6.19.11-1.el9.elrepo.x86_64`.
- Host `perf_event_paranoid`: `0`.
- Docker image: `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.0`.
- Container compiler: GCC 12.2.0, CMake 3.25.1.
- Container perf used during collection: `perf version 6.1.174`.
- Build under test: remote GCC Release benchmark binary from `build-remote-gcc/build/Release`.

## Method

- Perf events:
  `cycles,instructions,cache-references,cache-misses,branches,branch-misses,context-switches,cpu-migrations,page-faults`.
- Benchmark runner mode: `--benchmark_min_time=1x` for perf counter collection.
- Rationale: time-mode perf runs caused `BM_RuntimeCrossThreadHop/8192` to over-amplify under the benchmark runner and exceed the validation timeout. Fixed-iteration mode keeps perf canary runs bounded and repeatable enough for refactor validation.
- Pressure command:
  `ASYNCFLOW_STRESS_MS=5000 ctest --test-dir build-remote-gcc/build/Debug -j6 -R "RuntimeStressTests.ConcurrentInitShutdownAndStartTask|RuntimeIo.StopImmediatelyDropsPendingIoWaitsAndCanRestart|IoRuntimeEpollFixture|IoRuntimeStreamFixture|IoRuntimeDatagramFixture" --repeat until-fail:5 --output-on-failure`.

## Perf Snapshot

| Case | Time | CPU | Throughput | Cycles | Instructions | IPC | Cache miss | Branch miss | Ctx switches | Page faults |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 5.56 ms | 5.53 ms | 1.481 M/s | 123,583,889 | 111,649,840 | 0.90 | 18.481% | 0.63% | 3,295 | 6,856 |
| `BM_RuntimeCrossThreadHop/8192` | 2.39 ms | 2.17 ms | 3.767 M/s | 78,004,051 | 72,460,894 | 0.93 | 26.415% | 0.51% | 120 | 6,871 |
| `BM_RuntimeIoThreadHop/8192` | 3.91 ms | 3.82 ms | 2.142 M/s | 93,050,071 | 72,820,405 | 0.78 | 20.845% | 0.62% | 630 | 6,838 |
| `BM_RuntimeParallelShards/512` | 1.77 ms | 1.71 ms | 298.959 k/s | 72,100,014 | 73,125,834 | 1.01 | 23.640% | 0.74% | 514 | 6,924 |
| `BM_SpscQueuePushPop/16384` | 30,474 ns | 29,679 ns | 552.040 M/s | 8,867,276 | 11,438,341 | 1.29 | 11.915% | 1.81% | 0 | 422 |
| `BM_MpscQueuePushPop/16384` | 150,541 ns | 149,721 ns | 109.430 M/s | 11,751,146 | 15,765,169 | 1.34 | 17.317% | 1.37% | 0 | 803 |
| `BM_ObjectPoolCreateDestroy/16384` | 58,275 ns | 57,563 ns | 284.627 M/s | 8,203,395 | 10,944,102 | 1.33 | 11.517% | 1.98% | 0 | 301 |

## Interpretation

- Branch miss rates are low across all sampled cases. The runtime cases stayed below 1%, and the queue/object-pool microbenchmarks stayed below 2%.
- Queue and object-pool cases had zero context switches. That is consistent with local hot-path behavior and does not indicate unexpected kernel blocking.
- Runtime hop cases still show non-trivial context switches because each fixed-iteration benchmark includes runtime setup, worker interaction, and wake paths.
- Cache miss percentages in the runtime cases are not clean steady-state numbers. The fixed single iteration includes process startup, benchmark fixture setup, thread creation, and first-touch effects. Use these values as a canary only.
- `BM_RuntimeExternalStart/8192` remains the noisiest sampled runtime path because it exercises external task submission and wake behavior; it is the right place to watch for extra syscalls or wake storms.

## Pressure Result

- Selected tests repeated until failure: 47/47 passed.
- Repeat count: 5.
- Stress duration per `RuntimeStressTests.ConcurrentInitShutdownAndStartTask` run: 5 seconds.
- Total test time: 25.03 seconds.
- Exit status: 0.

The pressure run covered concurrent init/shutdown/start_task, StopImmediately pending-IO cleanup/restart behavior, epoll readiness/cancel/timeout/lifecycle helpers, stream IO, and datagram IO.

## Follow-Up Work

- Add a dedicated long-running perf benchmark mode that initializes the runtime once and loops inside the benchmark body. That would remove startup noise and make cache-miss and branch-miss comparisons more meaningful.
- Keep CI baseline checks focused on runtime key paths, but keep IO benchmarks out of strict CI regression gates unless the runner provides stable IO characteristics.
- For future performance commits, collect both fixed-iteration perf counters and normal benchmark regression output. The former catches microarchitectural surprises; the latter catches user-visible throughput regressions.

## 2026-06-02 Thread Layout API Migration Validation

This run validates the enum-thread to `thread_layout` migration on the requested remote Linux host with `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.0`.

Changes under validation:

- Runtime traits now provide `static constexpr auto threads = af::thread_layout(...)`; the old `using Thread`, manual `thread_count`, and traits `thread_kind()` path has been removed from the active runtime config.
- Thread ids are compact layout-derived `uint16_t` indexes. `thread_group<Tag>().at()` / `.shard()` compute targets with inline arithmetic and do not add an allocation, virtual dispatch, or pointer-chasing step to task handoff.
- Thread group kind/name metadata is kept in a compile-time table for executor setup/introspection, and POSIX executor threads are named `af-<group>-<offset>` with the offset preserved under Linux's 15-character visible-name limit.
- Examples, tests, benchmarks, and README snippets were migrated to layout groups and named thread aliases.

Correctness checks:

- Local macOS Debug full build: passed.
- Local macOS Debug full runtime suite: 166/166 no failures; Linux-only IO tests skipped by platform logic and kqueue tests passed.
- Remote Linux GCC Debug full build: passed.
- Remote Linux GCC Debug full runtime suite: 162/162 no failures; three platform/capability tests skipped by test logic.

## 2026-06-02 Async IO Support Audit And Epoll Wake Fix

This run validates the async IO audit fixes on the requested remote Linux host
with `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.0`.

Changes under validation:

- `runtime_executor_epoll_backend.hpp` now retries eventfd wake writes on
  `EINTR` and clears the pending-wake marker if the write truly fails.
- `ThreadKind::IoUring` readiness waits now fall back to epoll if io_uring poll
  submission closes/fails the ring after a wait registration has been created.
- `docs/async_io_support_audit.md` records the supported async IO surface,
  remaining gaps, and validation results.

Correctness checks:

- Local `git diff --check`: passed.
- Remote GCC Release `asyncflow_runtime_tests` build: passed.
- Remote GCC Release targeted IO suite:
  `ctest -R "Io|Uring|Epoll|Kqueue|RuntimeIo"` passed 89/89 selected tests; 3
  platform/capability tests were skipped by test logic.
- Remote GCC Release full runtime suite: 161/161 passed; the same 3
  platform/capability tests were skipped by test logic.

Interpretation:

- The epoll wake path no longer depends on a best-effort eventfd write after
  publishing `io_wake_pending_`.
- The io_uring poll-readiness fast path no longer leaves a stale fd-only wait
  registration if the ring backend fails during submission.
- kqueue and direct fixed-file capability skips remain expected for this Linux
  host/container validation lane.

## 2026-06-02 Framework Detail Directory Layout Split

This run validates the framework header directory split on the requested remote
Linux host with `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.0`.

Changes under validation:

- `.clang-format` was added and all C/C++ files under
  `include`, `tests`, `examples`, and `benchmarks` were formatted.
- `include/af/detail` now keeps only `config.hpp` at the root. Internal
  implementation headers moved into responsibility directories:
  `runtime`, `io/*`, `queue`, `task`, and `memory`.
- Public umbrella headers under `include/af/*.hpp` were kept as the public
  include surface and updated to include the new internal paths.

Correctness checks:

- Local `git diff --check`: passed.
- Remote GCC Release default build of runtime tests and benchmarks: passed.
- Remote GCC Release full runtime suite: 161/161 passed; 3 platform/capability
  tests were skipped by test logic.
- Local macOS Debug kqueue suite on Darwin 25.5.0 arm64:
  `ctest --test-dir build-local/build/Debug -R Kqueue --output-on-failure`
  passed 5/5.

Interpretation:

- The source tree is now organized by module ownership without changing runtime
  behavior, queue algorithms, IO semantics, or public umbrella headers.
- The validation build covered both runtime tests and benchmark translation
  units after the include-path move.
- The Linux kqueue placeholder skip is covered by the local macOS kqueue lane;
  the two remaining Linux skips are io_uring direct descriptor capability probes.

## 2026-06-01 Scheduler Correctness Pass

This run validates the scheduler modularity/correctness pass on the requested remote Linux host with `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2`.

Builds:

- Debug clang: `asyncflow_runtime_tests`.
- Debug clang + ThreadSanitizer: `asyncflow_runtime_tests` with `ASYNCFLOW_ENABLE_TSAN=ON`.
- Release clang: `asyncflow_runtime_tests` and `asyncflow_runtime_benchmarks`.

Correctness and race checks:

- Remote clang Debug targeted scheduler/parallel tests: 16/16 passed.
- Remote clang TSAN targeted scheduler/parallel tests: 16/16 passed, no TSAN report.
- Remote clang Release targeted scheduler/parallel tests: 16/16 passed.
- Remote clang Release full runtime suite: 132/132 passed; 21 platform/io_uring capability tests were skipped by test logic.
- Remote clang Release `BM_RuntimeParallelShards/128` timeout pressure loop: 100/100 iterations completed under `timeout 10s`.

Release benchmark snapshot after the ready-source clear/recheck optimization:

| Case | Time | CPU | Throughput |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 7.19 ms | 7.18 ms | 1.139 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 12.3 ms | 4.81 ms | 664.567 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.64 ms | 4.63 ms | 1.765 M/s |
| `BM_RuntimeParallelShards/128` | 0.542 ms | 0.521 ms | 236.041 k/s |
| `BM_RuntimeParallelShards/512` | 1.90 ms | 1.85 ms | 269.002 k/s |

Interpretation:

- The pass did not introduce deadlock in the targeted owner-resume or cross-thread hop paths under Debug, TSAN, Release, or the 100-iteration timeout loop.
- The TSAN run specifically covers repeated cross-thread SPSC hops, above-64-thread ready-source words, and parallel shard owner resume races.
- The benchmark values are fixed-iteration canaries. They are suitable for detecting gross regressions in this pass, not as a stable long-term microarchitectural baseline.

## 2026-06-01 Executor Fragment Split Validation

A follow-up mechanical split moved `Executor::pop_one()` into `runtime_executor_pop_fragment.hpp` and `finish_done()` / `finish_pending()` / `finish_again()` into `runtime_executor_finish_fragment.hpp`. `runtime_executor_core_state_fragment.hpp` now owns only executor field layout. No scheduling semantics or benchmark-targeted hot-path logic changed in this split.

Validation:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release targeted scheduler/parallel tests: 16/16 passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug targeted scheduler/parallel tests: 16/16 passed.
- Remote clang TSAN targeted scheduler/parallel tests: 16/16 passed with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 132/132 passed; 21 platform/io_uring capability tests were skipped by test logic.

## 2026-06-01 Executor Task Fragment Split Validation

A follow-up mechanical split turned `runtime_executor_task_fragment.hpp` into a small umbrella over ready-source/wake signaling, local queue push/pop, and execute/result dispatch fragments. The split preserves the same public/private class scope and does not change queue choice, task state transitions, memory ordering, or wake behavior.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release targeted scheduler/parallel tests: 16/16 passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug targeted scheduler/parallel tests: 16/16 passed.
- Remote clang TSAN targeted scheduler/parallel tests: 16/16 passed with no ThreadSanitizer report.
- Remote clang Release full runtime suite: 132/132 passed; 21 platform/io_uring capability tests were skipped by test logic.

Release benchmark canary after the task-fragment split:

| Case | Time | CPU | Throughput |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 7.56 ms | 7.54 ms | 1.083 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 12.3 ms | 4.84 ms | 668.570 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.65 ms | 4.63 ms | 1.763 M/s |
| `BM_RuntimeParallelShards/128` | 0.507 ms | 0.486 ms | 252.415 k/s |
| `BM_RuntimeParallelShards/512` | 1.93 ms | 1.86 ms | 265.746 k/s |

## 2026-06-01 BasicTask Fragment Split Validation

Status: superseded by the BasicTask de-fragmenting pass later on 2026-06-01. The split validated here was mechanically correct, but the class-body `#include` splice style is no longer the desired modularity pattern.

A follow-up mechanical split turned `basic_task_fragment.hpp` into a class shell over public task API, protected helper API, lifetime reference handling, scheduling/wake state-machine logic, and storage layout fragments. The task storage fields remain together in `basic_task_storage_fragment.hpp`; task lifecycle and scheduling code remain inline/template-visible.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release task/scheduler/parallel targeted tests: 24/24 passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug task/scheduler/parallel targeted tests: 24/24 passed.
- Remote clang TSAN task/scheduler/parallel targeted tests: 24/24 passed with no ThreadSanitizer report.
- Remote clang Release full runtime suite: 132/132 passed; 21 platform/io_uring capability tests were skipped by test logic.

Release benchmark canary after the `BasicTask` split, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 7.23 ms | 7.22 ms | 1.138 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 12.8 ms | 4.50 ms | 643.541 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.44 ms | 4.43 ms | 1.845 M/s |
| `BM_RuntimeParallelShards/128` | 0.519 ms | 0.511 ms | 246.555 k/s |
| `BM_RuntimeParallelShards/512` | 1.97 ms | 1.94 ms | 261.281 k/s |

## 2026-06-01 IO Common Split And Epoll Delete Race Fix

This run validates the IO common helper split and the epoll readiness correctness fix on the requested remote Linux host with `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2`.

Changes under validation:

- `io_common_detail_state_fragment.hpp` is now a small umbrella over target/socket-name helpers, readiness wait arming, wait-state helpers, io_uring status normalization, and iovec validation fragments.
- The deferred epoll delete path was removed. `EPOLL_CTL_DEL` now runs on the IO thread before the task is woken, so user code cannot close or reuse the numeric fd before the runtime removes the old epoll interest.
- The stale readiness rearm hint was removed from `IoOpState`. After deleting the epoll interest before wakeup, keeping the hint would make the next wait try `EPOLL_CTL_MOD` first and then fall back to `ADD`, adding an avoidable failed syscall to the hot rearm path.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release targeted IO tests: 82/82 passed; unsupported local platform/io_uring cases were skipped by test logic.
- Remote clang Debug targeted IO tests: 82/82 passed.
- Remote clang TSAN targeted IO tests: 82/82 passed, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 132/132 passed; 21 platform/io_uring capability tests were skipped by test logic.

Release runtime benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 7.17 ms | 7.16 ms | 1.144 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 12.4 ms | 4.56 ms | 665.491 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.41 ms | 4.40 ms | 1.860 M/s |
| `BM_RuntimeParallelShards/128` | 0.521 ms | 0.498 ms | 248.966 k/s |
| `BM_RuntimeParallelShards/512` | 1.82 ms | 1.77 ms | 281.439 k/s |

Existing IO adapter benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean |
| --- | ---: | ---: |
| `BM_IoDatagramAdapterZeroByteRecv` | 0.650 ns | 0.650 ns |
| `BM_IoFileAdapterZeroByteRead` | 0.628 ns | 0.627 ns |
| `BM_IoTimeoutInvalidDelay` | 0.837 ns | 0.836 ns |
| `BM_IoStreamAdapterZeroByteSend` | 0.628 ns | 0.627 ns |
| `BM_IoSendfileZeroCount` | 0.627 ns | 0.627 ns |
| `BM_IoSpliceZeroCount` | 0.628 ns | 0.627 ns |
| `BM_IoStreamAdapterZeroIovSendv` | 0.629 ns | 0.629 ns |

Interpretation:

- The TSAN failure observed before the fix was a close-vs-`epoll_ctl(DEL)` race caused by publishing readiness before deferred delete cleanup. The new ordering removes that race and avoids fd-number reuse hazards from stale epoll interest.
- Removing the rearm hint keeps the correctness fix from adding an avoidable `MOD -> ADD` syscall pair on normal rearm after readiness.
- The existing IO adapter benchmarks cover helper-level fast paths. Live native
  readiness-loop canaries now exist as `BM_LiveEpollReadinessRearm` and
  `BM_LiveKqueueReadinessRearm`, which should be used before treating readiness
  syscall-rate changes as meaningful.

## 2026-06-01 Runtime Common-State Split Validation

Status: superseded by the runtime core de-fragmenting correction later on 2026-06-01. The validation below remains useful as historical correctness/performance evidence, but class-scope fragment splicing is no longer the target modularity pattern.

This run validated the split of `runtime_common_fragment.hpp` into focused class-scope fragments and the removal of the now-obsolete `io_deferred_delete_reserve` public tuning knob.

Changes under validation:

- `runtime_common_fragment.hpp` is now a small umbrella over runtime status, cache-line atomic wrapper, ordered-batch state, parallel-group state, and external-post counter fragments.
- The `io_deferred_delete_reserve` trait/public config was removed because the epoll deferred-delete backend state was removed in the preceding correctness fix.
- README tuning guidance now documents only active IO reserve knobs: `io_wait_reserve` and `io_uring_provided_buffer_group_reserve`.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release runtime/scheduler/parallel targeted tests: 39/39 passed.
- Remote clang Debug runtime/scheduler/parallel targeted tests: 39/39 passed.
- Remote clang TSAN runtime/scheduler/parallel targeted tests: 39/39 passed, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 132/132 passed; 21 platform/io_uring capability tests were skipped by test logic.

Release runtime benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 6.91 ms | 6.91 ms | 1.197 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 11.9 ms | 4.47 ms | 689.139 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.44 ms | 4.43 ms | 1.846 M/s |
| `BM_RuntimeParallelShards/128` | 0.477 ms | 0.464 ms | 268.859 k/s |
| `BM_RuntimeParallelShards/512` | 2.03 ms | 1.96 ms | 253.068 k/s |

Interpretation:

- The split is structural and keeps all common runtime types inside `AsyncRuntime` class scope, so hot paths remain inline/template-visible.
- The removed tuning knob no longer controlled any storage reservation after deferred epoll delete cleanup was deleted. Removing it avoids a misleading no-op API while preserving active IO reserve knobs.

## 2026-06-01 Runtime Stress Source Split Validation

This run validates the test-structure split that removed the old combined `tests/runtime_stress_tests.cpp`. Runtime stress cases are now split by concern into lifecycle, cross-thread hop, and parallel shard sources, with reusable state machines in support headers.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release `RuntimeStressTests`: 4/4 passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug `RuntimeStressTests`: 4/4 passed.
- Remote clang TSAN `RuntimeStressTests`: 4/4 passed with no ThreadSanitizer report.
- Remote clang Release full runtime suite: 132/132 passed; 21 platform/io_uring capability tests were skipped by test logic.

Interpretation:

- This pass changed only test layout and CMake source registration. It did not change runtime scheduling, IO paths, queue selection, memory ordering, or wake behavior.
- The split keeps lifecycle shutdown pressure, cross-thread SPSC hop pressure, above-64-thread ready-source coverage, and parallel owner-resume pressure independently addressable.

## 2026-06-01 UDP Datagram Test Helper Split Validation

This run validates the test support split that moved repeated UDP loopback setup out of datagram test bodies and into `tests/support/runtime_io_udp_socket_helpers_fragment.hpp`.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release datagram-targeted tests: 14/14 passed, all skipped by local platform/backend logic.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug datagram-targeted tests: 14/14 passed.
- Remote clang TSAN datagram-targeted tests: 14/14 passed with no ThreadSanitizer report.
- Remote clang Release full runtime suite: 132/132 passed; 21 platform/io_uring capability tests were skipped by test logic.

Interpretation:

- This pass changed only test fixture setup. Runtime scheduling, IO backend behavior, queue selection, memory ordering, and public APIs were unchanged.
- The split makes epoll and io_uring datagram tests share one UDP loopback fixture helper, reducing duplicated socket lifecycle setup in the test bodies.

## 2026-06-01 Stream Transfer Test Helper Split Validation

This run validates the test support split that moved repeated sendfile/splice fixture setup out of `tests/runtime_io_stream_transfer_tests.cpp` and into `tests/support/runtime_io_stream_transfer_helpers_fragment.hpp`.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release sendfile/splice-targeted tests: 5/5 passed, all skipped by local platform/backend logic.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug sendfile-targeted tests: 4/4 passed; 2 io_uring-poll capability tests were skipped by test logic.
- Remote clang Debug splice-targeted tests: 1/1 passed.
- Remote clang TSAN sendfile-targeted tests: 4/4 passed with no ThreadSanitizer report; 2 io_uring-poll capability tests were skipped by test logic.
- Remote clang TSAN splice-targeted tests: 1/1 passed with no ThreadSanitizer report.
- Remote clang Release full runtime suite: 132/132 passed; 21 platform/io_uring capability tests were skipped by test logic.

Interpretation:

- This pass changed only test fixture setup and cleanup ownership. Runtime scheduling, IO backend behavior, queue selection, memory ordering, and public APIs were unchanged.
- The split keeps sendfile/splice behavior assertions in the test source while moving temporary files, blocked sockets, and pipe pairs into reusable support helpers.

## 2026-06-01 Scheduler Stress Support Split Validation

This run validates the support-header split that turned `tests/support/runtime_scheduler_stress_support.hpp` into an umbrella over focused stress scenario fragments.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release `RuntimeStressTests`: 4/4 passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug `RuntimeStressTests`: 4/4 passed.
- Remote clang TSAN `RuntimeStressTests`: 4/4 passed with no ThreadSanitizer report.
- Remote clang Release full runtime suite: 132/132 passed; 21 platform/io_uring capability tests were skipped by test logic.

Interpretation:

- This pass changed only test support layout. Runtime scheduling, queue choice, task state transitions, memory ordering, and public APIs were unchanged.
- The split gives repeat-hop, above-64-thread ready-source, and parallel owner-resume stress state machines separate ownership boundaries while preserving the same strict stress tests.

## 2026-06-01 Length-Prefixed RPC Server Example Split Validation

This run validates the example support split that turned `examples/support/io_rpc_length_prefixed_server.hpp` into an umbrella over focused server/process task fragments.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_io_rpc_length_prefixed_example` build: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug `asyncflow_io_rpc_length_prefixed_example` build/run: passed with `rpc response_ok=1`.
- Remote clang TSAN `asyncflow_io_rpc_length_prefixed_example` build/run: passed with `rpc response_ok=1` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_rpc_length_prefixed_example` build/run: passed with `rpc response_ok=1`.

Interpretation:

- This pass changed only example support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, and public APIs were unchanged.
- The split separates the IO-thread accept/read/write server state machine from the logic-thread request processing task while preserving the explicit `rpc_async::post(RpcThreads::IO_0, server_)` handoff.

## 2026-06-01 io_uring Fixed-File Task Split Validation

This run validates the example support split that turned `examples/support/io_uring_fixed_file_task.hpp` into a small task shell over flow, IO-operation, and registration/update fragments.

Status: superseded by the example task de-fragmenting validation later on 2026-06-01. The split validated here was mechanically correct, but the class-body `#include` splice style is no longer the desired modularity pattern.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_io_uring_fixed_file_example` build: passed; local run reported `io_uring fixed file example is Linux-only`.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug `asyncflow_io_uring_fixed_file_example` build/run: passed with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_io_uring_fixed_file_example` build/run: passed with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_uring_fixed_file_example` build/run: passed with `io_uring backend unavailable`.

Interpretation:

- This pass changed only example support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, and public APIs were unchanged.
- The remote host/container combination did not expose the io_uring fixed-file backend path for this example, so this validates build/link/run and TSAN startup/teardown cleanliness rather than the registered-file data path itself.

## 2026-06-01 io_uring File-Lifecycle Task Split Validation

This run validates the example support split that turned `examples/support/io_uring_file_lifecycle_task.hpp` into a small task shell over flow, file-operation, and namespace-operation fragments.

Status: superseded by the example task de-fragmenting validation later on 2026-06-01. The split validated here was mechanically correct, but the class-body `#include` splice style is no longer the desired modularity pattern.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_io_uring_file_lifecycle_example` build: passed; local run reported `io_uring lifecycle example is Linux-only`.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug `asyncflow_io_uring_file_lifecycle_example` build/run: passed with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_io_uring_file_lifecycle_example` build/run: passed with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_uring_file_lifecycle_example` build/run: passed with `io_uring backend unavailable`.

Interpretation:

- This pass changed only example support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, and public APIs were unchanged.
- The remote host/container combination did not expose the io_uring backend path for this example, so this validates build/link/run and TSAN startup/teardown cleanliness rather than the open/fallocate/read/write/stat/rename/unlink data path itself.

## 2026-06-01 io_uring UDP Recvmsg Multishot Task Split Validation

This run validates the example support split that turned `examples/support/io_uring_udp_recvmsg_multishot_task.hpp` into a small task shell over flow, provided-buffer ring, and recvmsg multishot fragments.

Status: superseded by the example task de-fragmenting validation later on 2026-06-01. The split validated here was mechanically correct, but the class-body `#include` splice style is no longer the desired modularity pattern.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_io_uring_udp_recvmsg_multishot_example` build: passed; local run reported `io_uring UDP recvmsg_multishot example is Linux-only`.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug `asyncflow_io_uring_udp_recvmsg_multishot_example` build/run: passed with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_io_uring_udp_recvmsg_multishot_example` build/run: passed with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_uring_udp_recvmsg_multishot_example` build/run: passed with `io_uring backend unavailable`.

Interpretation:

- This pass changed only example support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, and public APIs were unchanged.
- The remote host/container combination did not expose the io_uring backend path for this example, so this validates build/link/run and TSAN startup/teardown cleanliness rather than the provided-buffer recvmsg multishot data path itself.

## 2026-06-01 Length-Prefixed RPC Client Example Split Validation

This run validates the example support split that turned `examples/support/io_rpc_length_prefixed_client.hpp` into a small task shell over flow, request-send, and response-read fragments.

Status: superseded by the example task de-fragmenting validation later on 2026-06-01. The split validated here was mechanically correct, but the class-body `#include` splice style is no longer the desired modularity pattern.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_io_rpc_length_prefixed_example` build: passed; local run reported `rpc length-prefixed example is Linux-only`.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug `asyncflow_io_rpc_length_prefixed_example` build/run: passed with `rpc length-prefixed backend=epoll-fallback` and `rpc response_ok=1`.
- Remote clang TSAN `asyncflow_io_rpc_length_prefixed_example` build/run: passed with `rpc response_ok=1` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_rpc_length_prefixed_example` build/run: passed with `rpc response_ok=1`.

Interpretation:

- This pass changed only example support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, and public APIs were unchanged.
- Unlike the io_uring-only examples whose remote runs reported `io_uring backend unavailable`, this example completed the full TCP RPC path through the epoll fallback backend in Debug, TSAN, and Release.
- The remote build log showed a short clock-skew warning after rsync because modified file timestamps were a few seconds ahead of the container clock; each configuration still rebuilt the target and completed the RPC round trip.

## 2026-06-01 io_uring UDP Recv Multishot Task Split Validation

This run validates the example support split that turned `examples/support/io_uring_udp_recv_multishot_task.hpp` into a small task shell over flow, provided-buffer ring, and recv multishot fragments.

Status: superseded by the example task de-fragmenting validation later on 2026-06-01. The split validated here was mechanically correct, but the class-body `#include` splice style is no longer the desired modularity pattern.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_io_uring_udp_recv_multishot_example` build: passed; local run reported `io_uring UDP recv_multishot example is Linux-only`.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug `asyncflow_io_uring_udp_recv_multishot_example` build/run: passed with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_io_uring_udp_recv_multishot_example` build/run: passed with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_uring_udp_recv_multishot_example` build/run: passed with `io_uring backend unavailable`.

Interpretation:

- This pass changed only example support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, and public APIs were unchanged.
- The remote host/container combination did not expose the io_uring backend path for this example, so this validates build/link/run and TSAN startup/teardown cleanliness rather than the provided-buffer recv multishot data path itself.

## 2026-06-01 io_uring Stream Recv Multishot Task Split Validation

This run validates the example support split that turned `examples/support/io_uring_recv_multishot_task.hpp` into a small task shell over flow, provided-buffer ring, and recv multishot fragments.

Status: superseded by the example task de-fragmenting validation later on 2026-06-01. The split validated here was mechanically correct, but the class-body `#include` splice style is no longer the desired modularity pattern.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_io_uring_recv_multishot_example` build: passed; local run reported `io_uring recv_multishot example is Linux-only`.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug `asyncflow_io_uring_recv_multishot_example` build/run: passed with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_io_uring_recv_multishot_example` build/run: passed with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_uring_recv_multishot_example` build/run: passed with `io_uring backend unavailable`.

Interpretation:

- This pass changed only example support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, and public APIs were unchanged.
- The remote host/container combination did not expose the io_uring backend path for this example, so this validates build/link/run and TSAN startup/teardown cleanliness rather than the provided-buffer stream recv multishot data path itself.

## 2026-06-01 Example Task De-Fragmenting Validation

This run validates the cleanup that removes class-body `#include` splicing from the example task classes that were previously split into flow/request/response/ring/IO method fragments.

Changes under validation:

- `FixedFileRoundTripTask`, `FileLifecycleTask`, `FilesystemOpsTask`, `RecvMultishotTask`, `UdpRecvMultishotTask`, `UdpRecvmsgMultishotTask`, and `RpcClientTask` now keep each task state machine in one cohesive class definition.
- Removed 21 fragment headers that existed only to splice private methods into those seven class bodies.
- `io_rpc_length_prefixed_server.hpp` remains a namespace-level composition of process task declaration, server task, and process task implementation; it is not a class-body splice.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug build passed for `asyncflow_io_rpc_length_prefixed_example`, `asyncflow_io_uring_file_lifecycle_example`, `asyncflow_io_uring_filesystem_ops_example`, `asyncflow_io_uring_fixed_file_example`, `asyncflow_io_uring_recv_multishot_example`, `asyncflow_io_uring_udp_recv_multishot_example`, and `asyncflow_io_uring_udp_recvmsg_multishot_example`.
- Remote clang Debug run under `--security-opt seccomp=unconfined` passed for all seven executables.
- Runtime outputs confirmed the exercised paths: `rpc response_ok=1`, lifecycle/statx size `1`, fixed-file `byte=F vectored=IO updated=U`, stream recv multishot `MR`, UDP recv multishot `UM`, and UDP recvmsg multishot `RM peers=2`.

Interpretation:

- This pass changed only example support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, public APIs, task field layout, and example assertions were unchanged.
- No Release benchmark was run for this pass because no production runtime path or benchmarked helper path changed.

## 2026-06-01 io_uring File Lifecycle Test Support Split Validation

This run validates the test support split that turned `tests/support/runtime_io_file_lifecycle_tasks_fragment.hpp` into a 63-line task shell over flow, file-operation, and namespace-operation fragments.

Status: superseded by the io_uring file test support de-fragmenting validation later on 2026-06-01. The split validated here was mechanically correct, but the class-body `#include` splice style is no longer the desired modularity pattern.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed; the targeted local run reported `io_uring backend is Linux-only`.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug `asyncflow_runtime_tests` targeted run: skipped by test logic with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_runtime_tests` targeted run: skipped by test logic with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_runtime_tests` targeted run: skipped by test logic with `io_uring backend unavailable`.

Interpretation:

- This pass changed only test support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, public APIs, and test assertions were unchanged.
- The remote host/container combination did not expose the io_uring backend path for this test, so this validates build/link/run and TSAN startup/teardown cleanliness rather than the open/fallocate/read/write/stat/rename/unlink data path itself.

## 2026-06-01 io_uring Fixed-File Read/Write Test Support Split Validation

This run validates the test support split that turned `tests/support/runtime_io_file_fixed_file_rw_tasks_fragment.hpp` into a 53-line task shell over flow, registration, and IO-operation fragments.

Status: superseded by the io_uring file test support de-fragmenting validation later on 2026-06-01. The split validated here was mechanically correct, but the class-body `#include` splice style is no longer the desired modularity pattern.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed; the targeted local run reported `io_uring backend is Linux-only`.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug `asyncflow_runtime_tests` targeted run: skipped by test logic with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_runtime_tests` targeted run: skipped by test logic with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_runtime_tests` targeted run: skipped by test logic with `io_uring backend unavailable`.

Interpretation:

- This pass changed only test support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, public APIs, task field layout, and test assertions were unchanged.
- The remote host/container combination did not expose the io_uring backend path for this test, so this validates build/link/run and TSAN startup/teardown cleanliness rather than the registered fixed-file fixed-buffer/vectored read-write data path itself.

## 2026-06-01 io_uring Recv Multishot Test Support Split And Backend Diagnosis

This run validates the test support split that turned `tests/support/runtime_io_uring_socket_recv_multishot_tasks_fragment.hpp` into an 84-line task shell over flow, provided-buffer ring, and recv/cancel fragments. It also investigates why the remote io_uring-only tests skip.

Status: superseded by the io_uring filesystem/multishot test support de-fragmenting validation later on 2026-06-01. The split validated here was mechanically correct, but the class-body `#include` splice style is no longer the desired modularity pattern.

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed; the targeted local run reported Linux-only skips.
- Initial remote diagnosis with host `kernel.io_uring_disabled = 2`: `IoUringBackendAvailabilityReportsSetupError` reported `error=1 (Operation not permitted)`, and direct `io_uring_setup` probes returned `EPERM`.
- After temporarily enabling the host with `kernel.io_uring_disabled = 0`, host-root direct `io_uring_setup` succeeded.
- The default Docker seccomp profile still denied `io_uring_setup` with `EPERM`.
- `--security-opt seccomp=unconfined` allowed direct `io_uring_setup` and allowed runtime io_uring tests to execute the actual backend path.
- Remote clang Debug `asyncflow_runtime_tests --gtest_filter=*Uring*.*` with `seccomp=unconfined`: 35/37 passed, 2 skipped for unsupported direct-descriptor capabilities, 0 failed.
- Remote clang TSAN `asyncflow_runtime_tests --gtest_filter=*Uring*.*` with `seccomp=unconfined`: 35/37 passed, 2 skipped, no ThreadSanitizer report.
- Remote clang Release `asyncflow_runtime_tests --gtest_filter=*Uring*.*` with `seccomp=unconfined`: 35/37 passed, 2 skipped, 0 failed.
- The first broad Debug run exposed two failing io_uring poll-readiness sendfile tests. The tests were using a full AF_UNIX socketpair for `sendfile`; after switching them to the same full TCP connection shape used by the epoll sendfile wait test, both poll-readiness and cancel cases passed in Debug, TSAN, and Release.

Remote io_uring diagnosis:

- The remote kernel has io_uring compiled in: `CONFIG_IO_URING=y`.
- Before temporary host enablement, host sysctl reported `kernel.io_uring_disabled = 2` and `kernel.io_uring_group = -1`; host root, default container, `seccomp=unconfined` container, and `--privileged` container direct `io_uring_setup` probes all returned `EPERM`.
- After temporary host enablement, host sysctl reported `kernel.io_uring_disabled = 0`, host-root direct `io_uring_setup` succeeded, `seccomp=unconfined` and `--privileged` containers succeeded, and only the default Docker seccomp profile continued to return `EPERM`.
- The remaining container requirement is therefore Docker seccomp policy, not kernel support and not runtime skip logic.

How to enable the remote io_uring path:

- Temporary host enablement: `sysctl -w kernel.io_uring_disabled=0`.
- Persistent host enablement: write `kernel.io_uring_disabled = 0` to a file under `/etc/sysctl.d/`, then reload with `sysctl --system`.
- Verification order after enabling: first run a host-root direct `io_uring_setup` probe, then run the same probe in the default container, then run the runtime target tests.
- If the host probe succeeds but the default container probe still returns `EPERM`, run the container with `--security-opt seccomp=unconfined` or a custom seccomp profile that allows `io_uring_setup`, `io_uring_enter`, and `io_uring_register`.
- Restore the current restricted host behavior with `sysctl -w kernel.io_uring_disabled=2` if the remote should remain locked down.

Interpretation:

- The earlier skip was not caused by the runtime silently choosing to skip a usable backend. `AsyncRuntime::io_uring_backend_error(thread)` exposed the setup errno as `EPERM`, which matched direct syscall probes.
- To run actual remote io_uring data paths now that the host is enabled, use Docker with `--security-opt seccomp=unconfined` or an equivalent custom seccomp profile that allows `io_uring_setup`, `io_uring_enter`, and `io_uring_register`.
- This pass changed diagnostics, test support layout, and the sendfile poll-readiness test fixture shape. It does not add locks, alter queue selection, change task field order, or affect the available-backend fast path.

## 2026-06-01 Full Runtime Matrix With io_uring Enabled

After the host was temporarily enabled with `kernel.io_uring_disabled = 0`, the full runtime test binary was rerun in the clang container with `--security-opt seccomp=unconfined`.

Results:

- Remote clang Debug `asyncflow_runtime_tests`: 130/133 passed, 3 skipped, 0 failed.
- Remote clang TSAN `asyncflow_runtime_tests`: 130/133 passed, 3 skipped, no ThreadSanitizer report.
- Remote clang Release `asyncflow_runtime_tests`: 130/133 passed, 3 skipped, 0 failed.

Skipped tests:

- `IoRuntimeKqueue.KqueueBackendIsPlatformSpecific`: expected Linux skip.
- `UringIoRuntimeSocketAcceptFixture.IoUringAcceptDirectReceivesThroughFixedFile`: direct accept unsupported by the current kernel/runtime capability check.
- `UringIoRuntimeFileFixture.IoUringOpenAtDirectInstallsFixedFileSlot`: direct descriptor open unsupported with error 22.

Interpretation:

- This is the strongest current remote correctness signal: scheduler, epoll fallback, actual io_uring socket/file/multishot paths, runtime stress tests, above-64-thread ready-source coverage, and shutdown/restart cases all passed under Debug, TSAN, and Release.
- The default container profile still blocks io_uring; `--security-opt seccomp=unconfined` remains required unless a custom seccomp profile allows the three io_uring syscalls.

## 2026-06-01 Explicit Ready Queue Route And Self-Post Stress

This run validates the scheduler hot-path change that makes runtime-thread ready enqueue routing explicit: same-owner posts use `ReadyQueueRoute::Local`, while cross-owner posts use `ReadyQueueRoute::Spsc`.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang Debug targeted scheduler stress tests: 5/5 passed.
- Remote clang TSAN targeted scheduler stress tests: 5/5 passed, no ThreadSanitizer report.
- Remote clang Release targeted scheduler stress tests: 5/5 passed.
- Remote clang Release full runtime test suite: 135 total, 132 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 7.08 ms | 7.07 ms | 1.181 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 11.6 ms | 4.42 ms | 705.325 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.23 ms | 4.23 ms | 1.936 M/s |
| `BM_RuntimeParallelShards/128` | 0.500 ms | 0.480 ms | 256.161 k/s |

Interpretation:

- The new single-thread fanout stress test would strand work if same-thread runtime posts were accidentally routed through the self SPSC queue instead of the executor local FIFO.
- The `again()` stress test covers the `finish_again()` self-reschedule path and verifies it also drains through local queue behavior without relying on cross-thread ready hints.
- The route naming change adds no locks, no heap allocation, no additional queue type, and no executor state-layout changes.

## 2026-06-01 Ready-Source Word Cursor Rotation

This run validates the follow-up scheduler hot-path change that rotates multi-word ready-source hint scans. The executor no longer starts every ready-source pass at word 0 when `thread_count` spans more than one 64-bit ready-source word.

Changes under validation:

- `runtime_executor_pop_fragment.hpp` advances an executor-private ready-word cursor after a successful hinted SPSC pop.
- `runtime_executor_core_state_fragment.hpp` stores the cursor next to `next_source_` as a `std::uint16_t`.
- Ready-source bits remain fast hints only; the bounded all-source SPSC fallback scan remains the correctness backstop.
- The change adds no locks, no atomics, no heap allocation, and no queue topology change.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug targeted scheduler stress tests: 5/5 passed.
- Remote clang TSAN targeted scheduler stress tests: 5/5 passed, no ThreadSanitizer report.
- Remote clang Release targeted scheduler stress tests: 5/5 passed.
- Remote clang Release full runtime test suite: 135 total, 132 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 6.88 ms | 6.87 ms | 1.194 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 11.6 ms | 4.28 ms | 709.526 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.22 ms | 4.22 ms | 1.940 M/s |
| `BM_RuntimeParallelShards/128` | 0.541 ms | 0.529 ms | 237.390 k/s |

Interpretation:

- The rotation addresses the remaining multi-word ready-source scan bias without weakening the local-queue-vs-SPSC separation added in the previous pass.
- TSAN and the above-64 scheduler stress coverage did not expose races or stranded work after adding the cursor.
- The current benchmark canary is sufficient for gross regression detection. A future perf-counter run with 128+/256+ configured worker threads would be needed before making claims about large-thread-count cache and branch behavior.

## 2026-06-01 Thread-Count Config Boundary Validation

This run validates a configuration correctness fix for `AsyncRuntime<Traits>::thread_count`. The runtime now validates the raw trait value before narrowing it to the public 16-bit thread-index representation.

Changes under validation:

- `runtime_public_config_fragment.hpp` checks `Traits::thread_count > 0` before conversion.
- The same fragment checks `Traits::thread_count <= UINT16_MAX` before conversion, preventing silent wraparound for impossible thread-index counts.
- `tests/runtime_config_tests.cpp` adds compile-time and runtime coverage for a 257-thread runtime config, explicitly covering a value above 64.
- The change does not alter queue topology, scheduler state transitions, atomics, locks, wake behavior, or benchmark hot paths.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug targeted config/scheduler tests: 5/5 passed.
- Remote clang TSAN targeted config/scheduler tests: 5/5 passed, no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 136 total, 133 passed, 3 skipped, 0 failed.

Interpretation:

- The runtime remains explicitly capable of above-64 thread counts; the new 257-thread config test protects that API boundary.
- The only new limit is the existing representation limit implied by `std::uint16_t` thread indexes. It is now an explicit compile-time error instead of silent truncation.

## 2026-06-01 Running-to-Pending Wake Boundary And ObjectPool Split

This run validates the suspected owner-task hang boundary: a task is still Running when another runtime thread posts it, then the task returns Pending. The expected behavior is that the wake request either remains deferred until `finish_pending()` consumes it or wins a direct `Pending -> Queued` transition after the owner publishes Pending.

Changes under validation:

- `runtime_executor_finish_fragment.hpp` documents that `enqueue_pending_blocking()` converts a same-epoch running wake into a queue entry only if the task is still Pending.
- `tests/runtime_running_pending_stress_tests.cpp` adds `RuntimeStressTests.RunningToPendingWakeDoesNotStrandOwner`, which forces the wake request to arrive while the owner is still Running and about to return Pending.
- `object_pool.hpp` was temporarily a shell over storage, slot acquire/release, and lifecycle fragments. That class-body split has since been reverted; `ObjectPool` now stays in one cohesive class definition.
- `tests/pool_tests.cpp` adds concurrent ObjectPool create/destroy coverage.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug targeted Running/Pending, owner-resume, self-post, and pool tests: 6/6 passed.
- Remote clang TSAN targeted Running/Pending, owner-resume, self-post, and pool tests: 6/6 passed, no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 138 total, 135 passed, 3 skipped, 0 failed.

Release queue/pool benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_SpscQueuePushPop/16384` | 19,566 ns | 19,546 ns | 838.619 M/s |
| `BM_MpscQueuePushPop/16384` | 135,152 ns | 135,021 ns | 121.346 M/s |
| `BM_ObjectPoolCreateDestroy/16384` | 39,830 ns | 39,803 ns | 411.638 M/s |

Interpretation:

- The targeted stress test did not reproduce a lost wake or owner hang across Debug, TSAN, or Release. The state-machine audit matches the observed behavior: either the running wake slot is consumed by `finish_pending()`, or a concurrent post observes Pending and performs the `Pending -> Queued` transition itself.
- The ObjectPool split is structural. It preserves the free-list, TLS cache, cache-line slot sizing, and block/hot-block atomics while making storage and lifecycle responsibilities auditable separately.

## 2026-06-01 Executor Lifecycle/Notify Split Validation

This run validates the structural split of `runtime_executor_control_fragment.hpp` into lifecycle and notify/wake-control fragments.

Changes under validation:

- `runtime_executor_control_fragment.hpp` is now a 6-line umbrella.
- `runtime_executor_lifecycle_fragment.hpp` owns constructor/destructor, start, stop request, and join behavior.
- `runtime_executor_notify_fragment.hpp` owns wake publication and IO/native-wake fallback behavior.
- The split does not move executor fields, alter queue topology, change task state transitions, add locks, add allocations, or change the existing atomics in `notify()`.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug targeted runtime/backpressure/shutdown/IO/stress tests: 21/21 passed.
- Remote clang TSAN targeted runtime/backpressure/shutdown/IO/stress tests: 21/21 passed, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 138 total, 135 passed, 3 skipped, 0 failed.

Release runtime benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 7.70 ms | 7.69 ms | 1.064 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 12.6 ms | 4.49 ms | 655.313 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.19 ms | 4.18 ms | 1.957 M/s |
| `BM_RuntimeParallelShards/128` | 0.525 ms | 0.499 ms | 246.320 k/s |

Interpretation:

- The change is a modularity split only. Lifecycle and wake behavior are now separately reviewable while preserving header-only inlining and the existing hot-path branch shape.
- The targeted TSAN run covers the same high-risk wake paths: repeated cross-thread hops, above-64-thread ready-source words, owner resume under parallel shard bursts, the Running-to-Pending wake boundary, self-post local queue routing, shutdown, and epoll wake/cancel/timeout cases.

## 2026-06-01 Public Parallel API Split Validation

This run validates the structural split of `runtime_public_parallel_api_fragment.hpp` into focused public scheduling fragments.

Changes under validation:

- `runtime_public_parallel_api_fragment.hpp` is now a 7-line umbrella.
- `runtime_public_parallel_shard_fragment.hpp` owns `shard_by()` and `split_by_shard()`.
- `runtime_public_parallel_shards_fragment.hpp` owns `parallel_shards()` and `parallel_shards_ordered()` overloads.
- `runtime_public_ordered_start_fragment.hpp` owns `start_ordered_task()` and `ordered_last_applied_batch_id()`.
- The split keeps all templates inline inside `AsyncRuntime`; it does not move state fields, alter queue topology, change atomics, add locks, add allocations, or change public behavior.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug parallel/ordered/shutdown/stress targeted tests: 32/32 passed.
- Remote clang TSAN parallel/ordered/shutdown/stress targeted tests: 32/32 passed, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 138 total, 135 passed, 3 skipped, 0 failed.

Release runtime benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 7.37 ms | 7.36 ms | 1.117 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 13.9 ms | 4.67 ms | 592.463 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.27 ms | 4.27 ms | 1.918 M/s |
| `BM_RuntimeParallelShards/128` | 0.476 ms | 0.458 ms | 268.995 k/s |
| `BM_RuntimeParallelShards/512` | 1.82 ms | 1.76 ms | 280.922 k/s |

Interpretation:

- The split separates data partitioning APIs from parallel owner-resume orchestration and ordered-start APIs without changing the scheduler state machine.
- The TSAN target covers ordered batch/start flows, parallel shard owner resume, above-64 ready-source words, self-post local queue routing, shutdown behavior, and the Running-to-Pending wake boundary.

## 2026-06-01 Socket Accept/Connect Submit Split Validation

This run validates the structural split of socket accept/connect submit wrappers on both the public `AsyncRuntime` side and the io_uring executor side.

Changes under validation:

- `runtime_public_io_socket_accept_connect_submit_fragment.hpp` is now a 6-line umbrella over public accept and connect submit fragments.
- `runtime_public_io_socket_accept_submit_fragment.hpp` owns accept, accept-direct, and accept-multishot validation and executor handoff.
- `runtime_public_io_socket_connect_submit_fragment.hpp` owns connect validation and executor handoff.
- `runtime_executor_io_uring_socket_accept_connect_submit_fragment.hpp` is now a 6-line umbrella over executor accept and connect SQE submit fragments.
- `runtime_executor_io_uring_socket_accept_submit_fragment.hpp` owns accept, accept-direct, and accept-multishot SQE submit wrappers.
- `runtime_executor_io_uring_socket_connect_submit_fragment.hpp` owns connect SQE submit wrappers.
- The split keeps all submit wrappers inline in their original class scopes; it does not change queue topology, task state transitions, SQE arguments, atomics, locks, allocations, or fallback behavior.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug accept/connect targeted tests: 13 total, 12 passed, 1 skipped.
- Remote clang TSAN accept/connect targeted tests: 13 total, 12 passed, 1 skipped, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 138 total, 135 passed, 3 skipped, 0 failed.

Interpretation:

- This is a modularity split only. Accept variants stay grouped together because basic accept, direct accept, and multishot accept share the `IORING_OP_ACCEPT` submit family and validation boundary.
- The TSAN target covers epoll accept/connect helpers, io_uring accept/connect paths, accept-multishot unavailable-backend validation, shutdown wait for accepted tasks, and the direct-accept capability skip path.

## 2026-06-01 Fixed-File Resource Split Validation

This run validates the structural split of the io_uring fixed-file table resource helper.

Changes under validation:

- `runtime_executor_io_uring_file_resource_fragment.hpp` is now a 7-line umbrella.
- `runtime_executor_io_uring_file_register_fragment.hpp` owns `IORING_REGISTER_FILES`.
- `runtime_executor_io_uring_file_unregister_fragment.hpp` owns `IORING_UNREGISTER_FILES`.
- `runtime_executor_io_uring_file_update_fragment.hpp` owns `IORING_REGISTER_FILES_UPDATE`.
- The split keeps all helpers inline in `AsyncRuntime::Executor`; it does not move fixed-file table state, change `io_uring_register` arguments, alter pending-submit flush ordering, add locks, add allocations, or change error handling.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug fixed-file/resource targeted tests: 6 total, 4 passed, 2 skipped.
- Remote clang TSAN fixed-file/resource targeted tests: 6 total, 4 passed, 2 skipped, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 138 total, 135 passed, 3 skipped, 0 failed.

Interpretation:

- This is a modularity split only. The registration, unregister, and update paths are now separately auditable while preserving the executor-thread-only ownership model.
- The TSAN target covers fixed-file read/write/fsync, fixed-file table update, registered-buffer IO, unavailable-backend fixed-file boundary handling, and direct descriptor capability skip paths.

## 2026-06-01 Ready Enqueue Split Validation

This run validates the structural split of the scheduler ready-enqueue hot path on the requested remote Linux host with `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2`.

Changes under validation:

- `runtime_ready_enqueue_fragment.hpp` is now an 8-line umbrella.
- `runtime_ready_route_fragment.hpp` owns `ReadyQueueRoute` and same-thread-vs-cross-thread route selection.
- `runtime_ready_try_enqueue_fragment.hpp` owns non-blocking local/SPSC/MPSC enqueue paths.
- `runtime_ready_blocking_enqueue_fragment.hpp` owns yield-policy enqueue loops and local-queue drain behavior.
- `runtime_ready_post_fragment.hpp` owns `post_blocking()`, enqueue policy selection, and `Pending -> Queued` admission.
- The split keeps all helpers inline in `AsyncRuntime`; it does not change queue topology, task state transitions, memory ordering, wake ordering, locks, atomics, or allocation behavior.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang Debug scheduler/runtime targeted tests: 46/46 passed.
- Remote clang TSAN scheduler/runtime targeted tests: 46/46 passed, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 138 total, 135 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 7.00 ms | 7.00 ms | 1.179 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 13.6 ms | 4.60 ms | 603.070 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.28 ms | 4.27 ms | 1.917 M/s |
| `BM_RuntimeParallelShards/128` | 0.505 ms | 0.491 ms | 253.635 k/s |
| `BM_RuntimeParallelShards/512` | 1.83 ms | 1.77 ms | 280.185 k/s |

Interpretation:

- Same-thread runtime posts remain an explicit local-queue route, while cross-thread runtime posts remain an explicit per-source SPSC route.
- The TSAN target covers self-post local FIFO behavior, cross-thread SPSC hops, above-64 ready-source words, shutdown behavior, parallel owner resume, and the Running-to-Pending wake boundary.
- The benchmark canary is within the existing noisy remote range for these scheduler cases and does not show a gross regression from this structural split.

## 2026-06-01 BasicTask Schedule Split Validation

Status: superseded by the BasicTask de-fragmenting pass later on 2026-06-01. The schedule fragment files listed here were removed because they split a single class by private implementation regions rather than by independent data structure, algorithm, function family, or class boundary.

This run validates the structural split of the `BasicTask` scheduling and running-wake state-machine fragment on the requested remote Linux host with `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2`.

Changes under validation:

- `basic_task_schedule_fragment.hpp` is now an 8-line umbrella.
- `basic_task_schedule_constants_fragment.hpp` owns requested-thread/epoch packing constants.
- `basic_task_schedule_state_fragment.hpp` owns `request_schedule()`, cancel rollback, and simple schedule-state accessors.
- `basic_task_running_wake_fragment.hpp` owns run-epoch publication, Running wake registration, request consumption, and Running/Pending resolution.
- `basic_task_requested_thread_fragment.hpp` owns requested-thread clearing and encode/decode helpers.
- The split keeps all helpers inline in `BasicTask`; it does not change `TaskState` transitions, requested-thread atomics, run-epoch ordering, queue routing, locks, allocations, or wake behavior.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang Debug task/scheduler targeted tests: 45/45 passed.
- Remote clang TSAN task/scheduler/config targeted tests: 46/46 passed, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 138 total, 135 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 6.94 ms | 6.94 ms | 1.193 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 13.0 ms | 4.57 ms | 634.102 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.25 ms | 4.24 ms | 1.930 M/s |
| `BM_RuntimeParallelShards/128` | 0.503 ms | 0.488 ms | 256.183 k/s |
| `BM_RuntimeParallelShards/512` | 1.98 ms | 1.93 ms | 260.199 k/s |

Interpretation:

- The Running -> Pending wake boundary is now isolated in `basic_task_running_wake_fragment.hpp`, making the epoch/request-slot logic easier to audit without changing it.
- The TSAN target covers Running wake resolution, self-post local FIFO behavior, cross-thread SPSC hops, above-64 ready-source words, shutdown behavior, and parallel owner resume.
- The benchmark canary remains within the same noisy remote range as recent scheduler-structure splits and does not show a gross regression from this mechanical split.

## 2026-06-01 Generic io_uring SQE Fill Split Validation

This run validates the structural split of the generic io_uring SQE fill helper on the requested remote Linux host with `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2`.

Changes under validation:

- `runtime_executor_io_uring_generic_submit_sqe_fragment.hpp` is now an 8-line umbrella.
- `runtime_executor_io_uring_generic_submit_sqe_dispatch_fragment.hpp` owns common SQE initialization and opcode-family dispatch.
- `runtime_executor_io_uring_generic_submit_sqe_filesystem_fragment.hpp` owns fallocate, splice, openat, statx, renameat, and unlinkat SQE fields.
- `runtime_executor_io_uring_generic_submit_sqe_socket_fragment.hpp` owns message, accept, connect, and socket data SQE fields.
- `runtime_executor_io_uring_generic_submit_sqe_buffer_fragment.hpp` owns fixed-buffer and generic buffer SQE fields.
- The split keeps all helpers inline in `AsyncRuntime::Executor`; it does not change opcode classification, SQE field values, operation lifetime, pending-submit flush ordering, locks, allocations, or fallback behavior.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang Debug IO/io_uring targeted tests: 83 total, 81 passed, 2 skipped.
- Remote clang TSAN IO/io_uring targeted tests: 83 total, 81 passed, 2 skipped, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 138 total, 135 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_IoDatagramAdapterZeroByteRecv` | 0.653 ns | 0.652 ns | n/a |
| `BM_IoFileAdapterZeroByteRead` | 0.627 ns | 0.627 ns | n/a |
| `BM_IoTimeoutInvalidDelay` | 0.836 ns | 0.836 ns | n/a |
| `BM_IoStreamAdapterZeroByteSend` | 0.627 ns | 0.627 ns | n/a |
| `BM_RuntimeExternalStart/8192` | 6.95 ms | 6.95 ms | 1.201 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 12.0 ms | 4.30 ms | 688.852 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.19 ms | 4.19 ms | 1.955 M/s |
| `BM_RuntimeParallelShards/128` | 0.515 ms | 0.497 ms | 248.834 k/s |
| `BM_RuntimeParallelShards/512` | 1.92 ms | 1.86 ms | 266.386 k/s |

Interpretation:

- This is a modularity split only. SQE field assignment remains grouped by opcode family while preserving the original dispatcher order.
- The TSAN target covers epoll fallback, io_uring socket/file/multishot paths, timeout/cancel paths, and StopImmediately pending-IO cleanup.
- The benchmark canary does not show a gross runtime or helper-level fast-path regression from the structural split.

## 2026-06-01 Provided Buffer Resource Split Validation

This run validates the structural split of the io_uring provided-buffer ring resource helper on the requested remote Linux host with `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2`.

Changes under validation:

- `runtime_executor_io_uring_provided_buffer_resource_fragment.hpp` is now a 6-line umbrella.
- `runtime_executor_io_uring_provided_buffer_register_fragment.hpp` owns provided-buffer ring registration.
- `runtime_executor_io_uring_provided_buffer_unregister_fragment.hpp` owns provided-buffer ring unregistration.
- The split keeps the helpers inline in `AsyncRuntime::Executor`; it does not change registration arguments, pending-submit flush ordering, busy checks, group tracking, locks, allocations, memory ordering, or fallback behavior.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang Debug provided-buffer targeted tests: 6/6 passed.
- Remote clang TSAN provided-buffer targeted tests: 6/6 passed, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 138 total, 135 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_IoDatagramAdapterZeroByteRecv` | 0.647 ns | 0.647 ns | n/a |
| `BM_IoFileAdapterZeroByteRead` | 0.628 ns | 0.628 ns | n/a |
| `BM_IoStreamAdapterZeroByteSend` | 0.627 ns | 0.627 ns | n/a |
| `BM_IoStreamAdapterZeroByteSendZc` | 0.628 ns | 0.627 ns | n/a |
| `BM_IoFileAdapterZeroByteReadFixedAt` | 0.630 ns | 0.629 ns | n/a |
| `BM_RuntimeExternalStart/8192` | 6.70 ms | 6.69 ms | 1.224 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 13.5 ms | 4.59 ms | 605.570 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.19 ms | 4.19 ms | 1.954 M/s |
| `BM_RuntimeParallelShards/128` | 0.480 ms | 0.465 ms | 267.212 k/s |
| `BM_RuntimeParallelShards/512` | 1.83 ms | 1.77 ms | 280.493 k/s |

Interpretation:

- This is a modularity split only. The resource lifecycle remains executor-thread-only and the unregister path still flushes pending submissions before checking active operations.
- The TSAN target covers provided-buffer multishot recv paths plus invalid/unavailable backend boundaries for stream, accept, fixed-buffer, and epoll fallback helpers.
- The benchmark canary does not show a gross runtime or helper-level fast-path regression from this structural split.

## 2026-06-01 io_uring Support Split Validation

This run validates the structural split of the Linux `io_uring_support.hpp` helper on the requested remote Linux host with `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2`.

Changes under validation:

- `io_uring_support.hpp` is now a 21-line Linux-only umbrella.
- `io_uring_support_abi_fragment.hpp` owns fallback macro definitions for older kernel headers.
- `io_uring_support_opcode_fragment.hpp` owns opcode constants not guaranteed by the platform headers.
- `io_uring_support_types_fragment.hpp` owns setup, message, address, registration, and SQE request structs.
- `io_uring_support_syscall_fragment.hpp` owns raw `io_uring_*` syscall wrappers and setup parameter configuration.
- `io_uring_support_sqe_fragment.hpp` owns generic buffer and fixed-file read/write SQE filling.
- The split keeps all helpers inline; it does not change fallback macro values, opcode constants, struct layout, syscall arguments, setup flags, SQE field values, locks, allocations, branch shape, or memory ordering.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang Debug full runtime suite: 138 total, 135 passed, 3 skipped, 0 failed.
- Remote clang TSAN IO/io_uring/stress targeted suite: 96 total, 94 passed, 2 skipped, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 138 total, 135 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_IoDatagramAdapterZeroByteRecv` | 0.646 ns | 0.646 ns | n/a |
| `BM_IoFileAdapterZeroByteRead` | 0.628 ns | 0.627 ns | n/a |
| `BM_IoTimeoutInvalidDelay` | 0.837 ns | 0.836 ns | n/a |
| `BM_IoStreamAdapterZeroByteSend` | 0.627 ns | 0.627 ns | n/a |
| `BM_IoStreamAdapterZeroByteSendZc` | 0.629 ns | 0.629 ns | n/a |
| `BM_IoFileAdapterZeroByteReadFixedAt` | 0.627 ns | 0.627 ns | n/a |
| `BM_RuntimeExternalStart/8192` | 6.85 ms | 6.84 ms | 1.204 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 12.7 ms | 4.41 ms | 650.818 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.21 ms | 4.20 ms | 1.946 M/s |
| `BM_RuntimeParallelShards/128` | 0.483 ms | 0.464 ms | 265.762 k/s |
| `BM_RuntimeParallelShards/512` | 1.98 ms | 1.90 ms | 259.583 k/s |

Interpretation:

- This is a modularity split only. The Linux io_uring ABI support remains platform-gated and header-only, while syscall/setup and SQE fill responsibilities are now independently auditable.
- The TSAN target covers io_uring setup/config, epoll fallback, socket/file/multishot paths, StopImmediately pending-IO cleanup, above-64 thread scheduling, explicit same-thread local queue routing, and the Running -> Pending wake boundary.
- The benchmark canary does not show a gross runtime or helper-level fast-path regression from this structural split.

## 2026-06-01 Registered Buffer Resource Split Validation

This run validates the structural split of the io_uring registered-buffer resource helper on the requested remote Linux host with `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2`.

Changes under validation:

- `runtime_executor_io_uring_buffer_resource_fragment.hpp` is now an 8-line umbrella.
- `runtime_executor_io_uring_buffer_register_fragment.hpp` owns registered-buffer registration.
- `runtime_executor_io_uring_buffer_unregister_fragment.hpp` owns registered-buffer unregistration.
- The split keeps the helpers inline in `AsyncRuntime::Executor`; it does not change the `_WIN32` guard, syscall arguments, pending-submit flush ordering, busy checks, registered flag/count updates, locks, allocations, memory ordering, or fallback behavior.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang Debug fixed-buffer/resource targeted tests: 4/4 passed.
- Remote clang TSAN fixed-buffer/resource plus scheduler-boundary targeted tests: 9/9 passed, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 138 total, 135 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_IoDatagramAdapterZeroByteRecv` | 0.641 ns | 0.641 ns | n/a |
| `BM_IoFileAdapterZeroByteRead` | 0.629 ns | 0.629 ns | n/a |
| `BM_IoTimeoutInvalidDelay` | 0.837 ns | 0.836 ns | n/a |
| `BM_IoStreamAdapterZeroByteSend` | 0.627 ns | 0.627 ns | n/a |
| `BM_IoStreamAdapterZeroByteSendZc` | 0.630 ns | 0.629 ns | n/a |
| `BM_IoFileAdapterZeroByteReadFixedAt` | 0.629 ns | 0.629 ns | n/a |
| `BM_RuntimeExternalStart/8192` | 7.07 ms | 7.06 ms | 1.168 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 12.7 ms | 4.44 ms | 649.697 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.29 ms | 4.29 ms | 1.908 M/s |
| `BM_RuntimeParallelShards/128` | 0.496 ms | 0.477 ms | 259.553 k/s |
| `BM_RuntimeParallelShards/512` | 1.82 ms | 1.77 ms | 283.492 k/s |

Interpretation:

- This is a modularity split only. Registered-buffer lifecycle remains executor-thread-only, and unregistration still flushes pending submissions before rejecting active fixed-buffer operations.
- The TSAN target covers registered/fixed-buffer IO plus above-64 scheduling, explicit same-thread local queue routing, cross-thread hop pressure, and the Running -> Pending wake boundary.
- The benchmark canary does not show a gross runtime or helper-level fast-path regression from this structural split.

## 2026-06-01 Running Wake Terminal-State Fix

This run validates a scheduler correctness fix for wake requests that are made while a task is `Running` but the owner returns `Done` or `Again` instead of `Pending`.

Changes under validation:

- `finish_done()` now stores `TaskState::Done` before consuming the same-epoch requested-thread slot.
- `finish_again()` now stores `TaskState::Queued` before consuming the same-epoch requested-thread slot.
- `resolve_running_wake_request()` treats a same-epoch request that resolves after `Done` as deferred/no-op because the request was observed while the task was Running and only has scheduling meaning if the owner later becomes `Pending`.
- The fix does not add locks, allocations, queue hops, virtual dispatch, or changes to local/SPSC/MPSC topology, ready-source hints, wake notification, or cache-line-aligned executor state.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang Debug scheduler targeted tests: 8/8 passed.
- Remote clang TSAN scheduler targeted tests: 8/8 passed, no ThreadSanitizer report.
- Remote clang TSAN full runtime suite: 140 total, 137 passed, 3 skipped, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 140 total, 137 passed, 3 skipped, 0 failed.

New stress coverage:

- `RuntimeStressTests.RunningWakeBeforeDoneIsBenign`.
- `RuntimeStressTests.RunningWakeBeforeAgainIsBenign`.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 6.57 ms | 6.56 ms | 1.251 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 13.6 ms | 4.61 ms | 603.135 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.22 ms | 4.21 ms | 1.945 M/s |
| `BM_RuntimeParallelShards/128` | 0.486 ms | 0.469 ms | 263.680 k/s |
| `BM_RuntimeParallelShards/512` | 1.83 ms | 1.77 ms | 280.443 k/s |

Interpretation:

- The previous debug-only failure mode was a false invariant: a Running wake request can be real, but still become redundant if the owner completes or requeues itself instead of returning `Pending`.
- Publishing the non-Running finish state before consuming the request slot narrows the late-request window and keeps the request slot from leaking into the next run epoch.
- The benchmark canary stays within the existing noisy range for scheduler paths and does not show a gross regression from the correctness fix.

## 2026-06-01 io_uring Completion Cancel CQE Ownership Fix

This run validates the io_uring completion cancel fix on the requested remote Linux host with `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2`.

Changes under validation:

- `cancel_io_completion()` no longer publishes `ECANCELED` into the task wait state before the kernel CQE arrives. It submits the cancel SQE, marks `operation->cancel_requested`, and treats repeated cancel requests as already accepted.
- `completed_uring_status()` and `completed_uring_timeout_status()` now continue to report pending while the runtime still owns the completion token or the wait result is not ready.
- `UringIoRuntimeSocketStreamFixture.IoUringCompletionCancelIsNotConsumableBeforeCqe` covers self-cancel followed by an immediate second status check before the cancel CQE is delivered.
- The fix adds no locks, allocations, queue topology changes, virtual dispatch, or cross-thread route changes.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang Debug targeted IO/io_uring tests: 9/9 passed.
- Remote clang TSAN targeted IO/io_uring/scheduler-boundary tests: 12/12 passed, no ThreadSanitizer report.
- Remote clang TSAN full runtime suite: 141 total, 138 passed, 3 skipped, no ThreadSanitizer report.
- Remote clang Release full runtime suite: 141 total, 138 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_IoDatagramAdapterZeroByteRecv` | 0.672 ns | 0.671 ns | n/a |
| `BM_IoFileAdapterZeroByteRead` | 0.670 ns | 0.670 ns | n/a |
| `BM_IoTimeoutInvalidDelay` | 0.971 ns | 0.970 ns | n/a |
| `BM_IoStreamAdapterZeroByteSend` | 0.711 ns | 0.710 ns | n/a |
| `BM_IoStreamAdapterZeroByteSendZc` | 0.649 ns | 0.648 ns | n/a |
| `BM_IoFileAdapterZeroByteReadFixedAt` | 0.664 ns | 0.664 ns | n/a |
| `BM_RuntimeExternalStart/8192` | 7.44 ms | 7.43 ms | 1.103 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 12.1 ms | 4.36 ms | 677.438 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.18 ms | 4.17 ms | 1.961 M/s |
| `BM_RuntimeParallelShards/128` | 0.467 ms | 0.449 ms | 274.202 k/s |
| `BM_RuntimeParallelShards/512` | 2.10 ms | 1.99 ms | 246.025 k/s |

Interpretation:

- The previous behavior could let a task observe cancel completion and finish while the pending io_uring operation still held task/result ownership until the CQE path ran.
- Deferring user-visible `ECANCELED` until CQE completion preserves single-owner lifetime semantics without adding synchronization or changing queue routing.
- The benchmark canary stays within the existing noisy range for runtime paths and leaves helper-level zero-byte fast paths in the same sub-nanosecond range.

## 2026-06-01 BasicTask De-Fragmenting Validation

This run validates the cleanup that removes the class-body `#include` splice pattern from `BasicTask`.

Changes under validation:

- `basic_task_fragment.hpp` is now one cohesive `BasicTask` class definition again.
- Removed the public/protected/lifetime/schedule/storage BasicTask fragment headers that existed only to splice code into class access sections.
- Kept task lifecycle, scheduling, Running wake, requested-thread encoding, and storage inline/template-visible in the same class. This is a readability/maintainability cleanup, not a runtime behavior change.
- Future splits should use independent data structures, algorithms, function families, or classes as boundaries instead of access-section fragments.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug task/lifecycle/scheduler/parallel targeted tests: 47/47 passed.
- Remote clang TSAN task/lifecycle/scheduler/parallel targeted tests: 41/41 passed, no ThreadSanitizer report.
- Remote clang Release task/lifecycle/scheduler/parallel targeted tests: 41/41 passed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 6.39 ms | 6.37 ms | 1.299 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 13.9 ms | 4.72 ms | 591.198 k/s |
| `BM_RuntimeIoThreadHop/8192` | 5.65 ms | 5.63 ms | 1.458 M/s |
| `BM_RuntimeParallelShards/128` | 0.570 ms | 0.544 ms | 226.356 k/s |
| `BM_RuntimeParallelShards/512` | 1.85 ms | 1.79 ms | 276.807 k/s |

Interpretation:

- The cleanup removes an over-fragmented structure without changing scheduling state transitions, queue routing, memory ordering, locks, allocations, or task storage fields.
- Keeping `BasicTask` readable in declaration order improves maintainability for future scheduler audits, especially around Running -> Pending wake resolution.
- The benchmark canary stays within the recent noisy remote range for scheduler paths.

## 2026-06-01 ObjectPool De-Fragmenting Validation

This run validates the cleanup that removes the class-body `#include` splice pattern from `ObjectPool`.

Changes under validation:

- `object_pool.hpp` is now one cohesive `ObjectPool` class definition again.
- Removed the storage, slot acquire/release, and lifecycle fragment headers that existed only to splice code into the class body.
- Kept TLS cache, cache-line slot sizing, MPMC free-list, slot acquire/release, lifecycle, and hot-block atomics together in declaration order. This is a readability/maintainability cleanup, not a runtime behavior change.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug Pool/Runtime/Stress targeted tests: 27/27 passed.
- Remote clang TSAN Pool/Runtime/Stress targeted tests: 27/27 passed, no ThreadSanitizer report.
- Remote clang Release Pool/Runtime/Stress targeted tests: 27/27 passed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean | Throughput Mean |
| --- | ---: | ---: | ---: |
| `BM_ObjectPoolCreateDestroy/16384` | 48,464 ns | 48,396 ns | 341.805 M/s |
| `BM_RuntimeExternalStart/8192` | 6.69 ms | 6.68 ms | 1.230 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 13.4 ms | 4.68 ms | 613.925 k/s |

Interpretation:

- The cleanup removes an over-fragmented class-body structure without changing the object-pool allocation strategy, cache-line padding, TLS cache policy, queue type, or atomic ordering.
- Keeping the pool implementation in one file makes future false-sharing and lifetime audits easier than following public/private include fragments.

## 2026-06-01 Adapter Class De-Fragmenting Validation

This run validates the cleanup that removes class-body `#include` splicing from the public IO adapter classes.

Changes under validation:

- `IoStream`, `IoDatagramSocket`, `IoFile`, and `IoFixedFile` now keep their forwarding methods in one cohesive class definition per adapter.
- Removed the 16 recv/send/read/write/fixed/sync method fragment headers that existed only to splice code into class bodies.
- Preserved the adapter object model: two-field trivially-copyable views, no fd ownership, no heap allocation, no virtual dispatch, and inline/template-visible forwarding to the same IO helper functions.

Correctness and race checks:

- Local `git diff --check`: passed before the documentation update.
- Remote clang Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug adapter/stream/datagram/file-boundary targeted tests: 27/27 passed.
- Remote clang TSAN adapter/stream/datagram/file-boundary targeted tests: 27/27 passed, no ThreadSanitizer report.
- Remote clang Release adapter/stream/datagram/file-boundary targeted tests: 27/27 passed.

Release IO adapter benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | CPU Mean |
| --- | ---: | ---: |
| `BM_IoDatagramAdapterZeroByteRecv` | 0.640 ns | 0.640 ns |
| `BM_IoFileAdapterZeroByteRead` | 0.628 ns | 0.627 ns |
| `BM_IoStreamAdapterZeroByteSend` | 0.627 ns | 0.627 ns |
| `BM_IoStreamAdapterZeroByteSendZc` | 0.627 ns | 0.627 ns |
| `BM_IoFileAdapterZeroByteReadFixedAt` | 0.630 ns | 0.629 ns |

Interpretation:

- The cleanup removes an over-fragmented adapter class-body structure without changing IO routing, queue selection, syscall paths, io_uring submit paths, memory ordering, or ownership behavior.
- The benchmark canary stayed in the same sub-nanosecond helper-level range as previous adapter checks, which is the expected result for an inline-only source layout cleanup.

## 2026-06-01 io_uring File Test Support De-Fragmenting Validation

This run validates the cleanup that removes class-body `#include` splicing from the io_uring file lifecycle and fixed-file read/write test support tasks.

Changes under validation:

- `UringFileLifecycleTask` now keeps its state dispatcher plus open, fallocate, write, fsync, read, statx, rename, unlink, and close handlers in one cohesive class definition.
- `UringFixedFileTask` now keeps its state dispatcher plus file/buffer registration, fixed-buffer IO, vectored IO, fsync, and unregister handlers in one cohesive class definition.
- Removed the six task fragment headers that existed only to splice private methods into those two class bodies.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` fresh Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug `UringIoRuntimeFileFixture.*` targeted run: 11 tests loaded and skipped by test logic with `io_uring backend unavailable`.
- Remote clang Debug full `asyncflow_runtime_tests` run: 141 tests, 118 passed, 23 skipped.

Interpretation:

- This pass changed only test support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, public APIs, task field layout, and test assertions were unchanged.
- No Release benchmark was run for this pass because no production runtime path or benchmarked helper path changed.

## 2026-06-01 io_uring Filesystem/Multishot Test Support De-Fragmenting Validation

This run validates the cleanup that removes class-body `#include` splicing from the io_uring recv-multishot and filesystem-ops test support tasks.

Changes under validation:

- `UringRecvMultishotTask` now keeps its state dispatcher, provided-buffer ring lifecycle, recv completion, buffer recycling, stop, and cancel handling in one cohesive class definition.
- `UringFilesystemOpsTask` now keeps its state dispatcher plus mkdir, openat2, write, ftruncate, fsync, statx, close, link, symlink, unlink, and rmdir handling in one cohesive class definition.
- Removed the six task fragment headers that existed only to splice private methods into those two class bodies.

Correctness and race checks:

- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` fresh Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug targeted run under `--security-opt seccomp=unconfined`: 4/4 passed for `UringIoRuntimeSocketMultishotFixture.*` and `UringIoRuntimeFileFixture.IoUringFilesystemOpsRunOnIoThread`.
- Remote clang Debug full `asyncflow_runtime_tests` under `--security-opt seccomp=unconfined`: 141 tests, 138 passed, 3 skipped.

Interpretation:

- This pass changed only test support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, public APIs, task field layout, and test assertions were unchanged.
- No Release benchmark was run for this pass because no production runtime path or benchmarked helper path changed.

## 2026-06-01 Runtime Core De-Fragmenting Validation

This run validates the cleanup that removes class-body fragment splicing from the active core runtime path while preserving header-only/template-visible implementation.

Changes under validation:

- `include/af/async_runtime.hpp` now declares the `AsyncRuntime` shell and static runtime state directly, with no class-body `#include` fragments.
- Config, task handle, and common runtime state moved to named detail components: `runtime_config.hpp`, `runtime_task_handle.hpp`, and `runtime_common_state.hpp`.
- Public lifecycle/task/parallel APIs, public IO APIs, dispatch, task lifecycle, and parallel implementation now live in namespace-scope inline implementation headers: `runtime_public_api.hpp`, `runtime_public_io.hpp`, `runtime_dispatch.hpp`, `runtime_task_lifecycle.hpp`, and `runtime_parallel.hpp`.
- `detail::Executor<RuntimeT, TraitsT>` is now a standalone executor component in `runtime_executor.hpp` instead of a nested `AsyncRuntime::Executor` class assembled through class-scope fragments.
- Same-thread runtime posts still route explicitly through `ReadyQueueRoute::Local`; cross-thread runtime posts still use `ReadyQueueRoute::Spsc`; external posts still use the external MPSC path.
- The thread-count config boundary remains above-64 capable: `Traits::thread_count > 0` and `Traits::thread_count <= UINT16_MAX`.

Correctness and race checks:

- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release `asyncflow_runtime_tests` and `asyncflow_runtime_benchmarks` build: passed.
- Remote clang Release full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | Throughput Mean |
| --- | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 6.99 ms | 1.173 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 10.9 ms | 754 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.28 ms | 1.917 M/s |
| `BM_RuntimeParallelShards/128` | 0.494 ms | 259 k/s |

Interpretation:

- The cleanup changes structure, not scheduling semantics. No queue type, lock, atomic ordering, IO submit/completion rule, task state transition, or public behavior was intentionally changed.
- The active runtime path now follows the stricter modularity rule: split real components and operation families, not single class bodies by access section.
- `runtime_executor.hpp` remains the largest core file and should keep being reduced only through real ownership boundaries such as class-out-of-line operation-family definitions, backend-specific helper components, or scheduler-only helper algorithms. Reintroducing class-scope fragment splicing would repeat the maintainability problem this pass fixed.

## 2026-06-01 Executor Lifecycle And Resource Implementation Split

This run validates the first follow-up split of the large standalone executor file after removing class-body splicing.

Changes under validation:

- `runtime_executor.hpp` now keeps executor lifecycle/notify and io_uring resource APIs as declarations in the class body.
- `runtime_executor_lifecycle.hpp` owns the class-out-of-line template definitions for constructor/destructor, `start()`, `request_stop()`, `join()`, and `notify()`.
- `runtime_executor_io_resources.hpp` owns the class-out-of-line template definitions for backend status checks and io_uring buffer/provided-buffer/fixed-file registration operations.
- The split is not a class-body `#include` splice. The implementation headers are included after the complete `Executor` declaration and define `Executor<RuntimeT, TraitsT>::...` members in namespace scope.

Correctness and race checks:

- Local `git diff --check`: passed before this documentation update.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release `asyncflow_runtime_tests` and `asyncflow_runtime_benchmarks` build: passed.
- Remote clang Release full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | Throughput Mean |
| --- | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 7.44 ms | 1.106 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 11.6 ms | 706.687 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.28 ms | 1.916 M/s |
| `BM_RuntimeParallelShards/128` | 0.525 ms | 244.905 k/s |

Interpretation:

- This pass changes only declaration/definition placement. Queue topology, local/SPSC/MPSC routing, worker notification, native IO wake behavior, io_uring registration syscall arguments, pending-submit flush ordering, locks, atomics, allocations, and task state transitions are unchanged.
- `runtime_executor.hpp` dropped from 4916 lines to 4435 lines. The next executor split should target a similarly real operation family; splitting by access section or field block remains disallowed.

## 2026-06-01 Executor Scheduler Implementation Split

This run validates the follow-up split of executor ready/local-queue execution and run-loop scheduling into a normal implementation header.

Changes under validation:

- `runtime_executor.hpp` now keeps ready-source, local queue, task execution, run-loop, pop, and finish APIs as class declarations.
- `runtime_executor_scheduler.hpp` owns the class-out-of-line template definitions for `mark_ready()`, `notify_external_ready()`, local queue push/pop, `execute()`, `notify_force()`, `run_loop()`, ready-source scan cursor advance, `pop_one()`, and task finish paths.
- The split keeps executor field layout together in `runtime_executor.hpp`; it does not move queue/cache state fields or reintroduce class-body `#include` splicing.

Correctness and race checks:

- Local `git diff --check`: passed before this documentation update.
- Local scan for `#include "af/detail/*fragment.hpp"` inside framework class/struct bodies: no matches.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release `asyncflow_runtime_tests` and `asyncflow_runtime_benchmarks` build: passed.
- Remote clang Release full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | Throughput Mean |
| --- | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 6.85 ms | 1.206 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 12.4 ms | 662.395 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.17 ms | 1.964 M/s |
| `BM_RuntimeParallelShards/128` | 0.460 ms | 278.731 k/s |

Interpretation:

- This pass changes only declaration/definition placement. Same-thread local queue routing, cross-thread SPSC routing, external MPSC routing, ready-source hint handling, worker wait/notify behavior, task state transitions, locks, atomics, and allocations are unchanged.
- `runtime_executor.hpp` dropped from 4435 lines to 4209 lines. Further executor splits should keep following operation-family boundaries and leave state/cache layout auditable in the main executor declaration.

## 2026-06-01 Executor IO Backend Implementation Split

This run validates the follow-up split of executor IO backend setup, polling, io_uring completion, and operation cleanup into a normal implementation header.

Changes under validation:

- `runtime_executor.hpp` now keeps IO backend lifecycle, io_uring ring setup, submission flush, CQ completion, cancel SQE, operation tracking, and backend-failure cleanup APIs as declarations near the executor state layout.
- `runtime_executor_io_backend.hpp` owns the class-out-of-line template definitions for `init_io_backend()`, `close_io_backend()`, `poll_io()`, io_uring setup/mapping, SQ reservation/flush, CQ polling, operation completion, direct-file cleanup, cancel submit, operation tracking, backend failure cleanup, and operation destruction.
- The split keeps executor state/cache fields together in `runtime_executor.hpp`; it does not reintroduce class-body `#include` splicing.

Correctness and race checks:

- Local `git diff --check`: passed before this documentation update.
- Local scan for `#include "af/detail/*fragment.hpp"` inside framework class/struct bodies: no matches.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release `asyncflow_runtime_tests` and `asyncflow_runtime_benchmarks` build: passed.
- Remote clang Release full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | Throughput Mean |
| --- | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 6.56 ms | 1.249 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 12.3 ms | 670.388 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.34 ms | 1.888 M/s |
| `BM_RuntimeParallelShards/128` | 0.478 ms | 269.014 k/s |

Interpretation:

- This pass changes only declaration/definition placement. io_uring setup flags, mmap layout, SQ/CQ memory ordering, submission batching, completion result publication, cancel handling, direct-file cleanup, operation ownership, locks, atomics, allocations, and task wake behavior are unchanged.
- `runtime_executor.hpp` dropped from 4209 lines to 3605 lines. Further executor work should target remaining real operation families, such as native readiness backend wait/cancel or public io_uring submit wrappers.

## 2026-06-01 Executor Epoll Backend Implementation Split

This run validates the follow-up split of Linux native readiness backend setup, wake, polling, wait registration, and cancel into a normal implementation header.

Changes under validation:

- `runtime_executor.hpp` now keeps the Linux epoll native readiness backend as declarations in the executor class body.
- `runtime_executor_epoll_backend.hpp` owns the class-out-of-line template definitions for `native_io_backend_available()`, `notify_native_io_backend()`, `init_native_io_backend()`, `close_native_io_backend()`, `clear_io_waits()`, `poll_native_io()`, `register_native_io_wait()`, and `cancel_native_io_wait()`.
- The split keeps executor state/cache fields together in `runtime_executor.hpp`; it does not reintroduce class-body `#include` splicing.

Correctness and race checks:

- Local `git diff --check`: passed before this documentation update.
- Local scan for `#include "af/detail/*fragment.hpp"` inside framework class/struct bodies: no matches.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release `asyncflow_runtime_tests` and `asyncflow_runtime_benchmarks` build: passed.
- Remote clang Release full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.

Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | Throughput Mean |
| --- | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 6.62 ms | 1.242 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 12.5 ms | 656.049 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.28 ms | 1.913 M/s |
| `BM_RuntimeParallelShards/128` | 0.477 ms | 268.526 k/s |

Interpretation:

- This pass changes only declaration/definition placement. epoll fd/eventfd setup, wake coalescing, one-shot wait registration, io_uring poll-wait fallback, cancel behavior, task wake behavior, locks, atomics, allocations, and ownership semantics are unchanged.
- `runtime_executor.hpp` dropped from 3605 lines to 3356 lines. Further executor work should target remaining real operation families; kqueue should be handled separately on a platform where it can be compiled and tested directly.

## 2026-06-01 Executor io_uring Submit Core Split

This run validates the split of private Linux io_uring submit helpers into a normal implementation header.

Changes under validation:

- `runtime_executor.hpp` now keeps private submit-core APIs as declarations near the executor state layout.
- `runtime_executor_io_submit_core.hpp` owns the class-out-of-line template definitions for provided-buffer group lookup, buffer submit, fast SQE submit, socket submit, fixed-file read/write submit, generic submit validation, message/address attachment, SQE filling, and `submit_io_uring_op()`.
- The split leaves the generic submit argument/kind structs in the executor declaration so private type layout and submit classification remain auditable next to the executor state.
- The split keeps all code header-only/template-visible and does not reintroduce class-body `#include` splicing.

Correctness and race checks:

- Local `git diff --check`: passed before this documentation update.
- Local scan for `#include "af/detail/*fragment.hpp"` inside framework class/struct bodies: no matches.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release `asyncflow_runtime_tests` and `asyncflow_runtime_benchmarks` build: passed.
- Remote clang Release full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.

Release benchmark canary, second run, 3 repetitions with `--benchmark_min_time=0.05s`:

| Case | Real-Time Mean | Throughput Mean |
| --- | ---: | ---: |
| `BM_RuntimeExternalStart/8192` | 7.29 ms | 1.124 M/s |
| `BM_RuntimeCrossThreadHop/8192` | 13.0 ms | 630.069 k/s |
| `BM_RuntimeIoThreadHop/8192` | 4.28 ms | 1.915 M/s |
| `BM_RuntimeParallelShards/128` | 0.484 ms | 265.840 k/s |

Interpretation:

- This pass changes only declaration/definition placement. SQE contents, io_uring operation ownership, submit batching, fixed-resource validation, provided-buffer checks, message/address storage, locks, atomics, allocations, and task wake behavior are unchanged.
- The runtime hop benchmarks do not directly exercise the moved io_uring submit helpers; the external/cross-thread values remain within the historical canary range but are noisy enough that they should not be treated as a stable microarchitectural baseline.
- `runtime_executor.hpp` dropped from 3356 lines to 2627 lines. Further executor work should target remaining real operation families, especially public submit wrappers or platform-specific kqueue code when a kqueue-capable runner is available.

## 2026-06-01 Task Header Fragment Rename

This run validates removal of active `*_fragment.hpp` naming from the public task entry point.

Changes under validation:

- `include/af/task.hpp` now includes `task_types.hpp`, `task_io_state.hpp`, `task_registry.hpp`, and `basic_task.hpp`.
- The old `AF_TASK_FRAGMENT_INCLUDE` guard was replaced with `AF_TASK_DETAIL_INCLUDE`.
- No task state transition, memory ordering, queue routing, allocation policy, or virtual task API changed.

Correctness checks:

- Local `git diff --check`: passed before this documentation update.
- Local scan for `#include "af/detail/*fragment.hpp"` inside framework class/struct bodies: no matches.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release `asyncflow_runtime_tests` build: passed.
- Remote clang Release full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.

Interpretation:

- This is a naming/include-structure cleanup, not a performance-sensitive code change. A new benchmark run was not collected for this pass because the generated task/runtime logic is unchanged.

## 2026-06-01 IO Types Header Fragment Rename

This run validates removal of active `*_fragment.hpp` naming from the public IO type entry point.

Changes under validation:

- `include/af/io_types.hpp` now includes `io_types_base.hpp`, `io_types_provided_buffer.hpp`, `io_types_status.hpp`, and `io_types_unique_fd.hpp`.
- The old `AF_IO_TYPES_FRAGMENT_INCLUDE` guard was replaced with `AF_IO_TYPES_DETAIL_INCLUDE`.
- No IO type layout, fd ownership behavior, provided-buffer ring behavior, status normalization, or public API changed.

Correctness checks:

- Local `git diff --check`: passed before this documentation update.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug full runtime suite under `--security-opt seccomp=unconfined`: 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release `asyncflow_runtime_tests` build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.

Interpretation:

- This is a naming/include-structure cleanup, not a performance-sensitive code change. A new benchmark run was not collected for this pass because the generated IO type logic is unchanged.

## 2026-06-01 Timeout And Datagram Header Fragment Rename

This run validates removal of active `*_fragment.hpp` naming from the public timeout and datagram IO entry points.

Changes under validation:

- `include/af/io_timeout.hpp` now includes `io_timeout_status.hpp`, `io_timeout_wait.hpp`, and `io_timeout_deadline.hpp`.
- `include/af/io_datagram.hpp` now includes `io_datagram_recv.hpp`, `io_datagram_send.hpp`, `io_datagram_vectored.hpp`, and `io_datagram_zero_copy.hpp`.
- The old `AF_IO_TIMEOUT_FRAGMENT_INCLUDE` and `AF_IO_DATAGRAM_FRAGMENT_INCLUDE` guards were replaced with `AF_IO_TIMEOUT_DETAIL_INCLUDE` and `AF_IO_DATAGRAM_DETAIL_INCLUDE`.
- No timeout arbitration logic, datagram send/receive behavior, zero-copy path, wait/cancel ordering, or public API changed.

Correctness checks:

- Local `git diff --check`: passed before this documentation update.
- Local scan for `#include "af/detail/*fragment.hpp"` inside framework class/struct bodies: no matches.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug `asyncflow_runtime_tests` build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release `asyncflow_runtime_tests` build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.

Interpretation:

- This is a naming/include-structure cleanup, not a performance-sensitive code change. A new benchmark run was not collected for this pass because the generated timeout/datagram logic is unchanged.

## 2026-06-01 Filesystem Header Fragment Rename

This run validates removal of active `*_fragment.hpp` naming from the public filesystem IO entry point.

Changes under validation:

- `include/af/io_filesystem.hpp` now includes `io_filesystem_open.hpp`, `io_filesystem_namespace.hpp`, `io_filesystem_allocation.hpp`, and `io_filesystem_directory.hpp`.
- The old `AF_IO_FILESYSTEM_FRAGMENT_INCLUDE` guard was replaced with `AF_IO_FILESYSTEM_DETAIL_INCLUDE`.
- No open/openat2, namespace-operation, allocation/truncate, `IoDirectory`, or public API behavior changed.

Correctness checks:

- Local `git diff --check`: passed before this documentation update.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug `asyncflow_runtime_tests` build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release `asyncflow_runtime_tests` build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.

Interpretation:

- This is a naming/include-structure cleanup, not a performance-sensitive code change. A new benchmark run was not collected for this pass because the generated filesystem helper logic is unchanged.

## 2026-06-01 IO Common Header Fragment Rename

This run validates removal of active `*_fragment.hpp` naming from the public shared IO helper entry point.

Changes under validation:

- `include/af/io_common.hpp` now includes `io_common_base.hpp`, `io_common_state.hpp`, `io_common_fixed_file.hpp`, `io_common_linux_event_timer.hpp`, and `io_common_deadline.hpp`.
- `io_common_state.hpp` now includes `io_common_target.hpp`, `io_common_wait_arm.hpp`, `io_common_wait_state.hpp`, `io_common_uring_status.hpp`, and `io_common_iovec.hpp`.
- The old `AF_IO_COMMON_FRAGMENT_INCLUDE` guard was replaced with `AF_IO_COMMON_DETAIL_INCLUDE`.
- The `io_common_state.hpp` include order is protected because `io_common_uring_status.hpp` depends on wait-state helpers declared earlier.
- No IO wait state layout, readiness wait arming, io_uring status normalization, fixed-file vectored helper, Linux eventfd/timerfd helper, deadline state, or public API behavior changed.

Correctness checks:

- Local `git diff --check`: passed before this documentation update.
- Local scan for active `io_common_*fragment.hpp` references under `include/af`: no matches.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug `asyncflow_runtime_tests` build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release `asyncflow_runtime_tests` build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.

Interpretation:

- This is a naming/include-structure cleanup, not a performance-sensitive code change. A new benchmark run was not collected for this pass because the generated shared IO helper logic is unchanged.

## 2026-06-01 IO Adapters Header Fragment Rename

This run validates removal of active `*_fragment.hpp` naming from the public IO adapter entry point.

Changes under validation:

- `include/af/io_adapters.hpp` now includes `io_adapters_descriptor.hpp`, `io_adapters_file.hpp`, `io_adapters_stream_listener.hpp`, `io_adapters_datagram.hpp`, `io_adapters_aliases.hpp`, and `io_adapters_event_timer.hpp`.
- `io_adapters_file.hpp` now includes `io_adapters_file_descriptor.hpp` and `io_adapters_fixed_file.hpp`; `io_adapters_stream_listener.hpp` now includes `io_adapters_listener.hpp` and `io_adapters_stream.hpp`.
- The old `AF_IO_ADAPTERS_FRAGMENT_INCLUDE` guard was replaced with `AF_IO_ADAPTERS_DETAIL_INCLUDE`.
- No adapter object layout, fd/fixed-file ownership behavior, forwarding helper, queue selection, syscall/io_uring routing, or public API behavior changed.

Correctness checks:

- Local `git diff --check`: passed before this documentation update.
- Local scan for active `io_adapters_*fragment.hpp` references under `include/af`: no matches.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug `asyncflow_runtime_tests` build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release `asyncflow_runtime_tests` build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.

Interpretation:

- This is a naming/include-structure cleanup, not a performance-sensitive code change. A new benchmark run was not collected for this pass because the generated adapter forwarding logic is unchanged.

## 2026-06-01 IO File Header Fragment Rename

This run validates removal of active `*_fragment.hpp` naming from the public file IO helper entry point.

Changes under validation:

- `include/af/io_file.hpp` now includes `io_file_read.hpp`, `io_file_positioned.hpp`, `io_file_fixed_buffer.hpp`, `io_file_lifecycle.hpp`, and `io_file_write.hpp`.
- `io_file_read.hpp` now includes `io_file_read_current.hpp` and `io_file_read_positioned.hpp`.
- `io_file_fixed_buffer.hpp` now includes `io_file_fixed_file.hpp`, `io_file_registered_buffer.hpp`, and `io_file_vectored_write.hpp`.
- `io_file_lifecycle.hpp` now includes `io_file_close_fsync.hpp`, `io_file_metadata.hpp`, `io_file_namespace.hpp`, and `io_file_open.hpp`.
- The old `AF_IO_FILE_FRAGMENT_INCLUDE` guard was replaced with `AF_IO_FILE_DETAIL_INCLUDE`.
- No file read/write, positioned IO, fixed-file, registered-buffer, vectored write, open/close/fsync, metadata, namespace operation, wait/cancel ordering, or public API behavior changed.

Correctness checks:

- Local `git diff --check`: passed before this documentation update.
- Local scan for active `io_file_*fragment.hpp` includes under `include/af`: no matches.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.

Interpretation:

- This is a naming/include-structure cleanup, not a performance-sensitive code change. A new benchmark run was not collected for this pass because the generated file IO helper logic is unchanged.

## 2026-06-01 IO Socket Header Fragment Rename

This run validates removal of active `*_fragment.hpp` naming from the public socket IO helper entry point.

Changes under validation:

- `include/af/io_socket.hpp` now includes `io_socket_lifecycle.hpp`, `io_socket_accept_connect.hpp`, `io_socket_recv.hpp`, `io_socket_send.hpp`, `io_socket_transfer.hpp`, and `io_socket_vectored.hpp`.
- `io_socket_lifecycle.hpp` now includes `io_socket_create.hpp`, `io_socket_options.hpp`, `io_socket_name.hpp`, and `io_socket_listener.hpp`.
- `io_socket_accept_connect.hpp` now includes `io_socket_accept.hpp`, `io_socket_accept_direct.hpp`, `io_socket_accept_multishot.hpp`, and `io_socket_connect.hpp`.
- `io_socket_recv.hpp` now includes `io_socket_recv_basic.hpp`, `io_socket_recv_multishot.hpp`, and `io_socket_recvmsg_multishot.hpp`.
- `io_socket_send.hpp` now includes `io_socket_send_basic.hpp`, `io_socket_send_fixed_file.hpp`, `io_socket_send_zc.hpp`, and `io_socket_sendv_zc.hpp`.
- `io_socket_transfer.hpp` now includes `io_socket_sendfile.hpp`, `io_socket_shutdown.hpp`, and `io_socket_splice.hpp`.
- The old `AF_IO_SOCKET_FRAGMENT_INCLUDE` guard was replaced with `AF_IO_SOCKET_DETAIL_INCLUDE`.
- No socket create/options/name/listener, accept/connect, recv/send, multishot, zero-copy, transfer, vectored IO, wait/cancel ordering, or public API behavior changed.

Correctness checks:

- Local `git diff --check`: passed before this documentation update.
- Local scan for active `io_socket_*fragment.hpp` includes under `include/af`: no matches.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.

Interpretation:

- This is a naming/include-structure cleanup, not a performance-sensitive code change. A new benchmark run was not collected for this pass because the generated socket IO helper logic is unchanged.

## 2026-06-01 io_uring Support Header Fragment Rename

This run validates removal of active `*_fragment.hpp` naming from the Linux `io_uring_support.hpp` helper entry point.

Changes under validation:

- `include/af/detail/io_uring_support.hpp` now includes `io_uring_support_abi.hpp`, `io_uring_support_opcode.hpp`, `io_uring_support_types.hpp`, `io_uring_support_syscall.hpp`, and `io_uring_support_sqe.hpp`.
- The old `AF_IO_URING_SUPPORT_FRAGMENT_INCLUDE` guard was replaced with `AF_IO_URING_SUPPORT_DETAIL_INCLUDE`.
- The include order is explicitly fixed because syscall/setup and SQE helpers depend on support request types declared earlier.
- No fallback macro value, opcode constant, request struct layout, syscall argument, setup flag handling, SQE field value, branch, lock, allocation, memory ordering, or public API behavior changed.

Correctness checks:

- Local `git diff --check`: passed before this documentation update.
- Local scan for active `io_uring_support_*fragment.hpp` includes/guards under `include/af/detail`: no matches.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release build and full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.

Interpretation:

- This is a naming/include-structure cleanup, not a performance-sensitive code change. A new benchmark run was not collected for this pass because the generated Linux io_uring support helper logic is unchanged.

## 2026-06-01 Stale Runtime Fragment Entrypoint Cleanup

This run removes obsolete top-level runtime fragment entrypoint headers that were left behind after the active runtime moved to normal `runtime_*.hpp` and `runtime_executor_*.hpp` implementation headers.

Changes under validation:

- Removed 26 unreferenced `include/af/detail/runtime*_fragment.hpp` entrypoint wrappers.
- The removed files were not referenced by active framework headers, tests, examples, benchmarks, CMake, or Conan metadata outside documentation.
- No active scheduler, executor, IO backend, public IO, task lifecycle, queue, state layout, syscall, lock, allocation, memory ordering, or public API behavior changed.
- Existing dirty files outside this cleanup were left untouched.

Correctness checks:

- Local reference scan for each deleted basename under `include/af`, `tests`, `examples`, `benchmarks`, `CMakeLists.txt`, and `conanfile.txt`: no matches outside documentation.
- Local `git diff --check`: passed before this documentation update.

Interpretation:

- This is dead internal-header cleanup. A new build or benchmark run was not collected for this pass because the deleted entrypoint wrappers are not reachable from active code.

## 2026-06-01 Stale Runtime Fragment Child Cleanup

This run removes the remaining clean framework runtime fragment child headers that were only referenced from other obsolete fragment headers.

Changes under validation:

- Removed 137 clean `include/af/detail/runtime*_fragment.hpp` child headers that had no active non-fragment include path.
- Preserved the pre-existing dirty `runtime_executor_io_uring_backend_completion_operation_fragment.hpp` worktree file without editing or staging it.
- After the deletion, the only framework `include/af/detail/*fragment.hpp` file left in the worktree is that pre-existing dirty file.
- No active scheduler, executor, IO backend, public IO, task lifecycle, queue, state layout, syscall, lock, allocation, memory ordering, or public API behavior changed.

Correctness checks:

- Local scan for `#include "af/detail/*fragment.hpp"` in non-fragment framework headers, tests, examples, benchmarks, CMake, and Conan metadata: no matches.
- Local `git diff --check`: passed before this documentation update.

Interpretation:

- This is dead internal-header cleanup. A new build or benchmark run was not collected for this pass because the deleted child fragments are not reachable from active code.

## 2026-06-01 Stale Runtime Fragment Orphan Cleanup

This follow-up removes the final orphaned framework runtime fragment header after checking that its pending cancel-drain completion logic already exists in the active `runtime_executor_io_backend.hpp` implementation.

Changes under validation:

- Removed `include/af/detail/runtime_executor_io_uring_backend_completion_operation_fragment.hpp`.
- The removed file was not referenced by active framework headers, tests, examples, benchmarks, CMake, or Conan metadata.
- No active scheduler, executor, IO backend, public IO, task lifecycle, queue, state layout, syscall, lock, allocation, memory ordering, or public API behavior changed.

Correctness checks:

- Local scan for framework `include/af/detail/*fragment.hpp` files: no matches.
- Local scan for `#include "af/detail/*fragment.hpp"` in non-fragment framework headers, tests, examples, benchmarks, CMake, and Conan metadata: no matches.

Interpretation:

- This is dead internal-header cleanup. A new build or benchmark run was not collected for this pass because the deleted orphan header was unreachable and its behavior was already present in the active implementation.

## 2026-06-01 Test And Benchmark Support Fragment Rename

This run removes the remaining active `fragment` naming from test, example, and benchmark support code. The code still uses small support/detail headers where they represent real task families, fixture helpers, or benchmark stub families; only the misleading fragment terminology was removed.

Changes under validation:

- Renamed the remaining `tests/support/*_fragment.hpp` headers to normal `tests/support/*.hpp` names.
- Renamed the remaining `benchmarks/detail/*_fragment.hpp` headers to normal `benchmarks/detail/*.hpp` names.
- Replaced test/benchmark include gates such as `AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE` with `*_DETAIL_INCLUDE`.
- Replaced the RPC example include gate `IO_RPC_LENGTH_PREFIXED_SERVER_FRAGMENT_INCLUDE` with `IO_RPC_LENGTH_PREFIXED_SERVER_DETAIL_INCLUDE`.
- Adopted the pending io_uring multishot cancel-drain test coverage before the rename so behavior tests and file-name churn remain separable in history.

Correctness checks:

- Local scan for `*.mjs`: no matches.
- Local scan for active `*_fragment.hpp` source files: no matches.
- Local scan for `FRAGMENT_INCLUDE`, `_fragment.hpp`, or active source text saying `implementation fragment`: no matches outside documentation.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug full build, including examples, tests, and benchmarks: passed.
- Remote clang Debug full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release full build, including examples, tests, and benchmarks: passed.
- Remote clang Release full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.

Interpretation:

- This is a source-organization and naming cleanup. It does not change queue algorithms, task state transitions, IO submit/completion behavior, locks, allocations, memory ordering, cache-line layout, or public API semantics.

## 2026-06-01 Benchmark Runtime Stub De-Splicing

This follow-up removes the last active class-body include splice found outside the framework tree. `benchmarks/io_benchmark_support.hpp` no longer includes operation stub headers inside the `FakeRuntime` class body; the benchmark runtime stubs are normal operation-family base structs.

Changes under validation:

- `FakeRuntimeLinuxSocketOps`, `FakeRuntimePosixMessageOps`, `FakeRuntimePosixFileOps`, `FakeRuntimePosixAcceptOps`, and `FakeRuntimeFilesystemOps` now own the benchmark-only static IO submit stubs.
- `FakeRuntime` inherits those base structs instead of using `#include` directives in its class body.
- No benchmark stub return value, argument type, platform guard, public framework header, queue logic, task state transition, lock, allocation, memory ordering, or cache layout changed.

Correctness checks:

- Local scan for `#include` directives inside class/struct bodies under `include`, `tests`, `examples`, and `benchmarks`: no matches.
- Local `git diff --check`: passed.
- Remote clang Debug build of `asyncflow_runtime_benchmarks`: passed.
- Remote clang Release build of `asyncflow_runtime_benchmarks`: passed.

Interpretation:

- This completes the removal of active class-body include splicing in source code while keeping benchmark stub responsibilities split by operation family.

## 2026-06-01 Non-Runtime Detail Include Gate Cleanup

This run removes broad detail include gate macros from ordinary task, IO, io_uring support, test, example, and benchmark support headers. Those headers now behave like normal internal headers with `#pragma once`; the remaining gate is limited to runtime template implementation files that depend on being included after complete `AsyncRuntime` / `Executor` declarations.

Changes under validation:

- Removed `DETAIL_INCLUDE` gate blocks from public task, IO, io_uring support, test support, example support, and benchmark support detail headers.
- Removed matching temporary `#define ... DETAIL_INCLUDE` / `#undef ... DETAIL_INCLUDE` wrappers from their umbrella headers.
- Added `#pragma once` to the affected detail/support headers where it was previously missing.
- Kept `AF_ASYNC_RUNTIME_IMPL_INCLUDE` for runtime implementation headers because those class-out-of-line template definitions intentionally require the parent declaration context.

Correctness checks:

- Local scan for `DETAIL_INCLUDE`, `implementation detail`, and `internal to af/` under active source: no matches.
- Local scan for active `*_fragment.hpp` source files: no matches.
- Local `git diff --check`: passed.
- Remote clang Debug build of `asyncflow_runtime_tests` and `asyncflow_runtime_benchmarks`: passed.
- Remote clang Debug full runtime suite under `--security-opt seccomp=unconfined`: passed; 143 tests, 140 passed, 3 skipped, 0 failed.
- Remote clang Release build of `asyncflow_runtime_tests` and `asyncflow_runtime_benchmarks`: passed.

Interpretation:

- This reduces preprocessor coupling for normal internal headers. No queue algorithm, IO behavior, task state transition, lock, allocation, memory ordering, cache-line layout, benchmark stub return value, or public API behavior changed.

## 2026-06-03 MPSC Producer Contention Benchmark Coverage

This pass adds a live contention benchmark for the bounded MPSC queue used by
framework-external producers. The existing queue benchmark only measured the
single-threaded push/pop path, so it could not expose the tail-side atomic
contention that matters when multiple external threads submit work concurrently.

Changes under validation:

- Added `BM_MpscQueueConcurrentProducers` with four producer threads and one
  consumer thread.
- The benchmark reuses preallocated payload storage, measures only queue
  publication/drain work inside the benchmark loop, and reports real time for
  cross-thread contention.
- The benchmark keeps each iteration bounded below queue capacity to avoid
  turning the measurement into a full-queue backpressure test.
- No production queue algorithm, memory ordering, lock, allocation, task routing,
  or runtime scheduling behavior changed in this pass.

Correctness and benchmark checks:

- Local macOS Debug `asyncflow_log_tests` task-tag focused run: 5/5 passed. This
  confirms the existing `[task=...]` formatting keeps the tag after the Abseil
  line prefix on every user-log line, not at whole-line start.
- Local macOS Debug `asyncflow_runtime_benchmarks` build: passed.
- Local macOS Debug `BM_MpscQueueConcurrentProducers --benchmark_min_time=1x`:
  `/256` 167 us real time, `/4096` 2652 us real time.
- Remote GCC Debug `asyncflow_runtime_benchmarks` clean build: passed.
- Remote GCC Debug `BM_MpscQueueConcurrentProducers --benchmark_min_time=1x`:
  `/256` 120 us real time, `/4096` 1841 us real time.
- Remote Clang Debug `asyncflow_runtime_benchmarks` clean build: passed.
- Remote Clang Debug `BM_MpscQueueConcurrentProducers --benchmark_min_time=1x`:
  `/256` 93.7 us real time, `/4096` 1496 us real time.

Interpretation:

- The framework now has a repeatable canary for MPSC producer-side contention.
  This gives future queue/layout changes a concrete comparison point instead of
  relying on single-threaded queue microbenchmarks.

## 2026-06-03 MPSC Batch Push API

This pass adds `BoundedMpscQueue::try_push_many()` so one producer can reserve
and publish a contiguous run of MPSC slots with one tail CAS. This is a
foundation for future external batch submission and runtime drain batching; the
existing single-item `try_push()` / `try_pop()` behavior is unchanged.

Changes under validation:

- Added `BoundedMpscQueue::try_push_many(T *const *values, std::size_t count)`.
- Added FIFO and full-queue tests for MPSC batch push.
- Added `BM_MpscQueuePushManyPopMany` beside the existing single-item
  `BM_MpscQueuePushPop` benchmark.
- Kept MPSC consumer semantics single-consumer. No locks, allocations, task
  routing changes, or scheduler wake behavior were added.

Correctness and benchmark checks:

- Local macOS Debug build of `asyncflow_runtime_tests` and
  `asyncflow_runtime_benchmarks`: passed.
- Local macOS Debug `QueueTests.*`: 13/13 passed.
- Local macOS Debug MPSC benchmark comparison:
  `PushPop/1024` 35.4 us, `PushManyPopMany/1024` 18.3 us;
  `PushPop/16384` 443 us, `PushManyPopMany/16384` 232 us.
- Local macOS Debug `BM_MpscQueueConcurrentProducers --benchmark_min_time=1x`:
  `/256` 181 us real time, `/4096` 2897 us real time.
- Remote GCC Debug clean build of `asyncflow_runtime_tests` and
  `asyncflow_runtime_benchmarks`: passed.
- Remote GCC Debug `QueueTests.*`: 13/13 passed.
- Remote GCC Debug full `asyncflow_runtime_tests`: 183 tests, 179 passed,
  4 skipped, 0 failed.
- Remote GCC Debug MPSC benchmark comparison:
  `PushPop/1024` 69.8 us, `PushManyPopMany/1024` 67.5 us;
  `PushPop/16384` 1120 us, `PushManyPopMany/16384` 1100 us.
- Remote GCC Debug concurrent producer canary:
  `/256` 101 us real time, `/4096` 1591 us real time.
- Remote Clang Debug clean build of `asyncflow_runtime_tests` and
  `asyncflow_runtime_benchmarks`: passed.
- Remote Clang Debug `QueueTests.*`: 13/13 passed.
- Remote Clang Debug full `asyncflow_runtime_tests`: 183 tests, 179 passed,
  4 skipped, 0 failed.
- Remote Clang Debug MPSC benchmark comparison:
  `PushPop/1024` 52.2 us, `PushManyPopMany/1024` 41.0 us;
  `PushPop/16384` 833 us, `PushManyPopMany/16384` 628 us.
- Remote Clang Debug concurrent producer canary:
  `/256` 109 us real time, `/4096` 1820 us real time.

Interpretation:

- The new API is correctness-validated and gives a measurable single-producer
  batch path, especially under Clang and on local macOS. GCC Debug shows only a
  small gain because this implementation still validates each cell before the
  one CAS reservation. Further gains likely require a dedicated producer-side
  reservation/cache strategy or runtime-level batch drain so producers do less
  per-cell readiness probing.

## 2026-06-03 Executor Drain And Notify Experiments Rejected

This pass evaluated two scheduler hot-path ideas and rejected both after remote
benchmarking:

- Batch-draining SPSC/MPSC queues into an executor-local inbound buffer.
- Skipping repeated external-queue notifications when `external_ready_` was
  already set and the executor did not report itself sleeping.

The batch-drain experiment preserved correctness in local and remote tests, but
it changed the scheduler cost profile enough to regress remote runtime
benchmarks. The notify experiment was narrowed to external queues only, but the
remote result was still mixed and did not justify changing the wake protocol.

Validation and comparison data:

- Local macOS Debug full `asyncflow_runtime_tests`: 189 tests, 103 passed,
  86 skipped, 0 failed.
- Local macOS Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Remote GCC Debug batch-drain experiment:
  `ExternalStart/8192` 11.6 ms, `CrossThreadHop/8192` 20.2 ms.
- Remote Clang Debug batch-drain experiment:
  `ExternalStart/8192` 11.3 ms, `CrossThreadHop/8192` 19.2 ms.
- Remote GCC Debug external-notify-only experiment:
  `ExternalStart/8192` 11.6 ms, `CrossThreadHop/8192` 19.5 ms.
- Remote GCC Debug after reverting the experiments:
  `ExternalStart/8192` 9.20 ms, `CrossThreadHop/8192` 21.0 ms.
- Remote Clang Debug after reverting the experiments:
  `ExternalStart/8192` 11.8 ms, `CrossThreadHop/8192` 19.5 ms.

Interpretation:

- Do not add executor-side inbound drain just to reduce `pop_one()` queue
  probes; the extra buffering and scheduling-shape change are not justified by
  current Linux GCC/Clang results.
- Do not move the generic `notify()` sleeping check ahead of `wake_epoch_`, and
  do not currently special-case repeated external notifications. A future wake
  optimization needs a dedicated benchmark and likely a more explicit
  sleep-transition protocol rather than a local branch reorder.

## 2026-06-03 Semantic Schedule Modes

This pass adds a semantic scheduling mode to `Runtime::post()`, task
`schedule()`, and task `pending_on()`:

- `ScheduleMode::Auto` keeps the default low-overhead runtime-selected route.
- `ScheduleMode::Fast` is accepted only from a runtime thread and favors the
  fastest per-producer route.
- `ScheduleMode::Ordered` preserves one target-thread admission order across
  producers.

The public API deliberately exposes scheduling semantics rather than queue
implementation names. Running-task wake requests now carry both the requested
target thread and the requested schedule mode, so `pending_on(..., Ordered)`
cannot lose its ordering intent when the task transitions from Running to
Pending.

Changes under validation:

- Added `ScheduleMode` and threaded it through `post()`, `schedule()`,
  `pending_on()`, ready enqueue helpers, blocking enqueue helpers, and Pending
  wake requeue.
- Reject `Fast` from non-runtime producers before mutating task state.
- Preserve existing self-post and normal hop behavior under `Auto`.
- Added lifecycle tests for external `Fast` rejection, runtime-thread `Fast`
  acceptance, external `Ordered` acceptance, and `Ordered` Pending wake
  preservation.
- Added `BM_RuntimeScheduleModeHop/Fast` and `/Ordered` benchmark canaries.
- Rechecked `[task=...]` log formatting: the tag stays after the Abseil line
  prefix on every user-log line and never starts a continuation line.

Correctness and benchmark checks:

- Local macOS Debug build of `asyncflow_runtime_tests`,
  `asyncflow_runtime_stress_tests`, `asyncflow_runtime_benchmarks`, and
  `asyncflow_log_tests`: passed.
- Local macOS Debug schedule-mode focused tests: 4/4 passed.
- Local macOS Debug log task-tag focused tests: 2/2 passed.
- Local macOS Debug full `asyncflow_runtime_tests`: 193 tests, 107 passed,
  86 skipped, 0 failed.
- Local macOS Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Local macOS Debug schedule-mode benchmark smoke:
  `Fast/1024` 1.30 ms, `Fast/8192` 9.92 ms;
  `Ordered/1024` 1.01 ms, `Ordered/8192` 9.49 ms.
- Remote GCC Debug, `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.3`,
  `seccomp=unconfined`: build passed for runtime tests, stress tests, runtime
  benchmarks, and log tests.
- Remote GCC Debug schedule-mode focused tests: 4/4 passed.
- Remote GCC Debug log task-tag focused tests: 2/2 passed.
- Remote GCC Debug full `asyncflow_runtime_tests`: 187 tests, 183 passed,
  4 skipped, 0 failed.
- Remote GCC Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Remote GCC Debug schedule-mode benchmark smoke:
  `Fast/1024` 3.51 ms, `Fast/8192` 27.9 ms;
  `Ordered/1024` 3.22 ms, `Ordered/8192` 23.8 ms.
- Remote Clang Debug, `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.3`,
  `seccomp=unconfined`: build passed for runtime tests, stress tests, runtime
  benchmarks, and log tests.
- Remote Clang Debug schedule-mode focused tests: 4/4 passed.
- Remote Clang Debug log task-tag focused tests: 2/2 passed.
- Remote Clang Debug full `asyncflow_runtime_tests`: 187 tests, 183 passed,
  4 skipped, 0 failed.
- Remote Clang Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Remote Clang Debug schedule-mode benchmark smoke:
  `Fast/1024` 3.58 ms, `Fast/8192` 28.2 ms;
  `Ordered/1024` 3.05 ms, `Ordered/8192` 23.0 ms.

Interpretation:

- The API now makes the ordering tradeoff explicit without exposing internal
  queue names to users.
- The state machine rejects invalid `Fast` calls before task-state mutation and
  preserves `Ordered` through Running-to-Pending wake deferral.
- The new benchmark rows are one-iteration Debug canaries, so they are useful
  for catching gross regressions and semantic-path breakage, not for final
  throughput claims.

## 2026-06-03 Platform IO Backend Helper For Examples

This pass reduces platform preprocessor exposure in copyable example runtime
configuration. The framework already exposed `preferred_io_thread_kind` and
`native_io_thread_kind`; this adds a small runtime backend-name helper and
migrates examples to those semantic constants instead of spelling Linux/BSD
branches in each runtime header.

Changes under validation:

- Added `af::runtime_io_backend_name<RuntimeT>(thread)` in `af/platform.hpp`.
- Replaced repeated example runtime choices such as `#if defined(__linux__)`
  `IoUring` else `Io` with `af::preferred_io_thread_kind`.
- Replaced general native readiness examples that hard-coded `Epoll` with
  `af::native_io_thread_kind`.
- Updated TCP echo, TCP connect/accept, socket lifecycle, datagram, and RPC
  example backend messages to use the new helper.
- Left `io_uring_tuned_setup` explicitly on `ThreadKind::IoUring` because that
  example is specifically about io_uring setup parameters and fallback traits.

Correctness checks:

- Local macOS Debug full `all` build: passed, including examples, tests, and
  benchmarks.
- Local macOS Debug full `asyncflow_runtime_tests`: 193 tests, 107 passed,
  86 skipped, 0 failed.
- Local macOS Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Local macOS Debug log task-tag focused tests: 2/2 passed.
- Local macOS Debug example smoke:
  `asyncflow_io_tcp_connect_accept_example` reported `backend=kqueue` and
  completed; `asyncflow_io_socket_lifecycle_example` reported `backend=kqueue`
  and completed; `asyncflow_io_uring_datagram_example` reported
  `backend=kqueue` and completed; Linux-only RPC and io_uring-timeout examples
  exited cleanly through capability checks.
- Remote GCC Debug, `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.3`,
  `seccomp=unconfined`: full `all` build passed.
- Remote GCC Debug full `asyncflow_runtime_tests`: 187 tests, 183 passed,
  4 skipped, 0 failed.
- Remote GCC Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Remote GCC Debug log task-tag focused tests: 2/2 passed.
- Remote GCC Debug example smoke passed:
  TCP connect/accept `backend=io_uring`, socket lifecycle
  `backend=epoll-fallback`, datagram `backend=io_uring`, RPC
  `backend=io_uring`, and io_uring timeout fired.
- Remote Clang Debug, `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.3`,
  `seccomp=unconfined`: full `all` build passed.
- Remote Clang Debug full `asyncflow_runtime_tests`: 187 tests, 183 passed,
  4 skipped, 0 failed.
- Remote Clang Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Remote Clang Debug log task-tag focused tests: 2/2 passed.
- Remote Clang Debug example smoke passed with the same backend outcomes as GCC.

Interpretation:

- Example runtime configuration is now driven by framework-provided platform
  capability constants, so users copying normal examples no longer need to
  write Linux/BSD preprocessor branches just to select an IO thread kind or
  print a backend name.
- Low-level Linux-only example task bodies still contain platform guards around
  system headers and Linux-specific syscalls. Those are implementation details
  of specific examples and remain separate from the public scheduling/runtime
  configuration style.

## 2026-06-03 Event/Timer Wait Platform Branch Encapsulation

This pass moves the `eventfd`/`timerfd` wait-read platform branch out of the
public `io_event_timer.hpp` API surface and into the common IO detail layer.

Changes under validation:

- Added `detail::io_wait_uint64_counter_fd()` in
  `io_common_event_timer.hpp`.
- Kept the Linux implementation behavior unchanged: clear waiting state, retry
  `read()` on `EINTR`, arm readiness wait on would-block, return `EIO` on
  short reads, and publish the 64-bit counter on success.
- Kept the non-Linux behavior unchanged: public calls still validate
  cancellation/null output first and then return `ENOSYS`.
- Simplified `io_wait_eventfd()` and `io_wait_timerfd()` so the public header no
  longer contains `__linux__` conditional compilation.
- Rechecked log task-tag formatting based on the latest requirement:
  `[task=...]` may repeat on continuation lines, but it remains after the
  Abseil line prefix and never starts a physical log line.

Correctness checks:

- Local macOS Debug full `all` build: passed.
- Local macOS Debug full `asyncflow_runtime_tests`: 193 tests, 107 passed,
  86 skipped, 0 failed.
- Local macOS Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Local macOS Debug task-tag focused `asyncflow_log_tests`: 5/5 passed.
- Remote GCC Debug, `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.3`,
  `seccomp=unconfined`: full `all` build passed.
- Remote GCC Debug full `asyncflow_runtime_tests`: 187 tests, 183 passed,
  4 skipped, 0 failed.
- Remote GCC Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Remote GCC Debug event/timer example smoke passed:
  `event value=7`, `timer expirations=1`, and `io_uring timeout fired`.
- Remote Clang Debug, `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.3`,
  `seccomp=unconfined`: full `all` build passed.
- Remote Clang Debug full `asyncflow_runtime_tests`: 187 tests, 183 passed,
  4 skipped, 0 failed.
- Remote Clang Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Remote Clang Debug event/timer example smoke passed:
  `event value=7`, `timer expirations=1`, and `io_uring timeout fired`.

Interpretation:

- Normal users including `af/io_event_timer.hpp` no longer see the Linux
  platform branch for event/timer wait operations.
- The branch remains close to the syscall-specific detail implementation, where
  it belongs, without adding locks, allocations, state transitions, or runtime
  scheduling behavior.

## 2026-06-03 Public IO Platform Header Cleanup

This pass continues reducing platform preprocessor exposure from public IO and
runtime headers. It centralizes system-header and platform type declarations in
detail headers while keeping the public API signatures and behavior unchanged.

Changes under validation:

- Added `io_types_platform.hpp` for POSIX/Linux IO system headers and the
  non-Linux `statx` forward declaration.
- Added `io_filesystem_platform.hpp` for Linux `open_how`/`statx` definitions
  and non-Linux forward declarations.
- Added `runtime_platform_headers.hpp` for runtime executor platform headers
  such as pthread, epoll, eventfd, mmap, and kqueue.
- Simplified `io_types.hpp`, `io_filesystem.hpp`, `runtime_public_io.hpp`, and
  `async_runtime.hpp` to include those detail headers instead of spelling
  repeated platform branches in public-facing headers.
- Rechecked public headers at this point: the remaining platform macros were
  concentrated in `platform.hpp` capability constants and `signal.hpp` signal
  handling support.

Correctness checks:

- Local macOS Debug full `all` build: passed.
- Local macOS Debug full `asyncflow_runtime_tests`: 193 tests, 107 passed,
  86 skipped, 0 failed.
- Local macOS Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Remote GCC Debug, `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.3`,
  `seccomp=unconfined`: full `all` build passed.
- Remote GCC Debug full `asyncflow_runtime_tests`: 187 tests, 183 passed,
  4 skipped, 0 failed.
- Remote GCC Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Remote Clang Debug, `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.3`,
  `seccomp=unconfined`: full `all` build passed.
- Remote Clang Debug full `asyncflow_runtime_tests`: 187 tests, 183 passed,
  4 skipped, 0 failed.
- Remote Clang Debug `asyncflow_runtime_stress_tests`: 9/9 passed.

Interpretation:

- Public API users now see fewer raw platform branches when including the core
  IO/runtime headers.
- This is an include-boundary cleanup only; it does not add locks, atomics,
  allocations, scheduler branches, or syscall-path behavior changes.

## 2026-06-03 Platform And Signal Public Header Cleanup

This pass removes the remaining public platform preprocessor branches from
`platform.hpp` and `signal.hpp` while preserving the same public constants and
signal-waiting API.

Changes under validation:

- Added detail platform constexpr values in `config.hpp`; `platform.hpp` now
  exports `af::platform_*` and `af::supports_*` as simple public constants
  without spelling platform preprocessor branches.
- Added `detail/signal/signal_platform.hpp` and moved POSIX/Windows signal
  storage, `pthread_sigmask`, `sigwait`, `sigtimedwait`, fallback polling, and
  signal-ignore implementation into detail.
- Reduced `signal.hpp` to a small public API shell: `SignalWaitResult`,
  `SignalSet`, `make_termination_signal_set()`, and
  `ignore_process_signal()`.
- Rechecked public headers with a platform-macro scan under `include/af`
  excluding `include/af/detail`: no matches remained for the project platform
  macro patterns.

Correctness checks:

- Local macOS Debug full `all` build: passed.
- Local macOS Debug full `asyncflow_runtime_tests`: 193 tests, 107 passed,
  86 skipped, 0 failed.
- Local macOS Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Local macOS Debug `SignalTests.*`: 9/9 passed.
- Remote GCC Debug, `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.3`,
  `seccomp=unconfined`: full `all` build passed.
- Remote GCC Debug full `asyncflow_runtime_tests`: 187 tests, 183 passed,
  4 skipped, 0 failed.
- Remote GCC Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Remote GCC Debug `SignalTests.*`: 9/9 passed.
- Remote Clang Debug, `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.3`,
  `seccomp=unconfined`: full `all` build passed.
- Remote Clang Debug full `asyncflow_runtime_tests`: 187 tests, 183 passed,
  4 skipped, 0 failed.
- Remote Clang Debug `asyncflow_runtime_stress_tests`: 9/9 passed.
- Remote Clang Debug `SignalTests.*`: 9/9 passed.

Interpretation:

- Users including public framework headers no longer need to read through raw
  platform branches for platform capability constants or signal support.
- The change does not alter scheduler queues, IO submission paths, locking,
  atomics, allocation behavior, or signal API semantics.
