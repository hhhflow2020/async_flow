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
- The existing IO adapter benchmarks cover helper-level fast paths, not a long-running live epoll readiness loop. A dedicated readiness rearm benchmark should be added before using syscall-rate numbers as a hard performance gate.

## 2026-06-01 Runtime Common-State Split Validation

This run validates the split of `runtime_common_fragment.hpp` into focused class-scope fragments and the removal of the now-obsolete `io_deferred_delete_reserve` public tuning knob.

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
- The split separates the IO-thread accept/read/write server state machine from the logic-thread request processing task while preserving the explicit `rpc_async::post(RpcThread::IO_0, server_)` handoff.

## 2026-06-01 io_uring Fixed-File Task Split Validation

This run validates the example support split that turned `examples/support/io_uring_fixed_file_task.hpp` into a small task shell over flow, IO-operation, and registration/update fragments.

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

Correctness and race checks:

- Local `git diff --check`: passed.
- Local Release `asyncflow_io_uring_recv_multishot_example` build: passed; local run reported `io_uring recv_multishot example is Linux-only`.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug `asyncflow_io_uring_recv_multishot_example` build/run: passed with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_io_uring_recv_multishot_example` build/run: passed with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_uring_recv_multishot_example` build/run: passed with `io_uring backend unavailable`.

Interpretation:

- This pass changed only example support layout. Runtime scheduling, IO backend behavior, queue selection, memory ordering, and public APIs were unchanged.
- The remote host/container combination did not expose the io_uring backend path for this example, so this validates build/link/run and TSAN startup/teardown cleanliness rather than the provided-buffer stream recv multishot data path itself.

## 2026-06-01 io_uring File Lifecycle Test Support Split Validation

This run validates the test support split that turned `tests/support/runtime_io_file_lifecycle_tasks_fragment.hpp` into a 63-line task shell over flow, file-operation, and namespace-operation fragments.

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
- `object_pool.hpp` is now a small shell over storage, slot acquire/release, and lifecycle fragments.
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
