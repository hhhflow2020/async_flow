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
