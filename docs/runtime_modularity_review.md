# Runtime Modularity Review

Date: 2026-05-31

## Scope

This note tracks modularity and responsibility-boundary issues found while reviewing the fixed-thread runtime, IO executor, tests, benchmarks, and examples.

The runtime is intentionally header-only/template-visible for hot path inlining. Refactors should therefore prefer internal include fragments or small inline headers over moving performance-sensitive code to `.cpp` files.

## Already Improved

- `include/af/async_runtime.hpp` has been split into focused runtime fragments for common helpers, dispatch, lifecycle, parallel support, state, executor control, executor task helpers, readiness wait/cancel, backend polling, poll helper conversion, executor state, and io_uring resource registration.
- The public `AsyncRuntime` API is now split into lifecycle, parallel, IO resource/wait, file-data submit, fd lifecycle submit, socket data submit, and socket message/connect submit fragments. The top-level `include/af/async_runtime.hpp` is now an overview-sized class shell instead of a multi-thousand-line mixed implementation file.
- `tests/runtime_io_test_support.hpp` is now an umbrella header with domain fragments for core traits, basic tasks, stream, accept, file, timer/event, wait/cancel, socket lifecycle, and io_uring socket support.
- The file IO test support has been split into boundary, normal read/write, fixed-resource, lifecycle/open, and filesystem operation fragments.
- Each split so far preserved header-only/template visibility, passed `git diff --check`, Docker GCC Debug runtime tests, and, for core runtime header changes, Release runtime benchmark baseline regression.

## Current Findings

### P1: Public IO Adapter Headers Are Now The Largest API Files

`include/af/io_socket.hpp`, `include/af/io_file.hpp`, and `include/af/io_adapters.hpp` are the largest public headers after the runtime split. They are still coherent, but future IO additions will make them harder to audit unless the next features land in smaller domain headers.

Risk:
- Socket lifecycle, stream read/write, datagram, fixed-file, timeout/cancel, eventfd/timerfd, and generic adapter helpers can become visually interleaved.
- Public API review may require scanning unrelated network and file helpers.

Recommended split:
- `io_socket_stream.hpp`
- `io_socket_datagram.hpp`
- `io_socket_lifecycle.hpp`
- `io_file_data.hpp`
- `io_file_lifecycle.hpp`
- `io_event_timer.hpp`
- `io_pollable_adapter.hpp`

Keep `io_socket.hpp`, `io_file.hpp`, and `io_adapters.hpp` as compatibility umbrella headers.

### P1: io_uring Executor Internals Can Be Split Further

The largest remaining runtime-internal fragments are `runtime_executor_io_uring_submit_core_fragment.hpp`, `runtime_executor_io_uring_socket_submit_fragment.hpp`, and `runtime_executor_io_uring_backend_fragment.hpp`.

Risk:
- Operation construction, SQE preparation, CQE completion, multishot handling, and fallback bookkeeping are still close together.
- Concurrency/lifetime audits still require reading several dense io_uring fragments.

Recommended split:
- Split submit core into operation allocation/release, SQE acquisition/submission, CQE decoding, and completion dispatch fragments.
- Split socket submit by stream, datagram, accept/connect, multishot, and zero-copy send.
- Keep fragments included inside `AsyncRuntime::Executor` so hot submit helpers stay inlineable and no extra virtual/function-pointer dispatch is introduced.

### P2: IO Tests Still Have A Few Large Domain Files

The support umbrella is now small, but `tests/support/runtime_io_uring_socket_tasks_fragment.hpp` and `tests/runtime_io_uring_socket_tests.cpp` are still long.

Recommended split:
- Split io_uring socket task support by stream, datagram, multishot, direct accept, and zero-copy send.
- Split io_uring socket tests into stream, datagram, multishot, and direct/fixed-file test files.

### P2: README IO Section Has Become Dense

The README now documents a large amount of IO behavior in one long section. It is useful but hard to scan.

Recommended split:
- Keep README focused on quick start and core semantics.
- Move deep IO behavior, performance tuning, fixed files, registered buffers, multishot, timeout/cancel, and zero-copy guidance into `docs/io_runtime.md`.
- Move benchmark and CI baseline details into `docs/performance.md`.

## Performance Constraints For Refactors

- Preserve the fixed-thread ownership model: syscall, io_uring submit, completion, readiness fallback, and task resume all stay on the owning IO executor.
- Keep hot helpers inlineable by using fragments or inline headers.
- Do not introduce `std::function`, heap allocation, virtual dispatch, or cross-thread MPMC queues on hot IO paths.
- Keep operation metadata in executor-local object pools.
- Preserve cache-line alignment for executor state, queue indices, and high-contention atomics.
- Keep branch-heavy fallback paths behind cold/error checks where practical.
- Preserve submit batching and eventfd wake coalescing.

## Validation Checklist

For each split:

- Run `git diff --check`.
- Build Docker GCC Debug with `--parallel 2`.
- Run focused runtime/IO/stress tests with `ctest -j2`.
- Build Release benchmark target with `--parallel 2`.
- Run `BM_Runtime*` benchmark and `scripts/check_benchmark_regression.py`.
- Commit each independent refactor after it passes validation.
