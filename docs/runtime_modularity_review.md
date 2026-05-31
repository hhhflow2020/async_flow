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
- Public IO adapter headers are now compatibility umbrellas: `io_socket.hpp`, `io_file.hpp`, and `io_adapters.hpp` include focused inline fragments for lifecycle, data transfer, fixed resources, stream/listener, datagram, and event/timer adapters.
- io_uring socket test support and runtime socket test sources have been split by stream, datagram, accept/connect, and multishot responsibilities.
- `runtime_executor_io_uring_submit_core_fragment.hpp` is now a small umbrella over poll wait submit, buffer/fast SQE submit, and generic SQE submit fragments. The code still lives inside `AsyncRuntime::Executor` for inline visibility.
- `runtime_executor_io_uring_socket_submit_fragment.hpp` is now a small umbrella over recv, send, zero-copy, message, multishot, accept/connect, and socket-create submit wrappers.
- Runtime lifecycle tests have been split into base lifecycle, backpressure, and shutdown-policy sources with shared traits/tasks in support.
- Runtime lifecycle support is now a small umbrella over base, backpressure, and shutdown-policy task fragments.
- Runtime parallel tests have been split into shard scheduling, ordered-start, and ordered-batch sources with shared task support.
- Runtime parallel support is now a small umbrella over core runtime fixtures, shard tasks, ordered-batch tasks, and ordered-start tasks.
- Stream IO test support is now a small umbrella over connect, basic stream, vectored, zero-copy boundary, zero-copy send, and sendfile/splice task fragments.
- Each split so far preserved header-only/template visibility, passed `git diff --check`, Docker GCC Debug runtime tests, and, for core runtime header changes, Release runtime benchmark baseline regression.

## Current Findings

### P1: io_uring Executor Internals Can Be Split Further

The largest remaining runtime-internal fragments are `runtime_executor_io_uring_backend_fragment.hpp`, `runtime_executor_io_uring_file_data_submit_fragment.hpp`, the generic SQE submit fragment, and io_uring resource management.

Risk:
- CQE completion, multishot continuation, zero-copy notification, timeout/cancel, fallback bookkeeping, file data submit wrappers, and resource registration are still close together in a few dense executor fragments.
- Concurrency/lifetime audits still require reading several dense io_uring fragments.

Recommended split:
- Split backend completion into CQE decoding, normal completion, multishot continuation, zero-copy notification, timeout/cancel, and teardown paths.
- Split file data submit wrappers by normal read/write, positioned/vectored, fixed files, fixed buffers, and filesystem-sync helpers.
- Consider splitting the generic SQE submit fragment into validation, operation preparation, and SQE filling helpers, but only if the helper shape stays inline and does not add hot-path dispatch.
- Keep fragments included inside `AsyncRuntime::Executor` so hot submit helpers stay inlineable and no extra virtual/function-pointer dispatch is introduced.

### P2: Core Runtime Tests Still Have A Few Large Domain Files

The io_uring socket, runtime lifecycle, runtime parallel, and stream IO support files are now split, but several IO support fragments and IO example/benchmark support files are still long.

Recommended split:
- Split fixed-resource file task support by fixed-file table setup, registered buffers, fixed read/write, and batch submit helpers.
- Split timer/event and wait/cancel task support by timerfd/eventfd, deadline/cancel, and shutdown/cleanup cases.

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
