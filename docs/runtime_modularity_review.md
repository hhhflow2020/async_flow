# Runtime Modularity Review

Date: 2026-05-31

## Scope

This note tracks modularity and responsibility-boundary issues found while reviewing the fixed-thread runtime, IO executor, tests, benchmarks, and examples.

The runtime is intentionally header-only/template-visible for hot path inlining. Refactors should therefore prefer internal include fragments or small inline headers over moving performance-sensitive code to `.cpp` files.

## Already Improved

- `include/af/async_runtime.hpp` has been split into focused runtime fragments for common helpers, dispatch, lifecycle, parallel support, state, executor control, executor task helpers, readiness wait/cancel, backend polling, poll helper conversion, executor state, and io_uring resource registration.
- The latest split moved io_uring buffer/file/provided-buffer registration and update logic into `include/af/detail/runtime_executor_io_uring_resource_fragment.hpp`, leaving the public `Executor` surface at that point as an include-only boundary.
- Validation after the latest split passed `git diff --check`, Docker GCC Debug runtime tests, and Release runtime benchmark baseline regression.

## Current Findings

### P1: `async_runtime.hpp` Still Mixes Public API and Executor io_uring Submit Logic

`include/af/async_runtime.hpp` is still about 5.9k lines. The first half exposes many public forwarding APIs, while the nested `Executor` still contains a long io_uring submit block and generic operation construction/completion logic.

Risk:
- The file remains hard to audit for concurrency and lifetime invariants.
- Preprocessor boundaries around Linux-only code are easy to break during future changes.
- Reviewers must scan unrelated socket, file, timeout, fixed-file, multishot, and zero-copy code in one place.

Recommended split:
- `runtime_executor_io_uring_file_submit_fragment.hpp`
- `runtime_executor_io_uring_socket_submit_fragment.hpp`
- `runtime_executor_io_uring_datagram_submit_fragment.hpp`
- `runtime_executor_io_uring_submit_core_fragment.hpp`
- `runtime_executor_io_uring_completion_fragment.hpp`
- `runtime_executor_io_uring_backend_fragment.hpp`

Keep these included inside `AsyncRuntime::Executor` so hot submit helpers stay inlineable and no extra virtual/function-pointer dispatch is introduced.

### P1: `tests/runtime_io_test_support.hpp` Is Too Large

`tests/runtime_io_test_support.hpp` is currently the largest file at about 6.5k lines. It mixes runtime traits, fixtures, epoll tasks, stream tasks, datagram tasks, io_uring file tasks, io_uring socket tasks, timeout/cancel helpers, and filesystem helpers.

Risk:
- Test coverage is broad, but the support file is difficult to navigate.
- Adding new IO edge cases tends to grow one shared file instead of a domain-local helper.
- Build errors from one helper can be noisy and unrelated to the test being changed.

Recommended split:
- `tests/support/runtime_io_traits.hpp`
- `tests/support/runtime_io_fixtures.hpp`
- `tests/support/runtime_io_epoll_tasks.hpp`
- `tests/support/runtime_io_stream_tasks.hpp`
- `tests/support/runtime_io_datagram_tasks.hpp`
- `tests/support/runtime_io_uring_file_tasks.hpp`
- `tests/support/runtime_io_uring_socket_tasks.hpp`
- `tests/support/runtime_io_timeout_cancel_tasks.hpp`

The test `.cpp` files should include only the support headers they need.

### P2: Public Runtime Forwarders Are Verbose

The public `AsyncRuntime` section contains many small wrappers that validate thread index and forward to the executor. This is not a runtime performance problem, but it makes the core type visually heavy.

Recommended split:
- `runtime_public_task_api_fragment.hpp`
- `runtime_public_io_wait_api_fragment.hpp`
- `runtime_public_io_uring_api_fragment.hpp`
- `runtime_public_parallel_api_fragment.hpp`

This should be done only after executor internals are split further, because the executor code is the larger audit burden.

### P2: README IO Section Has Become Dense

The README now documents a large amount of IO behavior in one long section. It is useful but hard to scan.

Recommended split:
- Keep README focused on quick start and core semantics.
- Move deep IO behavior, performance tuning, fixed files, registered buffers, multishot, timeout/cancel, and zero-copy guidance into `docs/io_runtime.md`.
- Move benchmark and CI baseline details into `docs/performance.md`.

### P3: IO Adapter Headers Are Manageable But Near Future Split Points

`include/af/io_socket.hpp`, `include/af/io_file.hpp`, and `include/af/io_adapters.hpp` are still within a manageable range, but they are natural split points if more protocol helpers are added.

Possible future split:
- Socket lifecycle helpers.
- Stream read/write helpers.
- Datagram helpers.
- Fixed-file helpers.
- Eventfd/timerfd adapters.

Do this only when new features make the current files noticeably harder to review.

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
