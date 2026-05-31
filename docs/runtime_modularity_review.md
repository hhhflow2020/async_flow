# Runtime Modularity Review

Date: 2026-05-31

## Scope

This note tracks modularity and responsibility-boundary issues found while reviewing the fixed-thread runtime, IO executor, tests, benchmarks, and examples.

The runtime is intentionally header-only/template-visible for hot path inlining. Refactors should therefore prefer internal include fragments or small inline headers over moving performance-sensitive code to `.cpp` files.

## Already Improved

- `include/af/async_runtime.hpp` has been split into focused runtime fragments for public task handles/config, common helpers, dispatch, lifecycle, parallel support, state, executor control, executor task helpers, executor IO state types, executor thread-kind helpers, readiness wait/cancel, backend polling, poll helper conversion, executor state, and io_uring resource registration.
- The public `AsyncRuntime` API is now split into lifecycle, parallel, IO resource/wait, file-data submit, fd lifecycle submit, socket data submit, and socket message/connect submit fragments. The top-level `include/af/async_runtime.hpp` is now an overview-sized class shell instead of a multi-thousand-line mixed implementation file.
- `tests/runtime_io_test_support.hpp` is now an umbrella header with domain fragments for core traits, basic tasks, stream, accept, file, timer/event, wait/cancel, socket lifecycle, and io_uring socket support.
- The file IO test support has been split into boundary, normal read/write, fixed-resource, lifecycle/open, and filesystem operation fragments.
- Public IO adapter headers are now compatibility umbrellas: `io_socket.hpp`, `io_file.hpp`, and `io_adapters.hpp` include focused inline fragments for lifecycle, data transfer, fixed resources, stream/listener, datagram, and event/timer adapters.
- `include/af/io_datagram.hpp` is now an umbrella over focused datagram recv, send, vectored, and zero-copy helper fragments.
- io_uring socket test support and runtime socket test sources have been split by stream, datagram, accept/connect, and multishot responsibilities.
- `runtime_executor_io_uring_submit_core_fragment.hpp` is now a small umbrella over poll wait submit, buffer/fast SQE submit, and generic SQE submit fragments. The code still lives inside `AsyncRuntime::Executor` for inline visibility.
- `runtime_executor_io_uring_socket_submit_fragment.hpp` is now a small umbrella over recv, send, zero-copy, message, multishot, accept/connect, and socket-create submit wrappers.
- io_uring resource registration is now split by registered buffers, provided buffer rings, and registered/fixed file table helpers while staying inline inside `AsyncRuntime::Executor`.
- Public fd lifecycle submit wrappers and matching io_uring executor submit wrappers are now small umbrellas over open, close/shutdown, filesystem metadata/lifecycle, and splice transfer fragments.
- Public socket message submit wrappers are now split into recvmsg, sendmsg, and accept/connect fragments, with a small umbrella preserving inline visibility in `AsyncRuntime`.
- Runtime lifecycle tests have been split into base lifecycle, backpressure, and shutdown-policy sources with shared traits/tasks in support.
- Runtime lifecycle support is now a small umbrella over base, backpressure, and shutdown-policy task fragments.
- Runtime parallel tests have been split into shard scheduling, ordered-start, and ordered-batch sources with shared task support.
- Runtime parallel support is now a small umbrella over core runtime fixtures, shard tasks, ordered-batch tasks, and ordered-start tasks.
- Stream IO test support is now a small umbrella over connect, basic stream, vectored, zero-copy boundary, zero-copy send, and sendfile/splice task fragments.
- Stream IO runtime tests are split by basic stream, vectored send/recv, zero-copy send, fd-to-fd transfer, and connect/accept coverage.
- Timer/event IO test support is now a small umbrella over timer/timeout tasks, eventfd tasks, timer/event boundary tasks, and filesystem boundary tasks.
- Wait/cancel IO test support is now a small umbrella over basic wait/bad-fd tasks, cancel state-machine tasks, deadline timeout tasks, and zero-byte/vectored boundary tasks.
- Epoll runtime tests are split by setup, readiness, cancel/timeout, boundary, socket lifecycle, and event/timer adapter coverage.
- io_uring backend executor internals are now split into setup/close, SQ submit/poll, CQ completion, and operation lifecycle fragments while remaining inline in `AsyncRuntime::Executor`.
- io_uring fixed file/buffer test support is now a small umbrella over fixed-buffer, fixed-file read/write, fixed-file update, and openat-direct task fragments.
- IO benchmark support now keeps the hot benchmark-facing fake task shell small and splits FakeRuntime stubs by Linux socket, POSIX message, POSIX fixed file, accept/connect, and filesystem helpers.
- io_uring file-data submit wrappers are now split into basic read/write, timeout, fixed-file, fixed-buffer, vectored, and fsync fragments while staying inline in `AsyncRuntime::Executor`.
- Native readiness backends now have a platform-dispatch include point: Linux uses an epoll fragment and macOS/BSD uses a kqueue fragment, while public `io_*` helpers continue to expose one API. This keeps OS-specific syscall code out of the generic executor loop and preserves header-only inlining.
- `include/af/io_common.hpp` is now a small umbrella over focused common fragments: basic socket/error helpers, wait-state helpers, fixed-file vectored helpers, Linux eventfd/timerfd helpers, and deadline state.
- The macOS/BSD kqueue backend is split by setup, timeout, poll, storage, wait, and event translation helpers. kqueue now supports native one-shot timeout completion and cancel for `io_wait_timeout()` / `arm_io_timeout()` without routing through Linux `timerfd`.
- Each split so far preserved header-only/template visibility, passed `git diff --check`, Docker GCC Debug runtime tests, and, for core runtime header changes, Release runtime benchmark baseline regression.

## Current Findings

### P1: io_uring Executor Internals Can Be Split Further

The largest remaining runtime-internal fragments are io_uring completion/submit details and file data/public socket data submit wrappers.

Risk:
- CQE completion details and file data/public socket data submit wrappers still have dense local regions.
- Concurrency/lifetime audits still require reading several dense io_uring fragments.

Recommended split:
- Consider a second-pass split of backend completion into CQE decoding, normal completion, multishot continuation, zero-copy notification, timeout/cancel, and teardown paths if further audit needs it.
- Consider a follow-up split of io_uring setup tuning helpers if more backend setup knobs are added.
- Consider splitting the generic SQE submit fragment into validation, operation preparation, and SQE filling helpers, but only if the helper shape stays inline and does not add hot-path dispatch.
- Keep fragments included inside `AsyncRuntime::Executor` so hot submit helpers stay inlineable and no extra virtual/function-pointer dispatch is introduced.

### P1: Cross-Platform IO Backend Boundaries Need To Stay Explicit

The Linux epoll/io_uring path and the macOS/BSD kqueue readiness path should not share syscall-level implementation files.

Risk:
- Mixing platform-specific readiness, wake, cancel, and timeout logic in generic runtime files makes correctness audits harder and encourages accidental Linux-only assumptions in portable helpers.
- kqueue and epoll have different one-shot/delete semantics; hiding those differences behind ad hoc `#if` blocks inside large files increases the chance of stale registrations or double task resume bugs.

Recommended split:
- Keep generic executor scheduling in `runtime_executor_backend_fragment.hpp`.
- Keep each native readiness backend in a dedicated fragment selected by macros from `runtime_executor_native_io_backend_fragment.hpp`.
- Add future macOS event helpers as kqueue-specific fragments rather than extending Linux `timerfd/eventfd` adapters.
- Prefer `ThreadKind::Io` for portable readiness threads; use `ThreadKind::Epoll`, `ThreadKind::Kqueue`, and `ThreadKind::IoUring` only when a caller intentionally requires a specific backend.

### P2: Core Runtime Tests Still Have A Few Large Domain Files

The io_uring socket, runtime lifecycle, runtime parallel, stream IO, and fixed-resource file support files are now split, but several IO support fragments and IO example/benchmark support files are still long.

Recommended split:
- Continue moving long fixture sources toward small domain-focused test files as new cases are added.
- Keep platform-specific tests in separate files such as `runtime_io_epoll_tests.cpp`, `runtime_io_kqueue_tests.cpp`, and io_uring-focused files; shared task fixtures should remain in small support fragments.

### P2: README IO Section Has Become Dense

The README now documents a large amount of IO behavior in one long section. It is useful but hard to scan.

Recommended split:
- Keep README focused on quick start and core semantics.
- Move deep IO behavior, performance tuning, fixed files, registered buffers, multishot, timeout/cancel, and zero-copy guidance into `docs/io_runtime.md`.
- Move benchmark and CI baseline details into `docs/performance.md`.

## Remaining IO Gaps

- macOS/BSD now has a native kqueue readiness backend and one-shot timeout support. Event/user-trigger helpers are still Linux-specific (`eventfd`) or io_uring-specific; next step: add kqueue user-event helpers behind the same public event adapter shape.
- Portable network IO now has a common `ThreadKind::Io` entry point, but some examples and tests still intentionally target Linux-only features such as sendfile/splice, eventfd/timerfd, fixed files, provided buffers, multishot, and io_uring direct descriptors.
- File lifecycle helpers (`openat2/statx/fallocate/renameat/unlinkat/close`) remain Linux/io_uring-centered. For macOS, decide whether the first portable file layer should be explicit IO-thread synchronous syscalls, POSIX AIO, or a separate future backend; do not hide fundamentally different file semantics behind the same high-performance claim.
- Per-operation timeout/cancel is implemented for epoll readiness, io_uring completion, and kqueue readiness/timeout completion.
- Continue checking fixture sources as new IO capabilities land, keeping new tests in focused files instead of rebuilding a monolithic IO test source.

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
