# Runtime Modularity Review

Date: 2026-05-31

## Scope

This note tracks modularity and responsibility-boundary issues found while reviewing the fixed-thread runtime, IO executor, tests, benchmarks, and examples.

The runtime is intentionally header-only/template-visible for hot path inlining. Refactors should therefore prefer internal include fragments or small inline headers over moving performance-sensitive code to `.cpp` files.

## Already Improved

- `include/af/async_runtime.hpp` has been split into focused runtime fragments for public task handles/config, common helpers, dispatch, lifecycle, parallel support, state, executor control, executor task helpers, executor IO state types, executor thread-kind helpers, readiness wait/cancel, backend polling, poll helper conversion, executor state, and io_uring resource registration.
- The public `AsyncRuntime` API is now split into lifecycle, parallel, IO resource/wait, file-data submit, fd lifecycle submit, socket data submit, and socket message/connect submit fragments. The top-level `include/af/async_runtime.hpp` is now an overview-sized class shell instead of a multi-thousand-line mixed implementation file.
- `tests/runtime_io_test_support.hpp` is now an umbrella header with domain fragments for core traits, basic tasks, stream, accept, file, timer/event, wait/cancel, socket lifecycle, and io_uring socket support.
- Basic socket IO test support is now split into stream read/write, UDP datagram, and UDP vectored task fragments with the original basic-socket header kept as an umbrella.
- Socket lifecycle test support is now split into setup happy path, boundary validation, io_uring socket-create, and fast IO wait/done task fragments with the original lifecycle header kept as an umbrella.
- The file IO test support has been split into boundary, normal read/write, fixed-resource, lifecycle/open, and filesystem operation fragments.
- Public IO adapter headers are now compatibility umbrellas: `io_socket.hpp`, `io_file.hpp`, and `io_adapters.hpp` include focused inline fragments for lifecycle, data transfer, fixed resources, file descriptors/fixed files, stream/listener, datagram, and event/timer adapters.
- `include/af/io_datagram.hpp` is now an umbrella over focused datagram recv, send, vectored, and zero-copy helper fragments.
- io_uring socket test support and runtime socket test sources have been split by stream, datagram, accept/connect, and multishot responsibilities.
- io_uring file runtime tests are now split by basic file data, fixed resources/direct descriptors, submit batching, and lifecycle/filesystem operation responsibilities.
- `runtime_executor_io_uring_submit_core_fragment.hpp` is now a small umbrella over poll wait submit, buffer/fast SQE submit, and generic SQE submit fragments. The code still lives inside `AsyncRuntime::Executor` for inline visibility.
- `runtime_executor_io_uring_socket_submit_fragment.hpp` is now a small umbrella over recv, send, zero-copy, message, multishot, accept/connect, and socket-create submit wrappers.
- io_uring resource registration is now split by registered buffers, provided buffer rings, and registered/fixed file table helpers while staying inline inside `AsyncRuntime::Executor`.
- io_uring executor buffer submit helpers are now split into generic buffer SQE, fast SQE template, socket-create core, and fixed-file read/write fragments while staying inline inside `AsyncRuntime::Executor`.
- io_uring generic submit is now split into argument classification, validation, operation preparation, SQE filling, and the small core submit flow while staying inline inside `AsyncRuntime::Executor`.
- Public fd lifecycle submit wrappers and matching io_uring executor submit wrappers are now small umbrellas over open, close/shutdown, filesystem metadata/lifecycle, and splice transfer fragments.
- Public socket message submit wrappers are now split into recvmsg, sendmsg, and accept/connect fragments, with a small umbrella preserving inline visibility in `AsyncRuntime`.
- Public socket data submit wrappers are now a small umbrella over basic recv, recv multishot, basic send, and zero-copy send fragments. Public API validation and executor handoff remain inline in `AsyncRuntime`.
- Public socket accept/connect helpers are now a small umbrella over accept, accept-direct, accept-multishot, and connect fragments. POSIX fallback, io_uring direct accept, multishot completion handling, and connect readiness fallback are now separate while staying inline.
- Public file-data submit wrappers are now split into basic read/write/fsync, fixed-file, registered-buffer, and vectored fragments, with a small umbrella preserving inline visibility in `AsyncRuntime`.
- Public socket receive helpers are now split into basic recv/fixed-file recv, recv multishot, and recvmsg multishot parser/submit fragments, with `io_socket_recv_fragment.hpp` kept as a small inline umbrella.
- Public socket send helpers are now split into basic send, fixed-file send, zero-copy send, and vectored zero-copy send fragments, with `io_socket_send_fragment.hpp` kept as a small inline umbrella.
- Public file fixed-resource helpers are now split into fixed-file read/write/fsync, registered-buffer read/write, and vectored file write fragments, with `io_file_fixed_buffer_fragment.hpp` kept as a small inline umbrella.
- Public file lifecycle helpers are now split into open/open-direct, close/fsync, metadata, and namespace-operation fragments, with `io_file_lifecycle_fragment.hpp` kept as a small inline umbrella.
- Public stream/listener adapters are now split into `IoStream` and `IoListener` fragments, with `io_adapters_stream_listener_fragment.hpp` kept as a small inline umbrella.
- Runtime lifecycle tests have been split into base lifecycle, backpressure, and shutdown-policy sources with shared traits/tasks in support.
- Runtime lifecycle support is now a small umbrella over base, backpressure, and shutdown-policy task fragments.
- Runtime parallel tests have been split into shard scheduling, ordered-start, and ordered-batch sources with shared task support.
- Runtime parallel support is now a small umbrella over core runtime fixtures, shard tasks, ordered-batch tasks, and ordered-start tasks.
- Core runtime parallel implementation is now a small inline umbrella over ordered-start state, ordered-batch guard, shard runner/task, and shard dispatch fragments. This keeps the hot template path visible while separating sequencing, guard, and dispatch responsibilities.
- Stream IO test support is now a small umbrella over connect, basic stream, vectored, zero-copy boundary, zero-copy send, and sendfile/splice task fragments.
- Stream IO runtime tests are split by basic stream, vectored send/recv, zero-copy send, fd-to-fd transfer, and connect/accept coverage.
- Datagram IO runtime tests are split by readiness/hangup, UDP receive, and UDP send/zero-copy coverage.
- Timer/event IO test support is now a small umbrella over timer/timeout tasks, eventfd tasks, timer/event boundary tasks, and filesystem boundary tasks.
- Wait/cancel IO test support is now a small umbrella over basic wait/bad-fd tasks, cancel state-machine tasks, deadline timeout tasks, and zero-byte/vectored boundary tasks.
- io_uring socket multishot test support is now split between recv/provided-buffer and recvmsg/peer-address task fragments, with the original multishot header kept as a small compatibility umbrella.
- Epoll runtime tests are split by setup, readiness, cancel/timeout, boundary, socket lifecycle, and event/timer adapter coverage.
- The Linux epoll executor backend is now a small platform umbrella over setup/wake, storage/deferred-delete, poll, wait registration, and cancel fragments. This mirrors the kqueue split while keeping all syscall paths inline inside `AsyncRuntime::Executor`.
- io_uring backend executor internals are now split into setup/close, SQ submit/poll, CQ completion, and operation lifecycle fragments while remaining inline in `AsyncRuntime::Executor`.
- io_uring fixed file/buffer test support is now a small umbrella over fixed-buffer, fixed-file read/write, fixed-file update, and openat-direct task fragments.
- IO benchmark support now keeps the hot benchmark-facing fake task shell small and splits FakeRuntime stubs by Linux socket, POSIX message, POSIX fixed file, accept/connect, and filesystem helpers.
- Runtime benchmarks are now split into shared runtime benchmark task support, external-start, thread-hop, and parallel-shard benchmark families. This keeps benchmark harness changes separate from the task/state-machine fixtures they measure.
- The length-prefixed RPC example is now split into runtime traits, server/process task, client task, and a thin executable entry point.
- The vectored IO example is now split into runtime/common helpers, stream readv/writev tasks, datagram recvmsg/sendmsg tasks, and a thin executable entry point.
- The io_uring UDP recvmsg multishot example is now split into runtime/wait helpers, the provided-buffer multishot task, UDP socket setup helpers, and a thin executable entry point.
- The io_uring accept-direct example is now split into runtime/wait helpers, socket setup/read-write helpers, the fixed-file accept round-trip task, and a thin executable entry point.
- The io_uring fixed-file example is now split into runtime traits, temporary file lifecycle helpers, the registered-file/buffer round-trip task, and a thin executable entry point.
- The io_uring filesystem-ops example is now split into runtime/result types, temporary path lifecycle helpers, the filesystem operation state machine, and a thin executable entry point.
- The io_uring UDP recv multishot example is now split into runtime/wait helpers, UDP socket setup helpers, the provided-buffer recv multishot task, and a thin executable entry point.
- The io_uring file-lifecycle example is now split into runtime traits, temporary path lifecycle helpers, the open/fallocate/read/write/stat/rename/unlink/close task, and a thin executable entry point.
- The pollable-client adapter example is now split into runtime traits, a third-party-style pollable echo client, the AsyncFlow readiness adapter task, peer echo helpers, and a thin executable entry point.
- The io_uring stream recv multishot example is now split into runtime/wait helpers, socketpair setup helpers, the provided-buffer recv multishot task, and a thin executable entry point.
- The TCP connect/accept example is now split into runtime traits, portable loopback socket setup, server/client state-machine tasks, and a thin executable entry point. It uses the unified `ThreadKind::Io`/`ThreadKind::IoUring` API so Linux can prefer io_uring while macOS/BSD uses the native kqueue readiness backend.
- The TCP echo server example demonstrates a fully asynchronous 2-IO-thread/1-compute-thread flow: accept/read on IO threads, uppercase-to-lowercase transform on the compute thread, then send on the owning IO thread. Its runtime traits, socket setup, server acceptor, session state machine, client driver, and executable entry point are split into focused headers.
- The datagram round-trip example is now split into runtime traits, portable UDP loopback socket setup, server/client state-machine tasks, and a thin executable entry point. It uses the same unified API shape as TCP: Linux prefers io_uring with epoll fallback and macOS/BSD uses kqueue readiness.
- The socket lifecycle example is now split into runtime traits, an async listener lifecycle task, an async client connect task, and a thin executable entry point. The example no longer uses main-thread atomic polling to wait for listener readiness; the server task starts the client task after `getsockname`, and `ShutdownPolicy::WaitForTasks` drives completion.
- The io_uring multishot accept example is now split into runtime traits, listener/client socket helpers, the multishot accept state machine, and a thin executable entry point. It no longer polls task arm state with main-thread atomics; clients are queued before the accept task starts and completion is read after `ShutdownPolicy::WaitForTasks` shutdown.
- The sendfile static-file example is now split into runtime traits, payload/temp-file setup, listener setup, async accept/sendfile server task, async connect/read client task, and a thin executable entry point. The network path no longer uses main-thread blocking accept/read polling.
- The io_uring openat-direct example is now split into runtime traits, temporary path lifecycle, the fixed-file direct-open round-trip task, and a thin executable entry point.
- The io_uring send zero-copy example is now split into runtime traits, listener setup, async accept/send_zc server task, async connect/read verification client task, and a thin executable entry point. The network path no longer uses main-thread blocking accept/read polling.
- The IO adapter example is now split into POSIX socket setup helpers, stream adapter tasks, UDP adapter tasks, result types, and a thin executable entry point. Main no longer uses atomic readiness polling before writing to sockets; stream and UDP peer activity is driven by runtime tasks on the IO thread.
- io_uring file-data submit wrappers are now split into basic read/write, timeout, fixed-file, fixed-buffer, vectored, and fsync fragments while staying inline in `AsyncRuntime::Executor`.
- Native readiness backends now have a platform-dispatch include point: Linux uses an epoll fragment and macOS/BSD uses a kqueue fragment, while public `io_*` helpers continue to expose one API. This keeps OS-specific syscall code out of the generic executor loop and preserves header-only inlining.
- `include/af/io_common.hpp` is now a small umbrella over focused common fragments: basic socket/error helpers, wait-state helpers, fixed-file vectored helpers, Linux eventfd/timerfd helpers, and deadline state.
- `include/af/io_types.hpp` is now a small public umbrella over base IO typedefs/views, Linux provided-buffer rings, `IoStatus`, and `UniqueFd`. Linux-only ring storage and POSIX fd ownership are separate fragments while staying inline.
- `include/af/task.hpp` is now a small public umbrella over task enum/type declarations, IO wait state, optional task-registry links, and the `BasicTask` implementation. Task lifecycle and scheduling state transitions remain inline/template-visible.
- The macOS/BSD kqueue backend is split by setup, timeout, poll, storage, wait, and event translation helpers. kqueue now supports native one-shot timeout completion and cancel for `io_wait_timeout()` / `arm_io_timeout()` without routing through Linux `timerfd`.
- Each split so far preserved header-only/template visibility, passed `git diff --check`, Docker GCC Debug runtime tests, and, for core runtime header changes, Release runtime benchmark baseline regression.

## Current Findings

### Second-Pass Structure Snapshot

The current top-level runtime shell is no longer the main modularity problem:

- `include/af/async_runtime.hpp`: 226 lines. It is mostly a declaration shell that includes focused public/runtime/executor fragments.
- `include/af/task.hpp`: small umbrella over task declarations, IO wait state, optional registry links, and `BasicTask`.
- `include/af/io_types.hpp`: small umbrella over IO base types, provided buffers, status, and fd ownership.

The remaining code-size pressure is now in second-level fragments and fixtures. `include/af/detail/io_file_fixed_buffer_fragment.hpp`, `include/af/detail/io_socket_send_fragment.hpp`, `include/af/detail/io_file_lifecycle_fragment.hpp`, and `include/af/detail/io_adapters_stream_listener_fragment.hpp` have been reduced to small umbrellas over operation-family helpers. The largest remaining runtime-facing headers in the latest scan are:

- `include/af/detail/io_socket_lifecycle_fragment.hpp`: 264 lines. This remains a broad socket lifecycle helper fragment.
- `include/af/detail/runtime_executor_io_uring_filesystem_submit_fragment.hpp`: 261 lines. This combines several filesystem SQE preparation/submission helpers.
- `include/af/detail/runtime_executor_io_uring_socket_msg_submit_fragment.hpp`: 258 lines. This combines recvmsg/sendmsg/multishot message submission paths.
- `include/af/detail/runtime_public_io_resource_fragment.hpp`: 255 lines and `runtime_public_io_filesystem_submit_fragment.hpp`: 254 lines. These are still readable, but they are broad public API fragments.

This means the right next step is not another large rewrite of `async_runtime.hpp`; it is a second-pass split of the largest fragments into narrower operation-family files while keeping them included inline inside the same class/function scopes.

### Latest Structure Scan: async_runtime.hpp Is Now A Shell, Detail Fragments Carry The Weight

The current `include/af/async_runtime.hpp` is 226 lines and mostly wires public API fragments, executor fragments, and runtime state fragments together. `include/af/task.hpp` and `include/af/io_types.hpp` are now also overview-sized umbrellas. The remaining modularity risk is concentrated in several focused-but-still-dense fragments:

- Public IO adapter and socket/file lifecycle fragments are still around 250-300 lines in a few places. They are acceptable for inline APIs, but future additions should go into narrower recv/send/fixed/lifecycle fragments.

Recommendation:
- Do not move hot runtime code into `.cpp` files. Keep template and syscall-submit code inlineable through include fragments.
- Keep `runtime_executor_core_state_fragment.hpp` as the single state-layout owner unless the split can preserve declaration order and cache-line placement exactly.

### Recommended Second-Pass Split Order

P1, executor submit internals:

- Split `runtime_executor_io_uring_filesystem_submit_fragment.hpp` by SQE family: stat/fallocate, rename/unlink, and open/close/fsync if future additions increase density.
- Split `runtime_executor_io_uring_socket_msg_submit_fragment.hpp` into recvmsg, sendmsg, multishot recvmsg, and zero-copy notification preparation if further io_uring message features are added.

P2, tests/examples/benchmarks:

- Split large file IO support fixtures by boundary tests, lifecycle state machines, fixed-resource state machines, and filesystem operation state machines.
- Keep runtime tests grouped by backend and operation family. Avoid rebuilding a single all-in-one IO regression file.
- Keep examples as thin executable entry points plus task/helper headers. New examples should follow the newer TCP echo/socket lifecycle/sendfile structure rather than putting full state machines in `main`.

Performance constraints for these splits:

- Keep all hot helpers inline/template-visible. Use umbrella fragments, not `.cpp` moves.
- Do not introduce owning polymorphic base classes, `std::function`, heap allocation, or additional cross-thread queues just to make files smaller.
- Preserve existing executor state layout and cache-line alignment. Splitting state declaration should be avoided unless there is a concrete cache-layout improvement.
- Preserve branch shape in hot helpers; refactor by operation family, not by extracting tiny helper functions that add extra unpredictable branches.


### Latest Test/Example Scan: Long Files Remain Mostly In Fixtures

The largest remaining files are now test/support fixtures rather than runtime shell code:

- File IO support fragments remain heavy: open/lifecycle 393 lines, boundary 389 lines, read/write 328 lines, filesystem boundary 303 lines, filesystem ops 295 lines.
- A few runtime tests are still moderately large: io_uring socket multishot 275 lines, io_uring socket datagram 257 lines, stream transfer 255 lines.
- Some examples still use explicit atomics to observe readiness/completion (`io_epoll.cpp`, `io_timer.cpp`, and a few multishot io_uring examples). Those should be converted to task-owned state machines plus `ShutdownPolicy::WaitForTasks` when practical, matching the newer IO adapter/socket lifecycle examples.

Recommendation:
- Continue splitting test support by operation family and boundary category, but keep fixtures close to the tests that use them.
- Prefer example completion through runtime shutdown policy and task result structs, not main-thread atomic polling, unless the example is specifically demonstrating cross-thread observation.

### P1: io_uring Executor Internals Can Be Split Further

The largest remaining runtime-internal fragments are io_uring completion/submit details and public socket data submit wrappers.

Risk:
- CQE completion details and public socket data submit wrappers still have dense local regions.
- Concurrency/lifetime audits still require reading several dense io_uring fragments.

Recommended split:
- Consider a second-pass split of backend completion into CQE decoding, normal completion, multishot continuation, zero-copy notification, timeout/cancel, and teardown paths if further audit needs it.
- Consider a follow-up split of io_uring setup tuning helpers if more backend setup knobs are added.
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

### Validation Note: Runtime Benchmark Time-Mode Needs Follow-Up

The local macOS Release benchmark binary and the remote Linux Docker Release benchmark both completed fixed-iteration smoke runs, but CI-style time-mode runs exceeded a 180s timeout in the current validation environment before producing benchmark rows.

Risk:
- Full runtime baseline regression checks may be too sensitive to benchmark runner options or environment scheduling noise.
- A long time-mode run can hide whether a refactor introduced a real regression or whether the benchmark harness is over-running.

Recommended split:
- Keep benchmark task definitions and wait helpers in focused support headers before changing benchmark semantics.
- Add a short fixed-iteration smoke benchmark mode for local validation.
- Keep the CI baseline check on Linux, but audit the `--benchmark_min_time` / warmup options and timeout behavior separately from source modularization commits.

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
