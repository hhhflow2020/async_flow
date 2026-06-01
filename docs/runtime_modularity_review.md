# Runtime Modularity Review

Date: 2026-06-01

## Scope

This note tracks modularity and responsibility-boundary issues found while reviewing the fixed-thread runtime, IO executor, tests, benchmarks, and examples.

The runtime is intentionally header-only/template-visible for hot path inlining. Refactors should keep hot paths inline/template-visible, but class-body `#include` splicing is not a target pattern. Prefer named components: standalone classes/data structures, namespace-scope inline implementation headers, operation-family headers, or CRTP bases when they create a real boundary.

## Already Improved

- `include/af/async_runtime.hpp` is now a normal runtime class shell with public aliases/API declarations, static runtime state, and no class-body fragment includes. Namespace-scope inline implementation headers are included after the class definition.
- `detail::RuntimeConfig`, `detail::RuntimeTaskHandle`, and `detail::RuntimeCommonState` own config validation, task-handle lifetime, and common cache-line/state helpers instead of splicing those types into the `AsyncRuntime` class body.
- Public runtime implementation is split by responsibility into `runtime_public_api.hpp`, `runtime_public_io.hpp`, `runtime_dispatch.hpp`, `runtime_task_lifecycle.hpp`, and `runtime_parallel.hpp`.
- `detail::Executor<RuntimeT, TraitsT>` is now a standalone executor component instead of a nested class assembled through class-scope fragments. It remains header-only/template-visible for the scheduler and IO hot paths.
- `tests/runtime_io_test_support.hpp` is now an umbrella header with domain fragments for core traits, basic tasks, stream, accept, file, timer/event, wait/cancel, socket lifecycle, and io_uring socket support.
- Portable accept/multishot receive test support is now split into basic TCP accept, accept-multishot boundary, and recv/recvmsg-multishot boundary task fragments, with the previous accept header kept as a small umbrella.
- Basic socket IO test support is now split into stream read/write, UDP datagram, and UDP vectored task fragments with the original basic-socket header kept as an umbrella.
- Socket lifecycle test support is now split into setup happy path, boundary validation, io_uring socket-create, and fast IO wait/done task fragments with the original lifecycle header kept as an umbrella.
- The file IO test support has been split into boundary, normal read/write, fixed-resource, lifecycle/open, and filesystem operation fragments.
- File IO boundary test support is now split further into plain file adapter boundary, registered fixed-buffer boundary, and fixed-file/direct-descriptor boundary fragments. The fixed-file/direct-descriptor path is further split into resource registration/direct-open, direct-accept, and fixed-file data transfer boundary tasks.
- File IO read/write test support is now split further into basic offset read/write, vectored offset read/write, and current-offset state-machine fragments, with the previous read/write header kept as a small umbrella.
- File IO open/lifecycle test support is now split further into batched write, openat round-trip, and full lifecycle task fragments. The full lifecycle task is now kept as one cohesive state-machine class, and the previous open/lifecycle header is kept as a small umbrella.
- Filesystem boundary test support is now split into open/close, metadata/allocation, and namespace/openat2 operation-family fragments, with the previous filesystem boundary header kept as a small umbrella.
- File IO filesystem operation test support is now kept as one cohesive filesystem state-machine class instead of a class-body include shell; broader file IO support remains split by boundary, read/write, fixed-resource, lifecycle, and filesystem-operation responsibilities.
- Public IO helper headers remain focused umbrellas: `io_socket.hpp`, `io_file.hpp`, and `io_adapters.hpp` now include normal internal headers for socket helpers, file helpers, file descriptors/fixed files, stream/listener, datagram, aliases, and event/timer adapters.
- `include/af/io_datagram.hpp` is now an umbrella over focused datagram recv, send, vectored, and zero-copy helper headers with normal module names instead of `*_fragment.hpp`.
- io_uring socket test support and runtime socket test sources have been split by stream, datagram, accept/connect, and multishot responsibilities.
- io_uring socket stream test support is now split further into recv/cancel, send/zero-copy send, and vectored stream task fragments, with the previous stream header kept as a small umbrella.
- io_uring socket multishot runtime tests are now split into stream recv-multishot, connected UDP recv-multishot, and UDP recvmsg-multishot sources.
- io_uring socket accept test support is now split further into basic accept, accept-direct fixed-file round-trip, and accept-multishot task fragments, with the previous accept support header kept as a small umbrella.
- io_uring file runtime tests are now split by basic file data, fixed resources/direct descriptors, submit batching, and lifecycle/filesystem operation responsibilities.
- `runtime_executor_io_uring_submit_core_fragment.hpp` is now a small umbrella over poll wait submit, buffer/fast SQE submit, and generic SQE submit fragments. The code still lives inside `AsyncRuntime::Executor` for inline visibility.
- `runtime_executor_io_uring_socket_submit_fragment.hpp` is now a small umbrella over recv, send, zero-copy, message, multishot, accept/connect, and socket-create submit wrappers.
- io_uring resource registration is now split by registered buffers, provided buffer rings, and fixed-file table helpers while staying inline inside `AsyncRuntime::Executor`. The registered-buffer helper is now a small umbrella over register and unregister paths, the provided-buffer ring helper is now a small umbrella over register and unregister paths, and the fixed-file table helper is a small umbrella over register, unregister, and update paths.
- io_uring executor buffer submit helpers are now split into generic buffer SQE, fast SQE template, socket-create core, and fixed-file read/write fragments while staying inline inside `AsyncRuntime::Executor`.
- io_uring generic submit is now split into argument classification, validation, operation preparation, SQE filling, and the small core submit flow while staying inline inside `AsyncRuntime::Executor`. The SQE filling stage is further split by dispatcher/common initialization, filesystem/path opcodes, socket/message opcodes, and buffer/data opcodes.
- Public fd lifecycle submit wrappers and matching io_uring executor submit wrappers are now small umbrellas over open, close/shutdown, filesystem metadata/lifecycle, and splice transfer fragments.
- Public filesystem submit wrappers and matching io_uring executor SQE submit wrappers are now split by metadata, allocation/truncate, and namespace operation families, with small umbrellas preserving inline visibility.
- io_uring socket message executor submit wrappers are now split into recvmsg and sendmsg operation families, with `runtime_executor_io_uring_socket_msg_submit_fragment.hpp` kept as a small inline umbrella.
- Public socket message submit wrappers are now split into recvmsg, sendmsg, and accept/connect fragments, with a small umbrella preserving inline visibility in `AsyncRuntime`.
- Public socket data submit wrappers are now a small umbrella over basic recv, recv multishot, basic send, and zero-copy send fragments. Public API validation and executor handoff remain inline in `AsyncRuntime`.
- Public socket accept/connect helpers are now a small umbrella over accept, accept-direct, accept-multishot, and connect fragments. POSIX fallback, io_uring direct accept, multishot completion handling, and connect readiness fallback are now separate while staying inline.
- Public socket lifecycle helpers are now split into create, socket options, socket name lookup, and listener bind/listen headers, with `io_socket_lifecycle.hpp` kept as a small inline umbrella.
- Public file-data submit wrappers are now split into basic read/write/fsync, fixed-file, registered-buffer, and vectored fragments, with a small umbrella preserving inline visibility in `AsyncRuntime`.
- Public IO resource wrappers are now split into backend availability, buffer resources, file resources, and wait/cancel/timeout fragments, with `runtime_public_io_resource_fragment.hpp` kept as a small inline umbrella.
- Public io_uring availability now exposes setup/runtime failure diagnostics through `io_uring_backend_error(thread)`, so tests can distinguish a missing backend from `EPERM`/`ENOSYS`/mapping failures instead of relying on a silent boolean.
- Public socket receive helpers are now split into basic recv/fixed-file recv, recv multishot, and recvmsg multishot parser/submit headers, with `io_socket_recv.hpp` kept as a small inline umbrella.
- Public socket send helpers are now split into basic send, fixed-file send, zero-copy send, and vectored zero-copy send headers, with `io_socket_send.hpp` kept as a small inline umbrella.
- Public file fixed-resource helpers are now split into fixed-file read/write/fsync, registered-buffer read/write, and vectored file write headers, with `io_file_fixed_buffer.hpp` kept as a small inline umbrella.
- Public file lifecycle helpers are now split into open/open-direct, close/fsync, metadata, and namespace-operation headers, with `io_file_lifecycle.hpp` kept as a small inline umbrella.
- Public stream/listener adapters are now split into `IoStream` and `IoListener` internal headers, with `io_adapters_stream_listener.hpp` kept as a small inline umbrella.
- Runtime lifecycle tests have been split into base lifecycle, backpressure, and shutdown-policy sources with shared traits/tasks in support.
- Runtime lifecycle support is now a small umbrella over base, backpressure, and shutdown-policy task fragments.
- Runtime parallel tests have been split into shard scheduling, ordered-start, and ordered-batch sources with shared task support.
- Runtime parallel support is now a small umbrella over core runtime fixtures, shard tasks, ordered-batch tasks, and ordered-start tasks.
- Core runtime parallel implementation now lives in `runtime_parallel.hpp`, with ordered-start state, ordered-batch guard, shard runner/task, and shard dispatch kept as namespace-scope inline template definitions.
- Utility tests are now split by queue, object-pool, IO state, and batch/sequencer utility domains instead of collecting unrelated helpers in one source file.
- Stream IO test support is now a small umbrella over connect, basic stream, vectored, zero-copy boundary, zero-copy send, pending zero-copy send, sendfile, and splice task fragments.
- Stream IO runtime tests are split by basic stream, vectored send/recv, zero-copy send, fd-to-fd transfer, and connect/accept coverage.
- Datagram IO runtime tests are split by readiness/hangup, UDP receive, and UDP send/zero-copy coverage.
- Timer/event IO test support is now a small umbrella over timer/timeout tasks, eventfd tasks, timer/event boundary tasks, and filesystem boundary tasks.
- Wait/cancel IO test support is now a small umbrella over basic wait/bad-fd tasks, cancel state-machine tasks, deadline timeout tasks, and zero-byte/vectored boundary tasks.
- io_uring socket multishot test support is now split between recv/provided-buffer and recvmsg/peer-address task fragments, with the original multishot header kept as a small compatibility umbrella. The recv/provided-buffer task is kept as one cohesive state-machine class instead of a class-body include shell.
- Epoll runtime tests are split by setup, readiness, cancel/timeout, boundary, socket lifecycle, and event/timer adapter coverage.
- The Linux epoll executor backend now lives in `runtime_executor_epoll_backend.hpp` as class-out-of-line template definitions for setup/wake, storage cleanup, polling, wait registration, and cancel. This keeps syscall paths inline/template-visible without class-body include splicing.
- io_uring backend executor internals are now split into setup/close, SQ submit/poll, CQ completion, and operation lifecycle fragments while remaining inline in `AsyncRuntime::Executor`.
- io_uring CQ completion is now split by CQ polling, operation completion, poll-wait completion, and fd/direct-file cancel cleanup, with `runtime_executor_io_uring_backend_completion_fragment.hpp` kept as a small inline umbrella.
- io_uring backend setup is now split by init flow, mmap/pointer binding, feature probing, close/reset, and storage reservation, with `runtime_executor_io_uring_backend_setup_fragment.hpp` kept as a small inline umbrella.
- kqueue timeout internals are now split by timer-unit conversion, registration tracking, submit, and cancel/complete paths, with `runtime_executor_kqueue_timeout_fragment.hpp` kept as a small inline umbrella inside `AsyncRuntime::Executor`.
- io_uring fixed file/buffer test support is now a small umbrella over fixed-buffer, fixed-file read/write, fixed-file update, and openat-direct task fragments. The fixed-file read/write task is kept as one cohesive state-machine class instead of a class-body include shell.
- IO benchmark support now keeps the hot benchmark-facing fake task shell small and splits FakeRuntime stubs by Linux socket, POSIX message, POSIX fixed file, accept/connect, and filesystem helpers. Adapter benchmark cases are also split by stream/listener, datagram, and resource/file-like families.
- Runtime benchmarks are now split into shared runtime benchmark task support, external-start, thread-hop, and parallel-shard benchmark families. This keeps benchmark harness changes separate from the task/state-machine fixtures they measure.
- The length-prefixed RPC example is now split into runtime traits, server/process task fragments, a cohesive client state-machine task, and a thin executable entry point.
- The vectored IO example is now split into runtime/common helpers, stream readv/writev tasks, datagram recvmsg/sendmsg tasks, and a thin executable entry point.
- The io_uring UDP recvmsg multishot example is now split into runtime/wait helpers, UDP socket setup helpers, a cohesive provided-buffer recvmsg multishot task class, and a thin executable entry point.
- The io_uring accept-direct example is now split into runtime/wait helpers, socket setup/read-write helpers, the fixed-file accept round-trip task, and a thin executable entry point.
- The io_uring fixed-file example is now split into runtime traits, temporary file lifecycle helpers, a cohesive registered-file/buffer task class, and a thin executable entry point.
- The io_uring filesystem-ops example is now split into runtime/result types, temporary path lifecycle helpers, a cohesive filesystem operation task class, and a thin executable entry point.
- The io_uring UDP recv multishot example is now split into runtime/wait helpers, UDP socket setup helpers, a cohesive provided-buffer recv task class, and a thin executable entry point.
- The io_uring file-lifecycle example is now split into runtime traits, temporary path lifecycle helpers, a cohesive lifecycle task class, and a thin executable entry point.
- The pollable-client adapter example is now split into runtime traits, a third-party-style pollable echo client, the AsyncFlow readiness adapter task, peer echo helpers, and a thin executable entry point.
- The io_uring stream recv multishot example is now split into runtime/wait helpers, socketpair setup helpers, a cohesive provided-buffer recv task class, and a thin executable entry point.
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
- `include/af/detail/io_uring_support.hpp` is now a small Linux-only umbrella over ABI fallback definitions, opcode constants, support request/types, syscall/setup wrappers, and SQE fill helpers with normal module names instead of `*_fragment.hpp`. Its include order is fixed because syscall/setup and SQE helpers depend on support request types declared earlier.
- Native readiness backends now have a platform-dispatch include point: Linux uses an epoll fragment and macOS/BSD uses a kqueue fragment, while public `io_*` helpers continue to expose one API. This keeps OS-specific syscall code out of the generic executor loop and preserves header-only inlining.
- `include/af/io_common.hpp` is now a small umbrella over focused common headers: basic socket/error helpers, wait-state helpers, fixed-file vectored helpers, Linux eventfd/timerfd helpers, and deadline state. The active internal headers use normal module names (`io_common_base.hpp`, `io_common_state.hpp`, `io_common_fixed_file.hpp`, `io_common_linux_event_timer.hpp`, `io_common_deadline.hpp`) instead of `*_fragment.hpp`.
- `io_common_state.hpp` is now a small implementation umbrella. Target-thread/socket-name helpers, readiness wait arming, readiness wait state, io_uring status normalization, and iovec validation now live in focused `io_common_*` headers with normal module names. Its include order is fixed because `io_common_uring_status.hpp` depends on wait-state helpers declared earlier.
- `include/af/io_types.hpp` is now a small public umbrella over base IO typedefs/views, Linux provided-buffer rings, `IoStatus`, and `UniqueFd`. The active internal headers use normal module names (`io_types_base.hpp`, `io_types_provided_buffer.hpp`, `io_types_status.hpp`, `io_types_unique_fd.hpp`) instead of `*_fragment.hpp`; Linux-only ring storage and POSIX fd ownership stay inline.
- `include/af/task.hpp` is now a small public umbrella over task enum/type declarations, IO wait state, optional task-registry links, and the `BasicTask` implementation. The active task headers use normal module names (`task_types.hpp`, `task_io_state.hpp`, `task_registry.hpp`, `basic_task.hpp`) instead of `*_fragment.hpp`; task lifecycle and scheduling state transitions remain inline/template-visible.
- `include/af/io_filesystem.hpp` is now a small public umbrella over open/openat2, namespace operation, allocation/truncate, and `IoDirectory` adapter headers with normal module names instead of `*_fragment.hpp`. Public filesystem helpers remain header-only/template-visible while keeping operation families separate.
- The macOS/BSD kqueue backend is split by setup, timeout, poll, storage, wait, and event translation helpers. kqueue now supports native one-shot timeout completion and cancel for `io_wait_timeout()` / `arm_io_timeout()` without routing through Linux `timerfd`.
- The bounded queue implementations are now split by SPSC, MPSC, and MPMC queue family, with `bounded_queues.hpp` kept as a compatibility umbrella. The split is mechanical and preserves queue layout, cache-line alignment, memory ordering, and template visibility.
- Socket transfer helpers are now split by sendfile, shutdown, and splice operation family, with `io_socket_transfer.hpp` kept as a small inline umbrella.
- `include/af/io_socket.hpp` is now a small public umbrella over lifecycle, accept/connect, recv, send, transfer, and vectored helper headers with normal module names instead of `*_fragment.hpp`.
- `IoFixedFile`, `IoFile`, `IoStream`, and `IoDatagramSocket` are now cohesive thin adapter class definitions in normally named internal headers. The previous class-body method-fragment split has been reverted because it made each adapter harder to audit without creating an independent abstraction boundary.
- Public file read helpers are now split into current-offset read/readv and positioned read/readv headers, with `io_file_read.hpp` kept as a small inline umbrella.
- `include/af/io_file.hpp` is now a small public umbrella over current-offset read, positioned write/read, fixed-file/registered-buffer/vectored write, lifecycle/open/close/fsync, metadata, namespace, and current-offset write headers with normal module names instead of `*_fragment.hpp`.
- Public timeout helpers are now split into timeout completion status normalization, single timeout wait submission, and deadline arbitration headers with normal module names. `io_timeout.hpp` remains a small public umbrella while preserving inline/template visibility for timeout and cancel race handling.
- Runtime common state now lives in `runtime_common_state.hpp` as real named types: runtime status, cache-line atomic wrapper, ordered-batch state, parallel-group state, and external-post counter.
- Runtime dispatch now lives in `runtime_dispatch.hpp`; explicit ready-route selection, non-blocking enqueue, blocking enqueue, and post/pending admission stay in one scheduler-owned implementation file. The same-thread local queue path and cross-thread SPSC path remain separate named helpers.
- Each split so far preserved header-only/template visibility, passed `git diff --check`, Docker GCC Debug runtime tests, and, for core runtime header changes, Release runtime benchmark baseline regression.

## Current Findings

### 2026-06-01 Runtime Core De-Fragmenting Correction

Status: current core-runtime structure. This section supersedes the earlier fragment-oriented runtime snapshots below where they recommend class-scope include fragments as the modularity target.

Issue recorded:

- P1: class-body `#include` splicing made code shorter per physical file but harder to audit. It hid declaration order, access boundaries, friendship requirements, cache-line placement, and state-machine invariants behind preprocessor wiring.
- P1: modularity should split independent data structures, algorithms, operation families, and task/adapter roles. It should not split one cohesive class by access section, field block, or a few private methods just to lower a line count.
- P1: runtime hot paths must stay header-only/template-visible, but that does not require injecting fragments into the middle of class definitions.

Current core-runtime layout:

- `include/af/async_runtime.hpp`: 391 lines. It declares the `AsyncRuntime` API aliases, public methods, private runtime state, and static members. It no longer includes fragment headers inside the class body.
- `include/af/detail/runtime_config.hpp`: config validation and public tuning values. The thread-count boundary is still `> 0` and `<= UINT16_MAX`; there is no 64-thread cap.
- `include/af/detail/runtime_task_handle.hpp`: public task-handle lifetime wrapper.
- `include/af/detail/runtime_common_state.hpp`: runtime status, cache-line atomic wrapper, ordered-batch state, parallel-group state, and external-post counter.
- `include/af/detail/runtime_public_api.hpp`: lifecycle, task creation/start/post, thread helpers, public parallel entry points, and ordered-start public APIs.
- `include/af/detail/runtime_public_io.hpp`: CRTP public IO API component inherited by `AsyncRuntime`.
- `include/af/detail/runtime_dispatch.hpp`: queue topology, local/SPSC/external enqueue paths, ready-route selection, and external-post admission accounting.
- `include/af/detail/runtime_task_lifecycle.hpp`: task object pools, handle release, optional StopImmediately registry/cancel, and unfinished-task accounting.
- `include/af/detail/runtime_parallel.hpp`: ordered-start, ordered-batch guard, shard task/runner, and shard dispatch.
- `include/af/detail/runtime_executor.hpp`: standalone `detail::Executor<RuntimeT, TraitsT>` declaration/state-layout component. Lifecycle/notify behavior lives in `runtime_executor_lifecycle.hpp`; io_uring backend status and resource registration lives in `runtime_executor_io_resources.hpp`; IO backend setup/poll/completion/operation cleanup lives in `runtime_executor_io_backend.hpp`; Linux epoll readiness setup/poll/wait/cancel lives in `runtime_executor_epoll_backend.hpp`; private Linux io_uring submit helpers live in `runtime_executor_io_submit_core.hpp`; ready/local-queue execution and run-loop scheduling live in `runtime_executor_scheduler.hpp`.

Correctness and performance audit points:

- No queue algorithm, task state transition, wake publication, memory ordering, or IO submit/completion semantic change was intended in this cleanup.
- Runtime-thread ready enqueue remains explicit: `ReadyQueueRoute::Local` for same-owner posts, `ReadyQueueRoute::Spsc` for cross-owner runtime posts, and external MPSC for external threads.
- `BasicTask` now explicitly friends `detail::Executor<RuntimeT, TraitsT>` because the executor is no longer nested inside `AsyncRuntime`.
- Public IO was moved behind a CRTP base to create a real API component boundary without adding virtual dispatch, allocation, `std::function`, or extra queues.
- Executor lifecycle/resource/IO-backend/scheduler behavior is now class-out-of-line template code included after the complete class declaration. This keeps private-state access and inlining while avoiding class-body include splicing.
- Unreferenced runtime fragment entrypoints and their unreferenced child implementation fragments have been removed from the active framework tree. The only remaining framework `*_fragment.hpp` file is an existing dirty worktree file that is not included by active non-fragment headers and was left untouched. Do not use historical fragment files as the model for new core-runtime structure.

Validation after this correction:

- Remote clang Debug build of `asyncflow_runtime_tests`: passed.
- Remote clang Debug full runtime suite with `--security-opt seccomp=unconfined`: 143 total, 140 passed, 3 skipped.
- Remote clang Release build of `asyncflow_runtime_tests` and `asyncflow_runtime_benchmarks`: passed.
- Remote clang Release full runtime suite with `--security-opt seccomp=unconfined`: 143 total, 140 passed, 3 skipped.
- Release benchmark canary after the executor io_uring submit-core split, 3 repetitions with `--benchmark_min_time=0.05s`: `BM_RuntimeExternalStart/8192` mean 7.29 ms, `BM_RuntimeCrossThreadHop/8192` mean 13.0 ms, `BM_RuntimeIoThreadHop/8192` mean 4.28 ms, `BM_RuntimeParallelShards/128` mean 0.484 ms.

Remaining follow-up:

- P1: `runtime_executor.hpp` is still large at 2627 lines after the io_uring submit-core split. Do not re-split it by access sections. The next acceptable split should continue moving real operation families out as class-out-of-line template definitions or extract backend-specific helper components with explicit owner boundaries.
- P2: non-core IO headers such as `io_common` and several `io_*` public umbrellas still use fragment includes. They should be cleaned opportunistically when the split can follow real operation-family or class boundaries.

### 2026-06-01 Scheduler Correctness And Modularity Pass

Status: superseded for current file layout. This section remains historical evidence for the scheduler fixes, but unreferenced top-level runtime fragment entrypoint files from that intermediate layout were later removed because they were no longer included by active framework headers.

This pass resolves the active fixed-thread scheduler issues that were blocking the next modularity step.

Resolved items:

- `runtime_dispatch_fragment.hpp` is now a 7-line umbrella. Queue topology, ready enqueue paths, and external-post admission accounting live in separate inline fragments:
  - `runtime_queue_topology_fragment.hpp`: SPSC/external queue aliases, queue initialization, and queue lookup.
  - `runtime_ready_enqueue_fragment.hpp`: small umbrella over route selection, try-enqueue, blocking enqueue, and post/pending admission.
  - `runtime_external_post_gate_fragment.hpp`: external post admission, active-post release, and shutdown drain wait.
- Runtime-thread self-post is now explicit. `ReadyQueueRoute` names the local-queue and SPSC enqueue routes, and `enqueue_ready_blocking_from_runtime_thread(source, target, task)` dispatches to `enqueue_local_from_runtime_thread_blocking()` when `source == target`, instead of hiding the local queue behind a generic enqueue helper.
- The previous `thread_count <= 64` ready-source hint limit is gone. `ReadySourceSet<ThreadCount>` stores ready sources across cache-line-aligned 64-bit words and supports runtimes above 64 threads.
- Runtime config now validates the raw `Traits::thread_count` before narrowing it to the runtime's 16-bit thread index type. This preserves above-64 support while rejecting impossible index counts instead of silently truncating them.
- Ready-source bits are hints, not correctness state. `pop_one()` now checks ready-source words, clears an empty source only after an SPSC pop miss, immediately rechecks that SPSC queue, and still has a bounded all-source SPSC fallback scan. This prevents lost/coalesced ready hints from stranding work while avoiding permanent scans of stale sticky bits.
- A late owner-resume race was fixed in `BasicTask`. Running wake requests are now tagged with a run epoch, `Executor::execute()` uses a `Queued -> Starting -> Running` transition, and a post that races with `finish_pending()` can either defer to the owner or directly transition `Pending -> Queued` and enqueue the task. This prevents a stale `Running` observation from writing `requested_thread_` after the owner has already checked the slot.
- The Running -> Pending boundary has direct stress coverage. `RuntimeStressTests.RunningToPendingWakeDoesNotStrandOwner` forces a waker on another runtime thread to post the owner while the owner is still Running and about to return Pending; the owner must resume and complete instead of hanging.
- Running wake requests are now terminal-state safe. If a wake request arrives while the owner is Running but the owner returns `Done` or `Again` instead of `Pending`, `finish_done()` and `finish_again()` publish the terminal/queued state before consuming the request slot, treating that request as a Pending-only wake instead of a false debug assertion.
- io_uring completion cancel no longer publishes a user-visible `ECANCELED` result before the kernel CQE arrives. `cancel_io_completion()` now only submits the cancel request, marks the operation as cancel-requested, and treats repeated cancel requests as already accepted; `completed_uring_status()` and `completed_uring_timeout_status()` keep reporting pending while the completion token is still owned by the runtime. This prevents a task from consuming cancel success and finishing while the pending operation still holds its task/result pointers.
- Scheduler and runtime stress fixtures are no longer concentrated in one source. Reusable scheduler state machines are split by repeat-hop, above-64 wide-hop, parallel owner-resume, and wait-helper fragments; lifecycle stress helpers live in `tests/support/runtime_lifecycle_stress_support.hpp`, and test cases are split by lifecycle, cross-thread, and parallel concerns.
- `runtime_executor_core_state_fragment.hpp` is now the executor field-layout owner only. Queue-drain behavior lives in `runtime_executor_pop_fragment.hpp`, and finish/reschedule behavior lives in `runtime_executor_finish_fragment.hpp`. This keeps declaration order and cache placement in one file while separating behavior that changes scheduling state.
- `runtime_executor_task_fragment.hpp` is now a 7-line umbrella. Ready-source/wake signaling, local queue push/pop, and execute/result dispatch live in `runtime_executor_ready_signal_fragment.hpp`, `runtime_executor_local_queue_fragment.hpp`, and `runtime_executor_execute_fragment.hpp`.
- `runtime_executor_control_fragment.hpp` is now a 6-line umbrella over executor lifecycle and notify/wake control fragments. This keeps thread lifecycle and IO/native wake decisions separately auditable without changing executor state layout.
- `runtime_public_parallel_api_fragment.hpp` is now a 7-line umbrella over shard splitting, parallel shard dispatch overloads, and ordered-start public APIs. The split keeps these public scheduling templates inline while separating data partitioning from owner-resume orchestration.
- Public socket accept/connect submit wrappers and matching io_uring executor submit wrappers are now small umbrellas over accept and connect operation families. The accept-direct and accept-multishot paths remain grouped with accept because they share validation and SQE opcode semantics.
- The previous aggressive `BasicTask` class-body fragment split has been reverted. `basic_task_fragment.hpp` is now one cohesive class definition again, so public API, protected task helpers, lifetime reference handling, scheduling/wake state machine, and storage fields can be read in declaration order without `#include` splicing inside the class body.
- The old task fragment filenames were later retired from the active public task path. `include/af/task.hpp` now includes normal internal task headers, and the `AF_TASK_FRAGMENT_INCLUDE` gate was replaced with `AF_TASK_DETAIL_INCLUDE`.
- The modularity rule for task internals is now stricter: split only when there is an independent data structure, algorithm, function family, or class boundary. Do not split a single class by access section, field block, or a few private member functions just to reduce line count.
- `runtime_executor_io_uring_generic_submit_sqe_fragment.hpp` is now an 8-line umbrella over SQE dispatch/common initialization, filesystem/path SQE fields, socket/message SQE fields, and buffer/data SQE fields.
- The previous `ObjectPool` class-body fragment split has been reverted. `object_pool.hpp` now keeps storage layout, TLS cache, slot acquire/release, lifecycle, MPMC free-list, and cache-line-aligned atomics in one cohesive class definition.
- The previous public adapter class-body fragment split has been reverted. `IoStream`, `IoDatagramSocket`, `IoFile`, and `IoFixedFile` now keep their operation-family methods in declaration order inside one cohesive class definition per adapter, preserving the two-field trivially-copyable adapter layout and inline/template visibility without `#include` splicing inside class bodies.

Historical file-size snapshot after that scheduler pass. For current core-runtime files, use the Runtime Core De-Fragmenting Correction section above:

- `include/af/async_runtime.hpp`: 239 lines.
- `include/af/detail/runtime_public_config_fragment.hpp`: 67 lines.
- `include/af/detail/runtime_dispatch_fragment.hpp`: 7 lines.
- `include/af/detail/runtime_queue_topology_fragment.hpp`: 30 lines.
- `include/af/detail/runtime_ready_enqueue_fragment.hpp`: 8 lines.
- `include/af/detail/runtime_ready_route_fragment.hpp`: 14 lines.
- `include/af/detail/runtime_ready_try_enqueue_fragment.hpp`: 54 lines.
- `include/af/detail/runtime_ready_blocking_enqueue_fragment.hpp`: 57 lines.
- `include/af/detail/runtime_ready_post_fragment.hpp`: 48 lines.
- `include/af/detail/runtime_external_post_gate_fragment.hpp`: 53 lines.
- `include/af/detail/runtime_ready_source_set.hpp`: 70 lines.
- `include/af/detail/runtime_executor_core_state_fragment.hpp`: 70 lines.
- `include/af/detail/runtime_executor_pop_fragment.hpp`: 82 lines.
- `include/af/detail/runtime_executor_finish_fragment.hpp`: 26 lines.
- `include/af/detail/runtime_executor_task_fragment.hpp`: 7 lines.
- `include/af/detail/runtime_executor_ready_signal_fragment.hpp`: 16 lines.
- `include/af/detail/runtime_executor_local_queue_fragment.hpp`: 25 lines.
- `include/af/detail/runtime_executor_execute_fragment.hpp`: 46 lines.
- `include/af/detail/runtime_executor_control_fragment.hpp`: 6 lines.
- `include/af/detail/runtime_executor_lifecycle_fragment.hpp`: 33 lines.
- `include/af/detail/runtime_executor_notify_fragment.hpp`: 27 lines.
- `include/af/detail/runtime_public_parallel_api_fragment.hpp`: 7 lines.
- `include/af/detail/runtime_public_parallel_shard_fragment.hpp`: 44 lines.
- `include/af/detail/runtime_public_parallel_shards_fragment.hpp`: 119 lines.
- `include/af/detail/runtime_public_ordered_start_fragment.hpp`: 23 lines.
- `include/af/detail/runtime_public_io_socket_accept_connect_submit_fragment.hpp`: 6 lines.
- `include/af/detail/runtime_public_io_socket_accept_submit_fragment.hpp`: 148 lines.
- `include/af/detail/runtime_public_io_socket_connect_submit_fragment.hpp`: 46 lines.
- `include/af/detail/runtime_executor_io_uring_socket_accept_connect_submit_fragment.hpp`: 6 lines.
- `include/af/detail/runtime_executor_io_uring_socket_accept_submit_fragment.hpp`: 139 lines.
- `include/af/detail/runtime_executor_io_uring_socket_connect_submit_fragment.hpp`: 41 lines.
- `include/af/detail/runtime_executor_io_uring_file_resource_fragment.hpp`: 7 lines.
- `include/af/detail/runtime_executor_io_uring_file_register_fragment.hpp`: 60 lines.
- `include/af/detail/runtime_executor_io_uring_file_unregister_fragment.hpp`: 67 lines.
- `include/af/detail/runtime_executor_io_uring_file_update_fragment.hpp`: 85 lines.
- `include/af/detail/runtime_executor_io_uring_buffer_resource_fragment.hpp`: 8 lines.
- `include/af/detail/runtime_executor_io_uring_buffer_register_fragment.hpp`: 60 lines.
- `include/af/detail/runtime_executor_io_uring_buffer_unregister_fragment.hpp`: 67 lines.
- `include/af/detail/runtime_executor_io_uring_provided_buffer_resource_fragment.hpp`: 6 lines.
- `include/af/detail/runtime_executor_io_uring_provided_buffer_register_fragment.hpp`: 75 lines.
- `include/af/detail/runtime_executor_io_uring_provided_buffer_unregister_fragment.hpp`: 74 lines.
- `include/af/detail/runtime_executor_io_uring_generic_submit_sqe_fragment.hpp`: 8 lines.
- `include/af/detail/runtime_executor_io_uring_generic_submit_sqe_dispatch_fragment.hpp`: 67 lines.
- `include/af/detail/runtime_executor_io_uring_generic_submit_sqe_filesystem_fragment.hpp`: 56 lines.
- `include/af/detail/runtime_executor_io_uring_generic_submit_sqe_socket_fragment.hpp`: 49 lines.
- `include/af/detail/runtime_executor_io_uring_generic_submit_sqe_buffer_fragment.hpp`: 22 lines.
- `include/af/detail/runtime_executor_io_wait_fragment.hpp`: 102 lines.
- `include/af/detail/io_common_uring_status_fragment.hpp`: 37 lines.
- `include/af/detail/io_timeout_status_fragment.hpp`: 37 lines.
- `include/af/detail/io_uring_support.hpp`: 23 lines.
- `include/af/detail/io_uring_support_abi.hpp`: 63 lines.
- `include/af/detail/io_uring_support_opcode.hpp`: 17 lines.
- `include/af/detail/io_uring_support_types.hpp`: 61 lines.
- `include/af/detail/io_uring_support_syscall.hpp`: 44 lines.
- `include/af/detail/io_uring_support_sqe.hpp`: 38 lines.
- `include/af/detail/basic_task_fragment.hpp`: 317 lines, intentionally kept as one cohesive class definition with no class-body `#include` splicing.
- `include/af/detail/object_pool.hpp`: 196 lines, intentionally kept as one cohesive class definition with no class-body `#include` splicing.
- `include/af/detail/io_adapters_stream.hpp`: 156 lines, intentionally kept as one cohesive `IoStream` class definition with no class-body `#include` splicing.
- `include/af/detail/io_adapters_datagram.hpp`: 121 lines, intentionally kept as one cohesive `IoDatagramSocket` class definition with no class-body `#include` splicing.
- `include/af/detail/io_adapters_file_descriptor.hpp`: 144 lines, intentionally kept as one cohesive `IoFile` class definition with no class-body `#include` splicing.
- `include/af/detail/io_adapters_fixed_file.hpp`: 177 lines, intentionally kept as one cohesive `IoFixedFile` class definition with no class-body `#include` splicing.
- `tests/runtime_lifecycle_stress_tests.cpp`: 63 lines.
- `tests/runtime_config_tests.cpp`: 30 lines.
- `tests/runtime_self_post_stress_tests.cpp`: 117 lines.
- `tests/runtime_cross_thread_stress_tests.cpp`: 123 lines.
- `tests/runtime_parallel_stress_tests.cpp`: 96 lines.
- `tests/runtime_running_pending_stress_tests.cpp`: 142 lines.
- `tests/pool_tests.cpp`: 79 lines.
- `tests/support/runtime_lifecycle_stress_support.hpp`: 109 lines.
- `tests/support/runtime_scheduler_stress_support.hpp`: 22 lines.
- `tests/support/runtime_scheduler_stress_self_post_fragment.hpp`: 157 lines.
- `tests/support/runtime_scheduler_stress_repeat_hop_fragment.hpp`: 83 lines.
- `tests/support/runtime_scheduler_stress_wide_hop_fragment.hpp`: 70 lines.
- `tests/support/runtime_scheduler_stress_parallel_resume_fragment.hpp`: 112 lines.
- `tests/support/runtime_scheduler_stress_running_pending_fragment.hpp`: 160 lines.
- `tests/support/runtime_scheduler_stress_running_wake_terminal_fragment.hpp`: 149 lines.
- `tests/support/runtime_scheduler_stress_wait_fragment.hpp`: 15 lines.
- `tests/runtime_io_uring_socket_stream_tests.cpp`: 199 lines.
- `tests/support/runtime_io_uring_socket_stream_recv_cancel_tasks_fragment.hpp`: 107 lines.

Validation evidence for the final pass:

- Local `git diff --check`: passed.
- Local Release build: `asyncflow_runtime_tests` and `asyncflow_runtime_benchmarks` built.
- Local Release targeted scheduler/parallel tests repeated until failure 20 times: 16/16 passed.
- Local TSAN targeted scheduler/parallel tests: 16/16 passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug targeted scheduler/parallel tests: 16/16 passed.
- Remote clang TSAN targeted scheduler/parallel tests: 16/16 passed with no ThreadSanitizer report.
- Remote clang Release targeted scheduler/parallel tests: 16/16 passed.
- Remote clang Release full runtime test suite: 132/132 passed, with 21 platform/io_uring capability tests skipped by test logic.
- Remote clang Release `BM_RuntimeParallelShards/128` was run 100 times under `timeout 10s`; all iterations exited successfully.

Additional validation after the mechanical executor pop/finish split:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release targeted scheduler/parallel tests: 16/16 passed.
- Remote clang Debug targeted scheduler/parallel tests: 16/16 passed.
- Remote clang TSAN targeted scheduler/parallel tests: 16/16 passed with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 132/132 passed, with 21 platform/io_uring capability tests skipped by test logic.

Additional validation after the mechanical executor task split:

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release targeted scheduler/parallel tests: 16/16 passed.
- Remote clang Debug targeted scheduler/parallel tests: 16/16 passed.
- Remote clang TSAN targeted scheduler/parallel tests: 16/16 passed with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 132/132 passed, with 21 platform/io_uring capability tests skipped by test logic.
- Remote clang Release runtime benchmark canary:
  - `BM_RuntimeExternalStart/8192`: 7.56 ms real, 1.083 M/s.
  - `BM_RuntimeCrossThreadHop/8192`: 12.3 ms real, 668.570 k/s.
  - `BM_RuntimeIoThreadHop/8192`: 4.65 ms real, 1.763 M/s.
  - `BM_RuntimeParallelShards/128`: 0.507 ms real, 252.415 k/s.
  - `BM_RuntimeParallelShards/512`: 1.93 ms real, 265.746 k/s.

Additional validation after the mechanical `BasicTask` split:

Status: superseded by the BasicTask de-fragmenting pass below. The earlier class-body split was mechanically correct but is no longer considered the desired modularity style.

- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release task/scheduler/parallel targeted tests: 24/24 passed.
- Remote clang Debug task/scheduler/parallel targeted tests: 24/24 passed.
- Remote clang TSAN task/scheduler/parallel targeted tests: 24/24 passed with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 132/132 passed, with 21 platform/io_uring capability tests skipped by test logic.
- Remote clang Release runtime benchmark, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_RuntimeExternalStart/8192` mean: 7.23 ms real, 1.138 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 12.8 ms real, 643.541 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.44 ms real, 1.845 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.519 ms real, 246.555 k/s.
  - `BM_RuntimeParallelShards/512` mean: 1.97 ms real, 261.281 k/s.

Additional validation after the `BasicTask` de-fragmenting pass:

- Removed the class-body include splice in `basic_task_fragment.hpp`.
- Deleted the now-obsolete public/protected/lifetime/schedule/storage BasicTask fragment headers.
- Kept all task lifecycle and scheduling logic inline/template-visible inside one readable `BasicTask` definition; no queue route, task state transition, atomic ordering, lock, allocation, or vtable behavior changed.
- Local `git diff --check`: passed.
- Remote clang Debug build of `asyncflow_runtime_tests`: passed.
- Remote clang Debug task/lifecycle/scheduler/parallel targeted tests: 47/47 passed.
- Remote clang TSAN task/lifecycle/scheduler/parallel targeted tests: 41/41 passed with no ThreadSanitizer report.
- Remote clang Release task/lifecycle/scheduler/parallel targeted tests: 41/41 passed.
- Remote clang Release runtime benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_RuntimeExternalStart/8192` mean: 6.39 ms real, 1.299 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 13.9 ms real, 591.198 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 5.65 ms real, 1.458 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.570 ms real, 226.356 k/s.
  - `BM_RuntimeParallelShards/512` mean: 1.85 ms real, 276.807 k/s.

Additional validation after the `ObjectPool` de-fragmenting pass:

- Removed the class-body include splice in `object_pool.hpp`.
- Deleted the now-obsolete storage/slot-ops/lifecycle ObjectPool fragment headers.
- Kept TLS cache, cache-line slot sizing, MPMC free-list, slot acquire/release, lifecycle, and hot-block atomics together in one readable class definition; no memory ordering, lock, allocation policy, queue type, or cache alignment changed.
- Local `git diff --check`: passed.
- Remote clang Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug Pool/Runtime/Stress targeted tests: 27/27 passed.
- Remote clang TSAN Pool/Runtime/Stress targeted tests: 27/27 passed with no ThreadSanitizer report.
- Remote clang Release Pool/Runtime/Stress targeted tests: 27/27 passed.
- Remote clang Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_ObjectPoolCreateDestroy/16384` mean: 48,464 ns real, 341.805 M/s.
  - `BM_RuntimeExternalStart/8192` mean: 6.69 ms real, 1.230 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 13.4 ms real, 613.925 k/s.

Additional validation after the adapter class de-fragmenting pass:

- Removed class-body include splices from `IoStream`, `IoDatagramSocket`, `IoFile`, and `IoFixedFile`.
- Deleted the 16 now-obsolete adapter method fragment headers that only existed to splice recv/send/read/write/fixed/sync methods into class bodies.
- Kept adapter objects as two-field trivially-copyable views and kept all forwarding helpers inline/template-visible; no IO routing, queueing, syscall, io_uring submit, memory ordering, allocation, virtual dispatch, or ownership behavior changed.
- Local `git diff --check`: passed before the documentation update.
- Remote clang Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug adapter/stream/datagram/file-boundary targeted tests: 27/27 passed.
- Remote clang TSAN adapter/stream/datagram/file-boundary targeted tests: 27/27 passed with no ThreadSanitizer report.
- Remote clang Release adapter/stream/datagram/file-boundary targeted tests: 27/27 passed.
- Remote clang Release IO adapter benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_IoDatagramAdapterZeroByteRecv` mean: 0.640 ns real.
  - `BM_IoFileAdapterZeroByteRead` mean: 0.628 ns real.
  - `BM_IoStreamAdapterZeroByteSend` mean: 0.627 ns real.
  - `BM_IoStreamAdapterZeroByteSendZc` mean: 0.627 ns real.
  - `BM_IoFileAdapterZeroByteReadFixedAt` mean: 0.630 ns real.

Additional validation after the `io_common` split and epoll delete race fix:

- `io_common_detail_state_fragment.hpp` is now a 13-line umbrella:
  - `io_common_target_fragment.hpp`: 39 lines.
  - `io_common_wait_arm_fragment.hpp`: 48 lines.
  - `io_common_wait_state_fragment.hpp`: 35 lines.
  - `io_common_uring_status_fragment.hpp`: 34 lines.
  - `io_common_iovec_fragment.hpp`: 47 lines.
- Remote clang TSAN exposed a real deferred epoll delete race: a test could close an fd after the runtime posted the resumed task while the IO executor still had a deferred `EPOLL_CTL_DEL` pending for the same numeric fd.
- The fix deletes the epoll interest on the IO thread before exposing the readiness result and waking the task. The obsolete deferred-delete set and `IoOpState` readiness rearm hint were removed; epoll now registers with `EPOLL_CTL_ADD` first and only falls back to `MOD` for defensive `EEXIST`.
- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release targeted IO tests: 82/82 passed; platform/io_uring capability tests skipped by local test logic where unsupported.
- Remote clang Debug targeted IO tests: 82/82 passed.
- Remote clang TSAN targeted IO tests: 82/82 passed with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 132/132 passed, with 21 platform/io_uring capability tests skipped by test logic.
- Remote clang Release runtime benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_RuntimeExternalStart/8192` mean: 7.17 ms real, 1.144 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 12.4 ms real, 665.491 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.41 ms real, 1.860 M/s.
  - `BM_RuntimeParallelShards/512` mean: 1.82 ms real, 281.439 k/s.
- Remote clang Release IO adapter canary stayed in the sub-nanosecond range for the zero-byte/zero-iov helper paths covered by existing benchmarks.

Additional validation after the runtime common-state split:

- `runtime_common_fragment.hpp` is now a 9-line umbrella:
  - `runtime_status_fragment.hpp`: 10 lines.
  - `runtime_cache_line_atomic_fragment.hpp`: 62 lines.
  - `runtime_ordered_batch_state_fragment.hpp`: 7 lines.
  - `runtime_parallel_group_fragment.hpp`: 33 lines.
  - `runtime_external_post_counter_fragment.hpp`: 7 lines.
- Removed the obsolete public `io_deferred_delete_reserve` tuning knob. The deferred epoll delete path no longer exists, so keeping the knob would expose a no-op performance API.
- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release runtime/scheduler/parallel targeted tests: 39/39 passed.
- Remote clang Debug runtime/scheduler/parallel targeted tests: 39/39 passed.
- Remote clang TSAN runtime/scheduler/parallel targeted tests: 39/39 passed with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 132/132 passed, with 21 platform/io_uring capability tests skipped by test logic.
- Remote clang Release runtime benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_RuntimeExternalStart/8192` mean: 6.91 ms real, 1.197 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 11.9 ms real, 689.139 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.44 ms real, 1.846 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.477 ms real, 268.859 k/s.
  - `BM_RuntimeParallelShards/512` mean: 2.03 ms real, 253.068 k/s.

Additional validation after the runtime stress source split:

- Removed the old combined `tests/runtime_stress_tests.cpp`.
- Split runtime stress cases into `tests/runtime_lifecycle_stress_tests.cpp`, `tests/runtime_cross_thread_stress_tests.cpp`, and `tests/runtime_parallel_stress_tests.cpp`.
- Added `tests/support/runtime_lifecycle_stress_support.hpp` for lifecycle stress runtime/task scaffolding. Cross-thread hop and parallel shard stress scaffolding initially remained in `tests/support/runtime_scheduler_stress_support.hpp`; a later pass split that support header into smaller scenario fragments.
- No runtime scheduling or IO behavior changed in this pass; the split is test-structure only.
- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release `RuntimeStressTests`: 4/4 passed.
- Remote clang Debug `RuntimeStressTests`: 4/4 passed.
- Remote clang TSAN `RuntimeStressTests`: 4/4 passed with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 132/132 passed, with 21 platform/io_uring capability tests skipped by test logic.

Additional validation after the UDP datagram test helper split:

- Added `tests/support/runtime_io_udp_socket_helpers_fragment.hpp` for UDP loopback socket setup and simple send/receive probes.
- `tests/runtime_io_datagram_tests.cpp` is now 88 lines, `tests/runtime_io_datagram_send_tests.cpp` is 95 lines, and `tests/runtime_io_uring_socket_datagram_tests.cpp` is 151 lines.
- The shared `tests/runtime_io_test_support.hpp` remains a 225-line umbrella over test support fragments.
- No runtime scheduling, IO backend, queue, memory-ordering, or public API behavior changed in this pass; only test fixture setup was de-duplicated.
- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release datagram-targeted tests: 14/14 passed, all skipped by local platform/backend logic.
- Remote clang Debug datagram-targeted tests: 14/14 passed.
- Remote clang TSAN datagram-targeted tests: 14/14 passed with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 132/132 passed, with 21 platform/io_uring capability tests skipped by test logic.

Additional validation after the stream transfer test helper split:

- Added `tests/support/runtime_io_stream_transfer_helpers_fragment.hpp` for temporary files, stream socket pairs, blocked TCP connections, and pipe pairs used by sendfile/splice tests.
- `tests/runtime_io_stream_transfer_tests.cpp` is now 219 lines, with resource setup separated from sendfile, splice, and io_uring-poll readiness assertions.
- The shared `tests/runtime_io_test_support.hpp` remains a 229-line umbrella over test support fragments.
- No runtime scheduling, IO backend, queue, memory-ordering, or public API behavior changed in this pass; only test fixture setup and cleanup ownership moved into RAII-style helpers.
- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release sendfile/splice-targeted tests: 5/5 passed, all skipped by local platform/backend logic.
- Remote clang Debug sendfile-targeted tests: 4/4 passed, with 2 io_uring-poll capability tests skipped by test logic.
- Remote clang Debug splice-targeted tests: 1/1 passed.
- Remote clang TSAN sendfile-targeted tests: 4/4 passed, with 2 io_uring-poll capability tests skipped by test logic and no ThreadSanitizer report.
- Remote clang TSAN splice-targeted tests: 1/1 passed with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 132/132 passed, with 21 platform/io_uring capability tests skipped by test logic.

Additional validation after the scheduler stress support split:

- `tests/support/runtime_scheduler_stress_support.hpp` is now a 20-line umbrella.
- Repeat cross-thread hop support lives in `tests/support/runtime_scheduler_stress_repeat_hop_fragment.hpp`.
- Above-64 ready-source hop support lives in `tests/support/runtime_scheduler_stress_wide_hop_fragment.hpp`.
- Parallel owner-resume stress support lives in `tests/support/runtime_scheduler_stress_parallel_resume_fragment.hpp`.
- The shared zero-wait helper lives in `tests/support/runtime_scheduler_stress_wait_fragment.hpp`.
- No runtime scheduling, queue selection, memory ordering, task state transition, or public API behavior changed in this pass; only test support ownership changed.
- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed.
- Local Release `RuntimeStressTests`: 4/4 passed.
- Remote clang Debug `RuntimeStressTests`: 4/4 passed.
- Remote clang TSAN `RuntimeStressTests`: 4/4 passed with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 132/132 passed, with 21 platform/io_uring capability tests skipped by test logic.

Additional validation after the length-prefixed RPC server example split:

- `examples/support/io_rpc_length_prefixed_server.hpp` is now a 22-line umbrella.
- `examples/support/io_rpc_length_prefixed_process_task_decl.hpp` declares the logic-thread processing task and its dependency on the server task.
- `examples/support/io_rpc_length_prefixed_server_task.hpp` owns the IO-thread accept/read/write state machine.
- `examples/support/io_rpc_length_prefixed_process_task_impl.hpp` owns the PING-to-PONG processing logic and reposts the server task to the IO thread.
- No runtime scheduling, IO backend, queue, memory-ordering, or public API behavior changed in this pass; only example support ownership changed.
- Local `git diff --check`: passed.
- Local Release `asyncflow_io_rpc_length_prefixed_example` build: passed.
- Remote clang Debug `asyncflow_io_rpc_length_prefixed_example` build/run: passed with `rpc response_ok=1`.
- Remote clang TSAN `asyncflow_io_rpc_length_prefixed_example` build/run: passed with `rpc response_ok=1` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_rpc_length_prefixed_example` build/run: passed with `rpc response_ok=1`.

Additional validation after the length-prefixed RPC client example split:

Status: superseded by the example task de-fragmenting pass below. The earlier split was mechanically correct, but it used class-body `#include` splicing and did not create an independent abstraction boundary.

- `examples/support/io_rpc_length_prefixed_client.hpp` was temporarily a client task shell over flow, request-send, and response-read fragments.
- No runtime scheduling, IO backend, queue, memory-ordering, or public API behavior changed in this pass; only example support ownership changed.
- Local `git diff --check`: passed.
- Local Release `asyncflow_io_rpc_length_prefixed_example` build: passed; local run reported `rpc length-prefixed example is Linux-only`.
- Remote clang Debug `asyncflow_io_rpc_length_prefixed_example` build/run: passed with `rpc length-prefixed backend=epoll-fallback` and `rpc response_ok=1`.
- Remote clang TSAN `asyncflow_io_rpc_length_prefixed_example` build/run: passed with `rpc response_ok=1` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_rpc_length_prefixed_example` build/run: passed with `rpc response_ok=1`.
- The remote build log showed a short clock-skew warning after rsync because modified file timestamps were a few seconds ahead of the container clock; each configuration still rebuilt the target and completed the RPC round trip.

Additional validation after the io_uring fixed-file task split:

Status: superseded by the example task de-fragmenting pass below. The earlier split was mechanically correct, but it used class-body `#include` splicing and did not create an independent abstraction boundary.

- `examples/support/io_uring_fixed_file_task.hpp` was temporarily a task shell over flow, IO-operation, and registration/update fragments.
- No runtime scheduling, IO backend, queue, memory-ordering, or public API behavior changed in this pass; only example support ownership changed.
- Local `git diff --check`: passed.
- Local Release `asyncflow_io_uring_fixed_file_example` build: passed; local run reported `io_uring fixed file example is Linux-only`.
- Remote clang Debug `asyncflow_io_uring_fixed_file_example` build/run: passed with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_io_uring_fixed_file_example` build/run: passed with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_uring_fixed_file_example` build/run: passed with `io_uring backend unavailable`.

Additional validation after the io_uring file-lifecycle task split:

Status: superseded by the example task de-fragmenting pass below. The earlier split was mechanically correct, but it used class-body `#include` splicing and did not create an independent abstraction boundary.

- `examples/support/io_uring_file_lifecycle_task.hpp` was temporarily a task shell over flow, file-operation, and namespace-operation fragments.
- No runtime scheduling, IO backend, queue, memory-ordering, or public API behavior changed in this pass; only example support ownership changed.
- Local `git diff --check`: passed.
- Local Release `asyncflow_io_uring_file_lifecycle_example` build: passed; local run reported `io_uring lifecycle example is Linux-only`.
- Remote clang Debug `asyncflow_io_uring_file_lifecycle_example` build/run: passed with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_io_uring_file_lifecycle_example` build/run: passed with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_uring_file_lifecycle_example` build/run: passed with `io_uring backend unavailable`.

Additional validation after the io_uring UDP recvmsg multishot task split:

Status: superseded by the example task de-fragmenting pass below. The earlier split was mechanically correct, but it used class-body `#include` splicing and did not create an independent abstraction boundary.

- `examples/support/io_uring_udp_recvmsg_multishot_task.hpp` was temporarily a task shell over flow, provided-buffer ring, and recvmsg multishot fragments.
- No runtime scheduling, IO backend, queue, memory-ordering, or public API behavior changed in this pass; only example support ownership changed.
- Local `git diff --check`: passed.
- Local Release `asyncflow_io_uring_udp_recvmsg_multishot_example` build: passed; local run reported `io_uring UDP recvmsg_multishot example is Linux-only`.
- Remote clang Debug `asyncflow_io_uring_udp_recvmsg_multishot_example` build/run: passed with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_io_uring_udp_recvmsg_multishot_example` build/run: passed with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_uring_udp_recvmsg_multishot_example` build/run: passed with `io_uring backend unavailable`.

Additional validation after the io_uring UDP recv multishot task split:

Status: superseded by the example task de-fragmenting pass below. The earlier split was mechanically correct, but it used class-body `#include` splicing and did not create an independent abstraction boundary.

- `examples/support/io_uring_udp_recv_multishot_task.hpp` was temporarily a task shell over flow, provided-buffer ring, and recv multishot fragments.
- No runtime scheduling, IO backend, queue, memory-ordering, or public API behavior changed in this pass; only example support ownership changed.
- Local `git diff --check`: passed.
- Local Release `asyncflow_io_uring_udp_recv_multishot_example` build: passed; local run reported `io_uring UDP recv_multishot example is Linux-only`.
- Remote clang Debug `asyncflow_io_uring_udp_recv_multishot_example` build/run: passed with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_io_uring_udp_recv_multishot_example` build/run: passed with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_uring_udp_recv_multishot_example` build/run: passed with `io_uring backend unavailable`.

Additional validation after the io_uring stream recv multishot task split:

Status: superseded by the example task de-fragmenting pass below. The earlier split was mechanically correct, but it used class-body `#include` splicing and did not create an independent abstraction boundary.

- `examples/support/io_uring_recv_multishot_task.hpp` was temporarily a task shell over flow, provided-buffer ring, and recv multishot fragments.
- No runtime scheduling, IO backend, queue, memory-ordering, or public API behavior changed in this pass; only example support ownership changed.
- Local `git diff --check`: passed.
- Local Release `asyncflow_io_uring_recv_multishot_example` build: passed; local run reported `io_uring recv_multishot example is Linux-only`.
- Remote clang Debug `asyncflow_io_uring_recv_multishot_example` build/run: passed with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_io_uring_recv_multishot_example` build/run: passed with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_io_uring_recv_multishot_example` build/run: passed with `io_uring backend unavailable`.

Additional validation after the example task de-fragmenting pass:

- `FixedFileRoundTripTask`, `FileLifecycleTask`, `FilesystemOpsTask`, `RecvMultishotTask`, `UdpRecvMultishotTask`, `UdpRecvmsgMultishotTask`, and `RpcClientTask` now keep each state machine in one cohesive class definition.
- Removed 21 fragment headers that existed only to splice private methods into those seven example task class bodies.
- `io_rpc_length_prefixed_server.hpp` remains a namespace-level composition of process task declaration, server task, and process task implementation; it is not a class-body splice.
- No runtime scheduling, IO backend, queue, memory-ordering, public API, task field layout, or example assertion behavior changed in this pass; only over-fragmented example support source layout changed.
- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` Debug build passed for all seven affected example targets.
- Remote clang Debug run under `--security-opt seccomp=unconfined` passed for all seven executables, covering RPC, lifecycle/statx, fixed-file, stream recv multishot, UDP recv multishot, and UDP recvmsg multishot paths.

Additional validation after the io_uring file lifecycle test support split:

Status: superseded by the io_uring file test support de-fragmenting pass below. The earlier split was mechanically correct, but it used class-body `#include` splicing and did not create an independent abstraction boundary.

- `tests/support/runtime_io_file_lifecycle_tasks_fragment.hpp` was temporarily a 63-line task shell over flow, file-operation, and namespace-operation fragments.
- No runtime scheduling, IO backend, queue, memory-ordering, public API, or test assertion behavior changed in this pass; only test support ownership changed.
- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed; local targeted run reported `io_uring backend is Linux-only`.
- Remote clang Debug `asyncflow_runtime_tests` targeted run: skipped by test logic with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_runtime_tests` targeted run: skipped by test logic with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_runtime_tests` targeted run: skipped by test logic with `io_uring backend unavailable`.

Additional validation after the io_uring fixed-file read/write test support split:

Status: superseded by the io_uring file test support de-fragmenting pass below. The earlier split was mechanically correct, but it used class-body `#include` splicing and did not create an independent abstraction boundary.

- `tests/support/runtime_io_file_fixed_file_rw_tasks_fragment.hpp` was temporarily a 53-line task shell over flow, registration, and IO-operation fragments.
- No runtime scheduling, IO backend, queue, memory-ordering, public API, task field layout, or test assertion behavior changed in this pass; only test support ownership changed.
- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed; local targeted run reported `io_uring backend is Linux-only`.
- Remote clang Debug `asyncflow_runtime_tests` targeted run: skipped by test logic with `io_uring backend unavailable`.
- Remote clang TSAN `asyncflow_runtime_tests` targeted run: skipped by test logic with `io_uring backend unavailable` and no ThreadSanitizer report.
- Remote clang Release `asyncflow_runtime_tests` targeted run: skipped by test logic with `io_uring backend unavailable`.

Additional validation after the io_uring file test support de-fragmenting pass:

- `tests/support/runtime_io_file_lifecycle_tasks_fragment.hpp` is now a cohesive `UringFileLifecycleTask` class definition again, with the state dispatcher and file/namespace operation handlers kept in declaration order.
- `tests/support/runtime_io_file_fixed_file_rw_tasks_fragment.hpp` is now a cohesive `UringFixedFileTask` class definition again, with registration, fixed-buffer IO, vectored IO, fsync, and unregister handling kept in declaration order.
- Removed six fragment headers that existed only to splice private methods into those two class bodies.
- No runtime scheduling, IO backend, queue, memory-ordering, public API, task field layout, or test assertion behavior changed in this pass; only over-fragmented test support source layout changed.
- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` fresh Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug `UringIoRuntimeFileFixture.*` targeted run: 11 tests loaded and skipped by test logic with `io_uring backend unavailable`.
- Remote clang Debug full `asyncflow_runtime_tests` run: 141 tests, 118 passed, 23 skipped.

Additional validation after the io_uring stream/UDP recv multishot test support split and backend diagnosis:

Status: superseded by the io_uring filesystem/multishot test support de-fragmenting pass below. The earlier split was mechanically correct, but it used class-body `#include` splicing and did not create an independent abstraction boundary.

- `tests/support/runtime_io_uring_socket_recv_multishot_tasks_fragment.hpp` was temporarily an 84-line task shell over flow, provided-buffer ring, and recv/cancel fragments.
- `AsyncRuntime::io_uring_backend_error(thread)` now exposes io_uring setup/runtime failure errno without changing the hot submit/completion path when the backend is available.
- Initial remote host evidence: `CONFIG_IO_URING=y`, `kernel.io_uring_disabled=2`, `kernel.io_uring_group=-1`, and host-root direct `io_uring_setup` returned `EPERM`.
- After temporary host enablement, `kernel.io_uring_disabled=0`, host-root direct `io_uring_setup` succeeded, `seccomp=unconfined` and `--privileged` containers succeeded, and only the default Docker seccomp profile still returned `EPERM`.
- Local `git diff --check`: passed.
- Local Release `asyncflow_runtime_tests` build: passed; local targeted run reported Linux-only skips.
- Remote clang Debug/TSAN/Release `asyncflow_runtime_tests --gtest_filter=*Uring*.*` under `--security-opt seccomp=unconfined`: 35/37 passed, 2 direct-descriptor capability tests skipped, 0 failed, and TSAN reported no races.
- Remote clang Debug full `asyncflow_runtime_tests` under `--security-opt seccomp=unconfined`: 130/133 passed, 3 skipped, 0 failed.
- Remote clang TSAN full `asyncflow_runtime_tests` under `--security-opt seccomp=unconfined`: 130/133 passed, 3 skipped, no ThreadSanitizer report.
- Remote clang Release full `asyncflow_runtime_tests` under `--security-opt seccomp=unconfined`: 130/133 passed, 3 skipped, 0 failed.
- The first broad Debug run exposed two failing io_uring poll-readiness sendfile tests. The test fixture used a full AF_UNIX socketpair for `sendfile`; switching it to the same full TCP connection shape used by the epoll sendfile wait test made both poll-resume and cancel cases pass in Debug, TSAN, and Release.

Additional validation after the io_uring filesystem/multishot test support de-fragmenting pass:

- `tests/support/runtime_io_uring_socket_recv_multishot_tasks_fragment.hpp` is now a cohesive `UringRecvMultishotTask` class definition again, with the state dispatcher, provided-buffer ring lifecycle, recv completion, buffer recycling, stop, and cancel handling kept in declaration order.
- `tests/support/runtime_io_file_filesystem_ops_tasks_fragment.hpp` is now a cohesive `UringFilesystemOpsTask` class definition again, with mkdir/open/write/truncate/fsync/stat/close/link/symlink/unlink/rmdir handling kept in declaration order.
- Removed six fragment headers that existed only to splice private methods into those two class bodies.
- No runtime scheduling, IO backend, queue, memory-ordering, public API, task field layout, or test assertion behavior changed in this pass; only over-fragmented test support source layout changed.
- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.0` fresh Debug `asyncflow_runtime_tests` build: passed.
- Remote clang Debug targeted run under `--security-opt seccomp=unconfined`: 4/4 passed for `UringIoRuntimeSocketMultishotFixture.*` and `UringIoRuntimeFileFixture.IoUringFilesystemOpsRunOnIoThread`.
- Remote clang Debug full `asyncflow_runtime_tests` under `--security-opt seccomp=unconfined`: 141 tests, 138 passed, 3 skipped.

Additional validation after the explicit ready-queue route and self-post stress tests:

- `runtime_ready_enqueue_fragment.hpp` now names `ReadyQueueRoute::Local` and `ReadyQueueRoute::Spsc` before dispatching runtime-thread posts. Same-owner posts route to the executor local queue; cross-owner posts route to the SPSC queue.
- `tests/runtime_self_post_stress_tests.cpp` adds a single-thread fanout test. If same-thread runtime posts are accidentally sent to the self SPSC queue, the test times out because the executor only drains the local FIFO for self-post work.
- The same test also verifies same-thread fanout FIFO order, and a separate `again()` stress test verifies the `finish_again()` self-reschedule path does not depend on cross-thread ready hints.
- Local `git diff --check`: passed.
- Remote clang Debug targeted scheduler stress tests: 5/5 passed.
- Remote clang TSAN targeted scheduler stress tests: 5/5 passed, no ThreadSanitizer report.
- Remote clang Release targeted scheduler stress tests: 5/5 passed.
- Remote clang Release full runtime test suite: 135 total, 132 passed, 3 platform/kernel capability tests skipped, 0 failed.
- Remote clang Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_RuntimeExternalStart/8192` mean: 7.08 ms real, 1.181 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 11.6 ms real, 705.325 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.23 ms real, 1.936 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.500 ms real, 256.161 k/s.

Additional validation after the ready-source word cursor rotation:

- `runtime_executor_pop_fragment.hpp` now rotates multi-word ready-source hint scans with executor-owned `next_ready_word_` instead of starting every hint pass at word 0. This reduces scan bias for runtimes whose ready-source set spans multiple 64-bit words.
- `runtime_executor_core_state_fragment.hpp` keeps the new `std::uint16_t` cursor next to `next_source_`. It is executor-private state, so the change adds no locks, no atomics, no heap allocation, and no queue topology change.
- Ready-source bits remain hints, not correctness state. The bounded all-source SPSC fallback scan is unchanged, so stale/lost/coalesced hint edges still cannot strand cross-thread work.
- Local `git diff --check`: passed.
- Remote clang Debug targeted scheduler stress tests: 5/5 passed.
- Remote clang TSAN targeted scheduler stress tests: 5/5 passed, no ThreadSanitizer report.
- Remote clang Release targeted scheduler stress tests: 5/5 passed.
- Remote clang Release full runtime test suite: 135 total, 132 passed, 3 platform/kernel capability tests skipped, 0 failed.
- Remote clang Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_RuntimeExternalStart/8192` mean: 6.88 ms real, 1.194 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 11.6 ms real, 709.526 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.22 ms real, 1.940 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.541 ms real, 237.390 k/s.

Additional validation after the thread-count config boundary fix:

- `runtime_public_config_fragment.hpp` now checks `Traits::thread_count > 0` and `Traits::thread_count <= UINT16_MAX` before converting it to `std::uint16_t`. This avoids a silent wrap if a user provides a value outside the public thread-index representation.
- `tests/runtime_config_tests.cpp` adds compile-time and runtime coverage for a 257-thread configuration. This explicitly verifies that the runtime config path is not capped at 64 threads.
- The change does not alter queue topology, task scheduling, wake behavior, atomics, locks, or hot-path branch structure.
- Local `git diff --check`: passed.
- Remote clang Debug targeted config/scheduler tests: 5/5 passed.
- Remote clang TSAN targeted config/scheduler tests: 5/5 passed, no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 136 total, 133 passed, 3 platform/kernel capability tests skipped, 0 failed.

Additional validation after the Running -> Pending wake-boundary audit and ObjectPool split:

- `finish_pending()` was rechecked against the suspected lost-wake boundary. The current ordering publishes `Pending`, consumes any same-epoch running wake request, then calls `enqueue_pending_blocking()`, which first performs `Pending -> Queued`; if a concurrent Pending wake already won, no duplicate queue entry is produced.
- `tests/runtime_running_pending_stress_tests.cpp` adds direct coverage for the owner hang scenario: a waker task on another runtime thread posts the owner while the owner is still Running and about to return Pending. The owner must be requeued and complete.
- Superseded: `object_pool.hpp` was temporarily split into storage, slot operations, and lifecycle fragments, but that class-body split has been reverted. `PoolTests.ObjectPoolSupportsConcurrentCreateDestroy` remains the multi-threaded create/destroy coverage for the task/IO object-pool primitive.
- Local `git diff --check`: passed.
- Remote clang Debug targeted Running/Pending, owner-resume, self-post, and pool tests: 6/6 passed.
- Remote clang TSAN targeted Running/Pending, owner-resume, self-post, and pool tests: 6/6 passed, no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 138 total, 135 passed, 3 platform/kernel capability tests skipped, 0 failed.
- Remote clang Release queue/pool benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_SpscQueuePushPop/16384` mean: 19,566 ns real, 838.619 M/s.
  - `BM_MpscQueuePushPop/16384` mean: 135,152 ns real, 121.346 M/s.
  - `BM_ObjectPoolCreateDestroy/16384` mean: 39,830 ns real, 411.638 M/s.

Remaining follow-up:

- Add a dedicated large-thread-count benchmark/perf-counter run before using ready-word fairness as a hard regression gate. Current coverage validates above-64 correctness and scheduler canaries, but does not isolate 128+/256+ worker ready-word cache/branch behavior.

### 2026-06-01 Current Scheduler WIP and Modularity Re-scan

Status: superseded by the scheduler correctness and modularity pass above. This section is kept as the historical issue ledger that led to the fix.

This scan records the current state before continuing the next runtime split. There are active scheduler hot-path changes in the worktree, so the modularity work must treat correctness validation as part of the structure boundary, not as a cosmetic cleanup.

Current file-size snapshot:

- `include/af/async_runtime.hpp`: 226 lines. This is still an acceptable overview shell that declares `AsyncRuntime`, declares the nested `Executor`, and includes inline fragments in the required class scopes.
- `include/af/detail/runtime_dispatch_fragment.hpp`: 195 lines. It remains the main core-runtime split candidate because it owns queue topology, same-thread local enqueue, cross-thread SPSC enqueue, external MPSC enqueue, ready-source signaling, and external-post admission accounting.
- Superseded after this scan: `include/af/detail/basic_task_fragment.hpp` was temporarily split into class-body fragments, but that style has since been reverted because it made the class harder to read and maintain.
- Resolved after this scan: `include/af/detail/io_common_detail_state_fragment.hpp` is now a small umbrella over IO target, wait-arm, wait-state, io_uring-status, and iovec helper fragments.
- Resolved after this scan: the old combined `tests/runtime_stress_tests.cpp` was removed. Runtime stress test cases are split by concern, and reusable stress state machines live in support headers.
- The largest remaining examples/tests are now fixture/state-machine files, not the runtime shell. `runtime_io_uring_socket_datagram_tests.cpp`, `runtime_io_stream_transfer_tests.cpp`, `io_rpc_length_prefixed_server.hpp`, `io_uring_fixed_file_task.hpp`, `io_uring_file_lifecycle_task.hpp`, `io_uring_udp_recvmsg_multishot_task.hpp`, and `io_uring_udp_recv_multishot_task.hpp` have been reduced by moving repeated setup or role-specific tasks into support fragments.

Active correctness/performance issues to track:

- P0: cross-thread SPSC ready-source handling is still being validated. The ready-source bitmask should be treated as a fast hint and must not be the only correctness signal that queued SPSC work exists. Any change in `mark_ready()`, `pop_one()`, or `notify()` needs repeated cross-thread hop stress and Release benchmark validation before commit.
- P0: the executor must never let stale or duplicate queue entries move a task from `Pending`, `Done`, or `Running` back to `Running`. `execute()` should continue to require a `Queued -> Running` transition.
- P1: worker-thread notification and IO-thread native wakeup should stay separated. Worker atomic waits need a real `notify_one()` when work is posted; IO threads should prefer eventfd/kqueue native wake only when they are actually sleeping so hot IO submission does not pay avoidable syscalls.
- Resolved after this scan: `runtime_executor_core_state_fragment.hpp` now owns only executor field layout; pop/drain and finish/reschedule behavior moved to dedicated inline fragments while preserving declaration order for false-sharing audits.
- Resolved after this scan: `runtime_executor_task_fragment.hpp` is now an umbrella over ready-source/wake signaling, local queue push/pop, and execute/result dispatch fragments.
- P2: tests and examples should follow the same structure rule as runtime code. Test sources should contain test cases; reusable task state machines should live in `tests/support/*_fragment.hpp`. Examples should keep `main()` thin and put each state-machine role in focused support headers.

Modularity rule for the next pass:

- Keep all hot runtime paths inline/template-visible through fragments; do not move scheduling, queue, submit, completion, or task-resume code into `.cpp` files for aesthetics.
- Split by operation family or state-machine role, not by tiny helper extraction. The goal is clearer ownership without adding unpredictable branches, virtual dispatch, `std::function`, heap allocation, or extra atomics.
- Preserve same-thread local queue, cross-thread per-source SPSC queues, and external MPSC queues as distinct paths. A cleaner-looking generic queue path would be a performance regression.
- After each P1 split, run `git diff --check`, targeted Debug tests, remote/Docker Release scheduler stress, and runtime benchmark regression checks.

Recommended immediate order:

1. Finish and validate the cross-thread SPSC visibility/wakeup fix.
2. Completed: move repeated-hop stress support out of the old combined runtime stress source.
3. Split `runtime_dispatch_fragment.hpp` into queue topology, ready enqueue, and external-post admission fragments.
4. Completed: split executor pop/finish behavior out of `runtime_executor_core_state_fragment.hpp`, leaving only state layout in that file.
5. Completed: split `runtime_executor_task_fragment.hpp` once the preceding scheduler benchmarks were clean.

### 2026-06-01 Core Runtime Correctness and Modularity Follow-up

Status: superseded by the scheduler correctness and modularity pass above. Items marked P0/P1 here were the pre-fix review notes.

This follow-up recorded the active issues from that fixed-thread runtime review. At the time, the top-level `include/af/async_runtime.hpp` was 226 lines and was not the immediate problem; it was a class shell that kept template and hot-path fragments inline-visible. The risks were correctness-sensitive scheduler behavior and second-level fragments that still mixed responsibilities.

Issue ledger:

- P0: cross-thread SPSC wake visibility must not depend on the source-ready bitmask as the only correctness signal. A Release stress repro showed a cross-thread hop burst leaving one task undrained after the expected run count was short by several invocations. The ready bitmask should remain the fast hint path, but executor draining needs a bounded fallback scan of SPSC queues so a lost or coalesced ready edge cannot strand work. Any fix must pass the repeated cross-thread hop stress test and the Release runtime benchmark regression check before commit.
- P1: `runtime_dispatch_fragment.hpp` still owns queue topology, same-thread local enqueue, cross-thread SPSC enqueue, external MPSC enqueue, source-ready signaling, and external-post shutdown accounting. Split it into queue topology, ready enqueue, and external-post admission fragments while preserving the exact same local/SPSC/MPSC data paths.
- Resolved: `runtime_executor_core_state_fragment.hpp` now keeps the field declarations together as the cache-layout owner. `pop_one`, `finish_done`, `finish_pending`, and `finish_again` moved into focused inline fragments included before the state layout block.
- Resolved: `runtime_executor_task_fragment.hpp` is split into ready signaling/wake, local queue, and execute/finish dispatch fragments. The split was followed by same-thread, cross-thread, external-start, IO-thread hop, and parallel-shard benchmark canaries.
- Superseded: `basic_task_fragment.hpp` no longer uses class-body fragments. The task state machine is kept inline in one cohesive class definition.
- Resolved: `io_common_detail_state_fragment.hpp` is split by helper family, and the epoll readiness path no longer depends on a deferred-delete/rearm-hint cleanup path.
- Resolved: the old combined runtime stress source was removed. Lifecycle, cross-thread hop, and parallel shard stress cases now live in separate test sources; reusable state machines live in support headers.
- P2: several IO tests and examples remain moderately dense after the first pass. `runtime_io_uring_socket_datagram_tests.cpp` and `runtime_io_stream_transfer_tests.cpp` were reduced by extracting repeated fixture setup into shared test support; `io_rpc_length_prefixed_client.hpp`, `io_rpc_length_prefixed_server.hpp`, `io_uring_fixed_file_task.hpp`, and `io_uring_file_lifecycle_task.hpp` were reduced to small task/umbrella shells over role or operation-family fragments. Future edits should split by protocol role, transfer mode, or operation family instead of appending new states to the existing file.
- P2: older examples such as `io_epoll.cpp`, `io_event.cpp`, `io_timer.cpp`, `io_native_readiness.cpp`, and several multishot examples still use explicit atomics to observe readiness/completion from `main`. Prefer task-owned state machines plus `ShutdownPolicy::WaitForTasks` for examples unless the example is specifically demonstrating cross-thread observation.

Performance guardrails:

- Keep same-thread scheduling on the executor local queue; do not route self-posts through SPSC, MPSC, MPMC, or heap-backed generic queues.
- Keep cross-thread runtime posts on per-source SPSC queues; external posts stay on the external MPSC queue.
- Treat the ready-source bitmask as a fast path hint, not as the only source of truth for queued cross-thread work.
- Preserve executor field declaration order, cache-line alignment, and padding when splitting behavior out of state fragments.
- Avoid virtual dispatch, `std::function`, owning polymorphic adapters, extra heap allocation, or extra atomics purely for file-size reduction.
- Re-run `git diff --check`, targeted Debug tests, Docker/remote Release stress, and runtime benchmark baseline checks after every P1 split.

Recommended next split order:

1. Finish and validate the cross-thread SPSC visibility fix first, because it is a correctness issue hidden inside the same hot path that the modularity split will touch.
2. Split `runtime_dispatch_fragment.hpp` into topology, ready enqueue, and external-post admission fragments.
3. Completed: split executor pop/finish behavior out of `runtime_executor_core_state_fragment.hpp`, leaving field layout in that file.
4. Completed: split `runtime_executor_task_fragment.hpp` after the first two P1 splits benchmarked cleanly.
5. Completed: split runtime stress sources and support so lifecycle, cross-thread hop, and parallel shard regressions are easier to isolate.

### 2026-06-01 Core Fixed-Thread Runtime Modularity Audit

Status: superseded for current file layout by the runtime core de-fragmenting correction above. The remaining guidance about preserving inline hot paths and executor cache layout still applies.

At that point, the core runtime was no longer bottlenecked by `include/af/async_runtime.hpp` itself. That file was 226 lines and worked as a class shell: it declared `AsyncRuntime`, the nested `Executor`, and wired inline fragments into the correct class scopes. The current core-runtime structure is recorded in the Runtime Core De-Fragmenting Correction section above.

Findings to track:

- P1: `runtime_dispatch_fragment.hpp` still mixes queue topology setup, runtime-thread enqueue, external-thread enqueue, SPSC source-ready marking, and external-post drain accounting. This is correct as one hot path today, but it is the next best core split candidate. Split by responsibility into queue topology, enqueue fast path, and external-post gate fragments while keeping everything inline inside `AsyncRuntime`.
- Resolved: `runtime_public_lifecycle_fragment.hpp` is now split into lifecycle control, task creation/start helpers, post admission, and thread helpers.
- Resolved: `runtime_lifecycle_fragment.hpp` is now split into task pool, task handle lifetime, task registry/cancel, and task accounting fragments. The StopImmediately path still preserves the order: stop workers, cancel registered pending/queued tasks, then clear executor/backend state.
- Resolved: `runtime_executor_core_state_fragment.hpp` now contains only executor state fields. Pop/finish behavior lives in inline behavior fragments while the field declarations stay in one state-layout owner for cache-line placement and false-sharing audits.
- Resolved: `runtime_executor_task_fragment.hpp` is now split by source-ready/wake signaling, local queue operations, and task execution/result dispatch; benchmark canaries were collected after the change.
- Superseded: `basic_task_fragment.hpp` is no longer a class shell over fragments; the storage fields and state-machine methods are visible together in declaration order.
- Resolved: `io_common_detail_state_fragment.hpp` is split by helper family, making IO wait/cancel/timeout audits smaller.
- Resolved: `runtime_common_fragment.hpp` is split by state/type family; runtime status, cache-line atomic wrapper, ordered-batch state, parallel-group state, and external-post counters now live in focused fragments.
- P2: examples still contain explicit atomics for readiness/completion observation in older files such as `io_epoll.cpp`, `io_timer.cpp`, `io_event.cpp`, `io_native_readiness.cpp`, and several multishot io_uring examples. For examples, prefer task-owned state machines plus `ShutdownPolicy::WaitForTasks`; test fixtures may continue using atomics when they are only assertion probes.

Performance guardrails for the split:

- Do not move hot scheduling, queue, task state, IO submit, IO completion, or task-resume code into `.cpp` files. These paths need template/inline visibility.
- Do not introduce virtual adapters, `std::function`, heap allocation, or extra cross-thread queues for modularity.
- Do not split executor state fields across files unless the change is a deliberate cache-layout change with benchmark evidence.
- Keep same-thread scheduling on the local no-lock queue path and cross-thread scheduling on the per-source SPSC path. Any cleanup that routes same-thread posts through MPSC/MPMC would be a performance regression.
- After each P1 split, run `git diff --check`, Debug `ctest`, Docker GCC Debug `ctest`, and Release runtime benchmark baseline regression.

Recommended order:

1. Split `runtime_dispatch_fragment.hpp` into topology, enqueue, and external-post gate fragments. Re-run runtime benchmarks because this touches the start/post hot path.
2. Completed: split executor behavior out of `runtime_executor_core_state_fragment.hpp` while leaving field layout intact.
3. Superseded: the `basic_task_fragment.hpp` class-body split was reverted; future modularity work should split independent classes/algorithms/functions, not access-section fragments.
4. Completed: split `io_common_detail_state_fragment.hpp` by IO helper family and remove the stale epoll readiness rearm hint.

### 2026-05-31 Core Runtime Modularity Recheck

Historical scan result before the runtime core de-fragmenting correction: `include/af/async_runtime.hpp` was 226 lines and was no longer a monolithic implementation file. It mainly declared the public `AsyncRuntime` shell, the nested `Executor` shell, and included operation-family fragments inside the right class scope so template/hot-path code remained inline-visible.

Issue ledger from that scan:

- P1: do not move hot runtime submit/wait/completion/task-resume code into `.cpp` files just to reduce header length. That would make the code look cleaner while risking lost inlining, extra call overhead, and weaker optimizer visibility. Continue using inline fragments included inside `AsyncRuntime` or `AsyncRuntime::Executor`.
- P1: keep `runtime_executor_core_state_fragment.hpp` as the single executor state-layout owner unless a split explicitly preserves declaration order, alignment, and cache-line placement. Splitting state for aesthetics can silently introduce false sharing or make queue/cache layout audits harder.
- P1: future changes must not append new operation families directly into `async_runtime.hpp`. New public methods should enter through the existing public IO/resource/lifecycle/parallel umbrellas, and new executor operations should enter through the matching backend submit/completion fragments.
- P2: `include/af/detail/bounded_queues.hpp` still contains SPSC, MPSC, and MPMC bounded queues in one file. The implementation is performance-sensitive and cache-line aligned, so a split is acceptable only as a mechanical separation into queue-family headers with no layout or memory-order changes, followed by queue benchmarks.
- P2: several test support files remain dense state-machine collections: stream sendfile/splice support and io_uring recvmsg multishot support. File lifecycle, fixed-file read/write, and recv multishot support have been reduced to small shells over operation-family fragments. Remaining dense support files should continue moving toward operation-family task fragments when touched.
- P2: several examples remain long because they combine protocol framing, socket IO, task state machines, and result reporting. The fixed-file round trip, file lifecycle, UDP recvmsg multishot, UDP recv multishot, stream recv multishot, and length-prefixed RPC client/server paths have been reduced to focused fragments; the biggest current examples are now older readiness examples and remaining setup-heavy example entry points. Prefer protocol/helper headers plus small task headers for future edits.
- P2: `tests/utility_tests.cpp` is now one of the largest standalone tests. It should be split by utility domain if new utility coverage is added.

Assessment:

- `async_runtime.hpp` itself should stay as a shell. A further split of this file would mostly move include wiring around and would not materially improve runtime code ownership.
- The best next modularity work is second-level: split remaining dense fragments when they gain new functionality, not as a broad churn pass.
- Performance-sensitive files should be split only along existing operation-family boundaries: queue family, socket transfer type, fixed-file adapter method group, backend submit/completion family, and test/example task family.
- The example style is mostly converging toward member-function state handlers. Where a `run()` switch still exists, keep it as a tiny dispatcher and keep per-state logic in named member functions.

### 2026-05-31 Active Issue Ledger

That review found that `include/af/async_runtime.hpp` itself was no longer the primary modularity problem. It was 226 lines and mostly acted as an inline class shell plus fragment wiring. The remaining issues were second-level ownership boundaries:

- P2: the largest files are now tests and examples, not runtime entry points. The biggest current files are file IO support fragments, accept/socket support fragments, and protocol examples. New tests should be added as small operation-family files instead of growing the existing fixture files.
- P2: timeout/cancel race handling now lives in focused inline fragments. Keep the deadline arbitration fragment semantically intact unless new tests cover IO-first, timeout-first, user-cancel, cancel-completion-pending, and backend-unavailable paths.
- P2: several IO fixture fragments remain large enough to hide unrelated coverage. Split them by utility domain or operation family when adding new tests.

Performance guardrails for these issues:

- Keep header-only/template visibility for hot submit, wait, completion, and task-resume paths.
- Do not introduce virtual dispatch, owning polymorphic adapters, `std::function`, or heap allocation solely to make files smaller.
- Do not split executor state layout unless the change preserves cache-line placement and declaration order or is explicitly intended as a cache-layout improvement.
- Prefer operation-family fragments over tiny helper extraction. Tiny helpers that add unpredictable branches or obscure the fast path are worse than a moderately sized inline fragment.

### Second-Pass Structure Snapshot

Historical second-pass snapshot before the runtime core de-fragmenting correction:

- `include/af/async_runtime.hpp`: 226 lines. It is mostly a declaration shell that includes focused public/runtime/executor fragments.
- `include/af/task.hpp`: small umbrella over task declarations, IO wait state, optional registry links, and `BasicTask`.
- `include/af/io_types.hpp`: small umbrella over IO base types, provided buffers, status, and fd ownership.

The remaining code-size pressure is now in second-level fragments and fixtures. The public file/socket helper entry points have been reduced to small umbrellas over normally named operation-family headers, and the `io_adapters` public adapter path now uses normally named internal headers. The largest remaining runtime-facing headers in the latest scan are:

- `include/af/io_filesystem.hpp`: now an overview-sized public umbrella. Public filesystem helpers and the underlying runtime submit layer are both split by operation family.

At that point, the right next step was not another large rewrite of `async_runtime.hpp`; it was a second-pass split of the largest fragments into narrower operation-family files while keeping them included inline inside the same class/function scopes.

### Historical Structure Scan: async_runtime.hpp Was A Shell, Detail Fragments Carried The Weight

At that point, `include/af/async_runtime.hpp` was 226 lines and mostly wired public API fragments, executor fragments, and runtime state fragments together. `include/af/task.hpp` and `include/af/io_types.hpp` were also overview-sized umbrellas. The remaining modularity risk was concentrated in several focused-but-still-dense fragments:

- Public IO adapter and socket/file lifecycle fragments are still around 250-300 lines in a few places. They are acceptable for inline APIs, but future additions should go into narrower recv/send/fixed/lifecycle fragments.

Recommendation:
- Do not move hot runtime code into `.cpp` files. Keep template and syscall-submit code inlineable through include fragments.
- Keep `runtime_executor_core_state_fragment.hpp` as the single state-layout owner unless the split can preserve declaration order and cache-line placement exactly.

### Recommended Second-Pass Split Order

P1, executor submit internals:

- The largest socket/file submit umbrellas, io_uring completion/setup paths, and kqueue timeout internals have been split by operation family. The next executor-internal split should be tied to new backend functionality rather than file size alone.

P2, tests/examples/benchmarks:

- Split large file IO support fixtures by boundary tests, lifecycle state machines, fixed-resource state machines, and filesystem operation state machines.
- Keep runtime tests grouped by backend and operation family. Avoid rebuilding a single all-in-one IO regression file.
- Keep examples as thin executable entry points plus task/helper headers. New examples should follow the newer TCP echo/socket lifecycle/sendfile structure rather than putting full state machines in `main`.

Performance constraints for these splits:

- Keep all hot helpers inline/template-visible. Use umbrella fragments, not `.cpp` moves.
- Do not introduce owning polymorphic base classes, `std::function`, heap allocation, or additional cross-thread queues just to make files smaller.
- Preserve existing executor state layout and cache-line alignment. Splitting state declaration should be avoided unless there is a concrete cache-layout improvement.
- Preserve branch shape in hot helpers; refactor by operation family, not by extracting tiny helper functions that add extra unpredictable branches.

Additional validation after the executor lifecycle/notify split:

- `runtime_executor_control_fragment.hpp` is now a 6-line umbrella:
  - `runtime_executor_lifecycle_fragment.hpp`: 33 lines.
  - `runtime_executor_notify_fragment.hpp`: 27 lines.
- The split is structural: no executor fields moved, no queue route changed, no locks or heap allocations were added, and `notify()` keeps the existing worker atomic-wait path separate from the IO-thread native-wake path.
- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug targeted runtime/backpressure/shutdown/IO/stress tests: 21/21 passed.
- Remote clang TSAN targeted runtime/backpressure/shutdown/IO/stress tests: 21/21 passed with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 138 total, 135 passed, 3 skipped, 0 failed.
- Remote clang Release runtime benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_RuntimeExternalStart/8192` mean: 7.70 ms real, 1.064 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 12.6 ms real, 655.313 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.19 ms real, 1.957 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.525 ms real, 246.320 k/s.

Additional validation after the public parallel API split:

- `runtime_public_parallel_api_fragment.hpp` is now a 7-line umbrella:
  - `runtime_public_parallel_shard_fragment.hpp`: 44 lines.
  - `runtime_public_parallel_shards_fragment.hpp`: 119 lines.
  - `runtime_public_ordered_start_fragment.hpp`: 23 lines.
- The split is structural: no runtime state fields moved, no queue route changed, no locks or allocations were added, and the existing public templates remain inline inside `AsyncRuntime`.
- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug parallel/ordered/shutdown/stress targeted tests: 32/32 passed.
- Remote clang TSAN parallel/ordered/shutdown/stress targeted tests: 32/32 passed with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 138 total, 135 passed, 3 skipped, 0 failed.
- Remote clang Release runtime benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_RuntimeExternalStart/8192` mean: 7.37 ms real, 1.117 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 13.9 ms real, 592.463 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.27 ms real, 1.918 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.476 ms real, 268.995 k/s.
  - `BM_RuntimeParallelShards/512` mean: 1.82 ms real, 280.922 k/s.

Additional validation after the socket accept/connect submit split:

- Public submit wrappers:
  - `runtime_public_io_socket_accept_connect_submit_fragment.hpp`: 6 lines.
  - `runtime_public_io_socket_accept_submit_fragment.hpp`: 148 lines.
  - `runtime_public_io_socket_connect_submit_fragment.hpp`: 46 lines.
- Executor submit wrappers:
  - `runtime_executor_io_uring_socket_accept_connect_submit_fragment.hpp`: 6 lines.
  - `runtime_executor_io_uring_socket_accept_submit_fragment.hpp`: 139 lines.
  - `runtime_executor_io_uring_socket_connect_submit_fragment.hpp`: 41 lines.
- The split is structural: no task state transitions, queue routing, SQE arguments, atomics, locks, allocations, or fallback semantics changed.
- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug accept/connect targeted tests: 13 total, 12 passed, 1 skipped.
- Remote clang TSAN accept/connect targeted tests: 13 total, 12 passed, 1 skipped, with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 138 total, 135 passed, 3 skipped, 0 failed.

Additional validation after the fixed-file resource split:

- `runtime_executor_io_uring_file_resource_fragment.hpp` is now a 7-line umbrella:
  - `runtime_executor_io_uring_file_register_fragment.hpp`: 60 lines.
  - `runtime_executor_io_uring_file_unregister_fragment.hpp`: 67 lines.
  - `runtime_executor_io_uring_file_update_fragment.hpp`: 85 lines.
- The split is structural: no fixed-file table state fields moved, no `io_uring_register` opcode/argument changed, no busy/flush ordering changed, and no locks or allocations were added.
- Local `git diff --check`: passed.
- Remote clang image `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.2` Debug fixed-file/resource targeted tests: 6 total, 4 passed, 2 skipped.
- Remote clang TSAN fixed-file/resource targeted tests: 6 total, 4 passed, 2 skipped, with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 138 total, 135 passed, 3 skipped, 0 failed.

Additional validation after the ready enqueue route split:

- `runtime_ready_enqueue_fragment.hpp` is now an 8-line umbrella:
  - `runtime_ready_route_fragment.hpp`: 14 lines.
  - `runtime_ready_try_enqueue_fragment.hpp`: 54 lines.
  - `runtime_ready_blocking_enqueue_fragment.hpp`: 57 lines.
  - `runtime_ready_post_fragment.hpp`: 48 lines.
- The split is structural: same-thread runtime posts still route to the executor local queue, cross-thread runtime posts still route to the per-source SPSC queue, external posts still route to the per-target MPSC queue, and no locks, atomics, queue storage, or wake ordering were changed.
- Local `git diff --check`: passed.
- Remote clang Debug scheduler/runtime targeted tests: 46/46 passed.
- Remote clang TSAN scheduler/runtime targeted tests: 46/46 passed, with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 138 total, 135 passed, 3 skipped, 0 failed.
- Remote clang Release runtime benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_RuntimeExternalStart/8192` mean: 7.00 ms real, 1.179 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 13.6 ms real, 603.070 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.28 ms real, 1.917 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.505 ms real, 253.635 k/s.
  - `BM_RuntimeParallelShards/512` mean: 1.83 ms real, 280.185 k/s.

Additional validation after the `BasicTask` schedule split:

Status: superseded by the BasicTask de-fragmenting pass. These files were removed when the class-body splice style was rejected.

- `basic_task_schedule_fragment.hpp` is now an 8-line umbrella:
  - `basic_task_schedule_constants_fragment.hpp`: 8 lines.
  - `basic_task_schedule_state_fragment.hpp`: 52 lines.
  - `basic_task_running_wake_fragment.hpp`: 92 lines.
  - `basic_task_requested_thread_fragment.hpp`: 28 lines.
- The split is structural: no `TaskState` transition, run-epoch handling, requested-thread atomic operation, memory ordering, queue routing, wake ordering, lock, or allocation behavior changed.
- Local `git diff --check`: passed.
- Remote clang Debug task/scheduler targeted tests: 45/45 passed.
- Remote clang TSAN task/scheduler/config targeted tests: 46/46 passed, with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 138 total, 135 passed, 3 skipped, 0 failed.
- Remote clang Release runtime benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_RuntimeExternalStart/8192` mean: 6.94 ms real, 1.193 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 13.0 ms real, 634.102 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.25 ms real, 1.930 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.503 ms real, 256.183 k/s.
  - `BM_RuntimeParallelShards/512` mean: 1.98 ms real, 260.199 k/s.

Additional validation after the Running wake terminal-state fix:

- `finish_done()` and `finish_again()` now publish `Done`/`Queued` before consuming the same-epoch requested-thread slot. This removes false debug assertions when a Running wake request is made but the owner does not return `Pending`.
- `resolve_running_wake_request()` treats a same-epoch request that resolves after `Done` as deferred/no-op, because the request was observed while the task was Running and is only meaningful for a later `Pending` transition.
- The fix does not add locks, allocations, queue hops, virtual dispatch, or changes to local/SPSC/MPSC topology, ready-source hints, wake notification, or cache-line-aligned executor state.
- New stress coverage:
  - `RuntimeStressTests.RunningWakeBeforeDoneIsBenign`.
  - `RuntimeStressTests.RunningWakeBeforeAgainIsBenign`.
- Local `git diff --check`: passed.
- Remote clang Debug scheduler targeted tests: 8/8 passed.
- Remote clang TSAN scheduler targeted tests: 8/8 passed, with no ThreadSanitizer report.
- Remote clang TSAN full runtime test suite: 140 total, 137 passed, 3 skipped, with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 140 total, 137 passed, 3 skipped, 0 failed.
- Remote clang Release runtime benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_RuntimeExternalStart/8192` mean: 6.57 ms real, 1.251 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 13.6 ms real, 603.135 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.22 ms real, 1.945 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.486 ms real, 263.680 k/s.
  - `BM_RuntimeParallelShards/512` mean: 1.83 ms real, 280.443 k/s.

Additional validation after the generic io_uring SQE fill split:

- `runtime_executor_io_uring_generic_submit_sqe_fragment.hpp` is now an 8-line umbrella:
  - `runtime_executor_io_uring_generic_submit_sqe_dispatch_fragment.hpp`: 67 lines.
  - `runtime_executor_io_uring_generic_submit_sqe_filesystem_fragment.hpp`: 56 lines.
  - `runtime_executor_io_uring_generic_submit_sqe_socket_fragment.hpp`: 49 lines.
  - `runtime_executor_io_uring_generic_submit_sqe_buffer_fragment.hpp`: 22 lines.
- The split is structural: no opcode classification, SQE field value, operation lifetime, pending-submit flush ordering, memory ordering, lock, allocation, or fallback behavior changed.
- Local `git diff --check`: passed.
- Remote clang Debug IO/io_uring targeted tests: 83 total, 81 passed, 2 skipped.
- Remote clang TSAN IO/io_uring targeted tests: 83 total, 81 passed, 2 skipped, with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 138 total, 135 passed, 3 skipped, 0 failed.
- Remote clang Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_IoDatagramAdapterZeroByteRecv` mean: 0.653 ns real.
  - `BM_IoFileAdapterZeroByteRead` mean: 0.627 ns real.
  - `BM_IoTimeoutInvalidDelay` mean: 0.836 ns real.
  - `BM_IoStreamAdapterZeroByteSend` mean: 0.627 ns real.
  - `BM_RuntimeExternalStart/8192` mean: 6.95 ms real, 1.201 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 12.0 ms real, 688.852 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.19 ms real, 1.955 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.515 ms real, 248.834 k/s.
  - `BM_RuntimeParallelShards/512` mean: 1.92 ms real, 266.386 k/s.

Additional validation after the provided-buffer resource split:

- `runtime_executor_io_uring_provided_buffer_resource_fragment.hpp` is now a 6-line umbrella:
  - `runtime_executor_io_uring_provided_buffer_register_fragment.hpp`: 75 lines.
  - `runtime_executor_io_uring_provided_buffer_unregister_fragment.hpp`: 74 lines.
- The split is structural: no registration argument, pending-submit flush ordering, busy check, group tracking, memory ordering, lock, allocation, or fallback behavior changed.
- Local `git diff --check`: passed.
- Remote clang Debug provided-buffer targeted tests: 6/6 passed.
- Remote clang TSAN provided-buffer targeted tests: 6/6 passed, with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 138 total, 135 passed, 3 skipped, 0 failed.
- Remote clang Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_IoDatagramAdapterZeroByteRecv` mean: 0.647 ns real.
  - `BM_IoFileAdapterZeroByteRead` mean: 0.628 ns real.
  - `BM_IoStreamAdapterZeroByteSend` mean: 0.627 ns real.
  - `BM_RuntimeExternalStart/8192` mean: 6.70 ms real, 1.224 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 13.5 ms real, 605.570 k/s.
- `BM_RuntimeIoThreadHop/8192` mean: 4.19 ms real, 1.954 M/s.
- `BM_RuntimeParallelShards/128` mean: 0.480 ms real, 267.212 k/s.
- `BM_RuntimeParallelShards/512` mean: 1.83 ms real, 280.493 k/s.

Additional validation after the registered-buffer resource split:

- `runtime_executor_io_uring_buffer_resource_fragment.hpp` is now an 8-line umbrella:
  - `runtime_executor_io_uring_buffer_register_fragment.hpp`: 60 lines.
  - `runtime_executor_io_uring_buffer_unregister_fragment.hpp`: 67 lines.
- The split is structural: no `_WIN32` guard, syscall argument, pending-submit flush ordering, busy check, registered flag/count update, memory ordering, lock, allocation, or fallback behavior changed.
- Local `git diff --check`: passed.
- Remote clang Debug fixed-buffer/resource targeted tests: 4/4 passed.
- Remote clang TSAN fixed-buffer/resource plus scheduler-boundary targeted tests: 9/9 passed, with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 138 total, 135 passed, 3 skipped, 0 failed.
- Remote clang Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_IoDatagramAdapterZeroByteRecv` mean: 0.641 ns real.
  - `BM_IoFileAdapterZeroByteRead` mean: 0.629 ns real.
  - `BM_IoTimeoutInvalidDelay` mean: 0.837 ns real.
  - `BM_IoStreamAdapterZeroByteSend` mean: 0.627 ns real.
  - `BM_IoStreamAdapterZeroByteSendZc` mean: 0.630 ns real.
  - `BM_IoFileAdapterZeroByteReadFixedAt` mean: 0.629 ns real.
  - `BM_RuntimeExternalStart/8192` mean: 7.07 ms real, 1.168 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 12.7 ms real, 649.697 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.29 ms real, 1.908 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.496 ms real, 259.553 k/s.
  - `BM_RuntimeParallelShards/512` mean: 1.82 ms real, 283.492 k/s.

Additional validation after the io_uring support split:

- `io_uring_support.hpp` is now a 23-line Linux-only umbrella with normal module names:
  - `io_uring_support_abi.hpp`: 63 lines.
  - `io_uring_support_opcode.hpp`: 17 lines.
  - `io_uring_support_types.hpp`: 61 lines.
  - `io_uring_support_syscall.hpp`: 44 lines.
  - `io_uring_support_sqe.hpp`: 38 lines.
- The split is structural: no fallback macro value, opcode constant, request struct layout, syscall argument, setup flag handling, SQE field value, branch, lock, allocation, or memory ordering changed.
- Local `git diff --check`: passed.
- Remote clang Debug full runtime test suite: 138 total, 135 passed, 3 skipped, 0 failed.
- Remote clang TSAN IO/io_uring/stress targeted tests: 96 total, 94 passed, 2 skipped, with no ThreadSanitizer report.
- Remote clang Release full runtime test suite: 138 total, 135 passed, 3 skipped, 0 failed.
- Remote clang Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_IoDatagramAdapterZeroByteRecv` mean: 0.646 ns real.
  - `BM_IoFileAdapterZeroByteRead` mean: 0.628 ns real.
  - `BM_IoTimeoutInvalidDelay` mean: 0.837 ns real.
  - `BM_IoStreamAdapterZeroByteSend` mean: 0.627 ns real.
  - `BM_RuntimeExternalStart/8192` mean: 6.85 ms real, 1.204 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 12.7 ms real, 650.818 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.21 ms real, 1.946 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.483 ms real, 265.762 k/s.
  - `BM_RuntimeParallelShards/512` mean: 1.98 ms real, 259.583 k/s.

Additional validation after the io_uring completion-cancel ownership fix:

- `cancel_io_completion()` no longer writes `ECANCELED` into `IoOpState` before the io_uring CQE is observed. It submits the cancel SQE, marks `operation->cancel_requested`, and treats a repeated cancel request as accepted without emitting another cancel SQE.
- `completed_uring_status()` and `completed_uring_timeout_status()` now return pending while `completion_token != nullptr` or the wait result is not ready. A task cannot consume the cancel result before the runtime has released CQE ownership.
- Added `UringIoRuntimeSocketStreamFixture.IoUringCompletionCancelIsNotConsumableBeforeCqe`, backed by `UringSelfCancelRecvCompletionTask`, to cover self-cancel followed by an immediate second status check before the cancel CQE.
- The fix adds no locks, heap allocations, queue topology changes, virtual dispatch, or cross-thread route changes.
- Local `git diff --check`: passed.
- Remote clang Debug targeted IO/io_uring tests: 9/9 passed.
- Remote clang TSAN targeted IO/io_uring/scheduler-boundary tests: 12/12 passed with no ThreadSanitizer report.
- Remote clang TSAN full runtime suite: 141 total, 138 passed, 3 skipped, with no ThreadSanitizer report.
- Remote clang Release full runtime suite: 141 total, 138 passed, 3 skipped, 0 failed.
- Remote clang Release benchmark canary, 3 repetitions with `--benchmark_min_time=0.05s`:
  - `BM_IoDatagramAdapterZeroByteRecv` mean: 0.672 ns real.
  - `BM_IoFileAdapterZeroByteRead` mean: 0.670 ns real.
  - `BM_IoTimeoutInvalidDelay` mean: 0.971 ns real.
  - `BM_IoStreamAdapterZeroByteSend` mean: 0.711 ns real.
  - `BM_IoStreamAdapterZeroByteSendZc` mean: 0.649 ns real.
  - `BM_IoFileAdapterZeroByteReadFixedAt` mean: 0.664 ns real.
  - `BM_RuntimeExternalStart/8192` mean: 7.44 ms real, 1.103 M/s.
  - `BM_RuntimeCrossThreadHop/8192` mean: 12.1 ms real, 677.438 k/s.
  - `BM_RuntimeIoThreadHop/8192` mean: 4.18 ms real, 1.961 M/s.
  - `BM_RuntimeParallelShards/128` mean: 0.467 ms real, 274.202 k/s.
  - `BM_RuntimeParallelShards/512` mean: 2.10 ms real, 246.025 k/s.

### Latest Test/Example Scan: Long Files Remain Mostly In Fixtures

The largest remaining files are now test/support fixtures rather than runtime shell code:

- File IO support fragments for boundary, read/write, open/lifecycle, filesystem boundary, and filesystem ops have been split into smaller operation-family task fragments. Portable accept support and io_uring accept/stream support are now split the same way.
- Runtime IO datagram tests now share `runtime_io_udp_socket_helpers_fragment.hpp`; the io_uring socket datagram source is down to 151 lines. Stream transfer tests now share `runtime_io_stream_transfer_helpers_fragment.hpp`; the stream transfer source is down to 219 lines.
- Some examples still use explicit atomics to observe readiness/completion (`io_epoll.cpp`, `io_timer.cpp`, and a few multishot io_uring examples). Those should be converted to task-owned state machines plus `ShutdownPolicy::WaitForTasks` when practical, matching the newer IO adapter/socket lifecycle examples.

Recommendation:
- Continue splitting test support by operation family and boundary category, but keep fixtures close to the tests that use them.
- Prefer example completion through runtime shutdown policy and task result structs, not main-thread atomic polling, unless the example is specifically demonstrating cross-thread observation.

### Post File-Descriptor Adapter Split Scan

After splitting `IoFile`, the largest remaining files are mostly tests/examples. Runtime-facing candidates worth tracking:

- `include/af/io_timeout.hpp`: now a small public umbrella. The deadline arbitration fragment remains intentionally larger than the status/wait fragments because it preserves the race ordering between IO completion, timeout completion, and cancel completion.
- Resolved after this scan: `include/af/detail/io_uring_support.hpp` is now a small umbrella over Linux io_uring ABI fallback definitions, opcode constants, request/types, syscall/setup wrappers, and SQE fill helpers.
- `include/af/detail/basic_task_fragment.hpp`: no longer a class shell over class-body fragments; task lifecycle and scheduling now remain inline in one cohesive class definition.
- Resolved after this scan: `include/af/detail/io_common_detail_state_fragment.hpp` is now a small umbrella over focused IO common helper fragments.
- Resolved after this scan: `include/af/detail/runtime_executor_io_uring_file_resource_fragment.hpp` is now a small umbrella over fixed-file table register, unregister, and update fragments.
- Resolved after this scan: `include/af/detail/io_adapters_datagram_fragment.hpp` and `include/af/detail/io_adapters_stream_fragment.hpp` no longer use class-body method fragments. Each adapter is kept as one cohesive thin class definition.
- Resolved after this scan: `include/af/detail/io_adapters_file_descriptor_fragment.hpp` and `include/af/detail/io_adapters_fixed_file_fragment.hpp` no longer use class-body method fragments. `IoFile` and `IoFixedFile` keep read/write/fixed/sync forwarding methods in declaration order.

Recommended next order:

- Avoid further timeout splitting unless the change adds focused tests around IO-first, timeout-first, user-cancel, and backend-unavailable paths.
- Leave executor state and task lifecycle fragments intact unless a concrete cache-layout or correctness improvement justifies the change.

### P1: Executor Internals Are Now Mostly Operation-Family Fragments

The largest remaining runtime-internal fragments are no longer monolithic executor shells. Most remaining density is in selected public helper wrappers and test/example support.

Risk:
- Public helper wrappers and fixture support can still grow dense local regions if new operation families are appended to existing files.
- Concurrency/lifetime audits still require reading several dense io_uring fragments.

Recommended split:
- Consider follow-up splits of io_uring setup tuning helpers or kqueue event/user helpers only when more backend knobs are added.
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

The local macOS Release benchmark binary and the remote Linux Docker Release benchmark both completed fixed-iteration smoke runs, but CI-style time-mode runs exceeded a 180s timeout in the current validation environment before producing benchmark rows. Google Benchmark 1.9.5 also expects `--benchmark_min_warmup_time` as a plain double while `--benchmark_min_time` still uses the normal duration/iteration suffix form.

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
