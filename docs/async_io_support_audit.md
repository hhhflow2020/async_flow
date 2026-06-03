# Async IO Support Audit

Date: 2026-06-03

## Scope

This note reviews the current async IO support in the `af` runtime: backend
coverage, public API surface, correctness properties, race/deadlock risks,
performance shape, and remaining gaps.

## Verdict

The framework has a broad and coherent async IO layer, especially on Linux:
native readiness through epoll, optional completion IO through io_uring, thin
file/socket/datagram/event/timer adapters, cancellation helpers, deadlines,
zero-copy send helpers, fixed buffers/files, and multishot receive/accept
coverage.

It is not yet "complete" in the absolute sense. The supported platform matrix
is POSIX-only; kqueue is narrower and needs regular macOS/BSD CI coverage,
io_uring runtime coverage depends on host/container capabilities, and native
readiness currently allows only one active wait registration per fd.

## Backend Model

- `ThreadKind::Worker`: no IO backend is exposed.
- `ThreadKind::Log`: no IO backend is exposed; it is intended for dedicated
  logging/background consumers that must not compete with ordinary worker
  queues.
- `ThreadKind::Io` / `ThreadKind::Epoll` on Linux: epoll readiness backend plus
  eventfd wakeup.
- `ThreadKind::IoUring` on Linux: still owns the epoll readiness/wakeup backend,
  then layers io_uring completion operations on top. If the ring is unavailable,
  readiness-based helpers can still run through epoll fallback.
- `ThreadKind::Io` / `ThreadKind::Kqueue` on macOS/BSD: kqueue readiness and
  timeout support.
- Unsupported platforms are out of scope. POSIX helpers still report
  unavailable/`ENOSYS` for backend-specific capabilities that are absent on the
  current OS.

The design intentionally requires IO submit/cancel/resource-registration calls
to run on the owning IO thread. That keeps backend state single-thread-owned and
avoids locks in the hot IO path.

## Supported API Families

- Readiness wait/cancel: `io_wait`, `cancel_io`, deadline/timeout arbitration.
- File IO through io_uring on Linux: read/write, vectored IO, current/positioned
  offsets, fsync, open/openat2, close, statx, fallocate, rename/unlink,
  fixed-file descriptors, and registered buffers.
- Socket IO: recv/send, vectored variants, zero-copy send, recv multishot,
  recvmsg multishot, accept/connect, accept direct into fixed files, accept
  multishot, shutdown, socket creation, listener helpers, peer/local name lookup.
- Datagram IO: recv/send/sendto, vectored message paths, connected and
  unconnected UDP helpers, zero-copy send helpers where supported.
- Linux event/timer adapters: eventfd and timerfd readiness integration.
- Thin public adapters: files, fixed files, streams, listeners, datagram sockets,
  events, and timers remain trivially copyable small views.

## Correctness Findings

Fixed in this pass:

- Epoll wakeup now retries `eventfd` writes on `EINTR`. The old path could set
  `io_wake_pending_ = true` while the interrupted write had not actually queued
  an eventfd wake, which is a rare but real lost-wake risk for an IO thread
  sleeping in `epoll_wait`.
- `ThreadKind::IoUring` readiness waits now fall back to epoll if the io_uring
  poll submission path closes/fails the ring during submission. The old path
  returned failure after inserting the wait registration, leaving a stale
  `io_waits_` entry with no epoll interest.
- The kqueue executor backend has been split out of `runtime_executor.hpp` into
  `runtime_executor_kqueue_backend.hpp`, matching the epoll/backend include
  style and reducing the main executor header's mixed responsibilities.
- Native readiness waits now use one owner-thread fd entry with independent
  read and write slots. Duplicate same-direction waits are still rejected, but a
  read waiter and a write waiter can now coexist on the same fd and complete
  independently on epoll and kqueue.
- io_uring poll waits are detached from their fd slot when the ring backend
  fails existing operations, so resumed tasks can safely re-arm through the
  epoll fallback without hitting a stale duplicate-wait registration.

Important existing strengths:

- Native IO backend state is single-thread-owned by the executor, so epoll,
  kqueue, io_uring operation lists, and IO object pools do not need runtime
  mutexes.
- Epoll readiness cleanup deletes the epoll interest before waking user code,
  avoiding fd-number reuse races after the task closes or reuses the fd.
- io_uring completion cancellation no longer publishes `ECANCELED` before the
  kernel completion arrives; pending operations keep ownership until the CQE is
  consumed.
- io_uring resource unregister paths flush pending submissions and reject
  unregister while operations are still active.
- Shutdown coverage includes dropping pending IO waits under
  `ShutdownPolicy::StopImmediately` and restarting the runtime afterward.

Remaining correctness and completeness gaps:

- `prefer_rearm` is still present on the public wait path, but the active epoll
  implementation ignores it after the deferred-delete race fix. This should be
  removed or redefined before users treat it as a performance guarantee.
- kqueue is not continuously exercised on the Linux validation path. It needs
  macOS/BSD CI, especially for timeout cancel/complete races and combined
  read/write waits.
- io_uring-heavy tests can be skipped when the host/container blocks
  `io_uring_setup`. A dedicated real-io_uring CI lane is required before calling
  the ring backend fully validated.
- The framework targets POSIX platforms only; remaining completeness work
  should focus on Linux io_uring/epoll and macOS/BSD kqueue coverage.

## Performance Assessment

The async IO path is generally shaped for high performance:

- IO backend mutation is executor-owned, so no locks are needed around epoll,
  kqueue, io_uring operation tracking, or IO wait pools.
- Wakeups are coalesced with `io_wake_pending_`, avoiding repeated eventfd or
  kqueue user-event writes while a wake is already pending.
- io_uring submissions are batched up to `io_uring_submit_batch_threshold` and
  flushed from the IO-thread run loop.
- IO wait and io_uring operation/message/address objects are object-pooled and
  pre-reserved from runtime traits (`io_wait_reserve`, `io_uring_entries`) to
  move allocation out of the first steady-state burst.
- Public adapters remain tiny trivially-copyable views, so passing them through
  tasks does not add hidden allocation or shared ownership traffic.

Remaining performance headroom:

- `Executor` still mixes hot scheduler fields and cold backend/resource fields
  in one large object. The current cache-line atomics protect the worst shared
  state, but a future hot/cold state split would improve auditability and
  instruction-cache locality.
- Epoll/kqueue poll batches are fixed at 64 events. This avoids heap allocation
  and keeps stack usage bounded, but high fan-in workloads may benefit from a
  runtime trait or benchmark-validated larger batch.
- io_uring submit batching defaults to `io_uring_entries / 4` and flushes after
  the run loop drains ready tasks. This is throughput-friendly, but low-volume
  submissions can wait behind a constantly non-empty task queue until the batch
  threshold is reached. A latency-oriented trait or periodic flush heuristic
  should be benchmarked before changing the default.
- Native readiness waits use `absl::flat_hash_map<int, IoWaitEntry>` with two
  pointer slots per fd. It stays owner-thread-only and avoids locks; very high fd
  counts should still be benchmarked against alternate table shapes.
- `BM_LiveEpollReadinessRearm` and `BM_LiveKqueueReadinessRearm` now cover live
  socketpair readiness loops with repeated one-byte `read_some` rearming. They
  complement the IO adapter microbenchmarks, which only cover helper fast paths.

## Test Coverage Map

The runtime test binary includes targeted sources for:

- epoll setup/readiness/cancel/timeout/boundary/socket-lifecycle/event-timer
  behavior.
- stream IO, vectored stream IO, zero-copy send, sendfile/splice transfer, and
  connect/accept flows.
- datagram receive/readiness/send and UDP vectored paths.
- io_uring socket, accept, datagram, stream, multishot stream receive,
  multishot UDP receive, recvmsg multishot, file, fixed-file, batched file, and
  lifecycle/filesystem flows.
- kqueue availability/readiness/cancel/timeout behavior on supported platforms,
  including same-fd read/write wait coexistence.
- shutdown behavior for pending IO waits.

## Validation For This Pass

- Local `git diff --check`: passed.
- Local macOS Debug `asyncflow_runtime_tests` built successfully.
- Local macOS Debug `ctest -R "IoRuntimeKqueue|Kqueue"` passed 7/7 kqueue tests.
- Local macOS Debug
  `ctest -R "^(Runtime|IoRuntime|IoState|BatchUtility)"` passed 87/87 selected
  tests; Linux-only epoll/io_uring cases skipped by platform guards.
- Remote Linux GCC Debug on
  `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.3`:
  `asyncflow_runtime_tests` built and
  `ctest -R "IoRuntimeDatagramFixture|IoRuntimeEpollFixture"` passed 33/33
  selected epoll/datagram tests, including same-fd read/write wait coexistence.
- Remote Linux GCC Debug on
  `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.3`:
  `ctest -R "^(Runtime|IoRuntime|IoState|BatchUtility)"` passed 81/81 selected
  tests; kqueue was skipped by platform guard.
- Remote Linux Clang Debug on
  `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.3`:
  `asyncflow_runtime_tests` built and
  `ctest -R "^(Runtime|IoRuntime|IoState|BatchUtility)"` passed 81/81 selected
  tests; kqueue was skipped by platform guard.
- Remote Linux Clang Debug + TSAN on
  `ghcr.io/hhhflow2020/cpp-dev-clang:bookworm-v2.0.3`:
  `asyncflow_runtime_tests` built with `ASYNCFLOW_ENABLE_TSAN=ON` and
  `ctest -R "^(Runtime|IoRuntime|IoState|BatchUtility)"` passed 72/72 selected
  tests with no ThreadSanitizer report; kqueue was skipped by platform guard.

## Next Actions

- Add or enable a CI lane where `io_uring_setup` is allowed, preferably with
  Release, Debug, and TSAN coverage for the ring-specific tests.
- Benchmark a latency-oriented io_uring submit flush policy against the current
  throughput-oriented batch threshold.
- Remove or re-specify `prefer_rearm` so public API semantics match the active
  backend implementation.
