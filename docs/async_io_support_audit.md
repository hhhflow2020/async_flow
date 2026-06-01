# Async IO Support Audit

Date: 2026-06-02

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

It is not yet "complete" in the absolute sense. Windows has no IOCP backend,
kqueue is narrower and needs regular macOS/BSD CI coverage, io_uring runtime
coverage depends on host/container capabilities, and native readiness currently
allows only one active wait registration per fd.

## Backend Model

- `ThreadKind::Worker`: no IO backend is exposed.
- `ThreadKind::Io` / `ThreadKind::Epoll` on Linux: epoll readiness backend plus
  eventfd wakeup.
- `ThreadKind::IoUring` on Linux: still owns the epoll readiness/wakeup backend,
  then layers io_uring completion operations on top. If the ring is unavailable,
  readiness-based helpers can still run through epoll fallback.
- `ThreadKind::Io` / `ThreadKind::Kqueue` on macOS/BSD: kqueue readiness and
  timeout support.
- Other platforms: public IO helpers report unavailable/`ENOSYS` rather than
  silently pretending to support async IO.

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

- One native readiness wait per fd is currently enforced by `io_waits_` being
  keyed only by fd. That is simple and safe for the current adapters, but it does
  not support independent concurrent read and write waiters on the same fd. The
  framework should either document this as an exclusive wait contract or move to
  fd/filter-keyed registrations.
- `prefer_rearm` is still present on the public wait path, but the active epoll
  implementation ignores it after the deferred-delete race fix. This should be
  removed or redefined before users treat it as a performance guarantee.
- kqueue code is less isolated than the Linux epoll implementation and is not
  continuously exercised on this Linux validation path. It needs macOS/BSD CI,
  especially for timeout cancel/complete races and combined read/write waits.
- io_uring-heavy tests can be skipped when the host/container blocks
  `io_uring_setup`. A dedicated real-io_uring CI lane is required before calling
  the ring backend fully validated.
- There is no Windows IOCP backend. The current behavior is explicit
  unavailability, not cross-platform async IO completeness.

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
- Native readiness waits use `absl::flat_hash_map<int, IoWaitRegistration *>`.
  It is owner-thread-only and usually fine, but fd/filter-keyed waits or very
  high fd counts should be benchmarked before changing the map shape.
- A live epoll readiness-loop benchmark is still missing. Existing IO adapter
  microbenchmarks cover helper fast paths, not end-to-end readiness rearm rates.

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
- kqueue availability/readiness behavior on supported platforms.
- shutdown behavior for pending IO waits.

## Validation For This Pass

- Local `git diff --check`: passed.
- Remote GCC Release build on
  `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.0`:
  `asyncflow_runtime_tests` built successfully.
- Remote GCC Release targeted IO suite:
  `ctest -R "Io|Uring|Epoll|Kqueue|RuntimeIo"` passed 89/89 selected tests.
  Three capability/platform cases were skipped by test logic: kqueue on Linux,
  io_uring accept-direct fixed-file, and io_uring openat-direct fixed-file.
- Remote GCC Release full runtime suite: 161/161 passed, with the same three
  capability/platform skips.

## Next Actions

- Add or enable a CI lane where `io_uring_setup` is allowed, preferably with
  Release, Debug, and TSAN coverage for the ring-specific tests.
- Decide and document the native readiness contract for same-fd concurrent
  waits. If concurrent read/write waits are a target, change registration keys
  from fd-only to fd/filter and add tests.
- Split the kqueue backend out of `runtime_executor.hpp` with the same
  class-out-of-line template style used by the epoll backend.
- Add a live readiness-loop benchmark for epoll and, on macOS/BSD, kqueue.
- Remove or re-specify `prefer_rearm` so public API semantics match the active
  backend implementation.
