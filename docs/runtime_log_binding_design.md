# Runtime-bound logging design notes

## Current state

- `AsyncLogger` still supports a dedicated consumer thread for the non-runtime
  `start_async_logging()` entry point. The runtime-aware
  `start_async_logging_for_runtime<RuntimeT>()` entry point now binds the
  consumer drain loop to a configured `RuntimeT::Thread` instead of creating an
  extra logging thread.
- Runtime threads use per-thread SPSC lanes, and external threads use sharded
  MPSC queues.
- `FileLogBackend`, `UdpLogBackend`, and `TcpLogBackend` execute synchronously on
  the consumer thread.
- `RuntimeFileLogBackend`, `RuntimeUdpLogBackend`, and `RuntimeTcpLogBackend`
  keep the consumer thread for batching, but bind the actual backend IO task to a
  configured runtime thread.
- The three runtime backends now share a `RuntimeLogQueueState` drain component
  for batch pooling, ready/free SPSC queues, pending counters, wake flags, and
  bounded flush waits. File, UDP, and TCP keep only their backend-specific IO
  strategy state.
- Runtime backend task binding is now shared through `RuntimeLogTaskBinding`.
  File, UDP, and TCP no longer each own duplicate task-handle, first-start,
  wake, and shutdown-wait logic. They keep backend-specific IO state machines,
  while framework-thread binding stays centralized in the common runtime log
  component.
- Runtime backend batch objects are now allocated in contiguous storage owned by
  `RuntimeLogQueueState`. Ready/free queues still pass stable batch pointers, but
  the batch headers themselves no longer require one heap allocation and pointer
  chase per slot.
- Runtime-bound log consumers are lazy-started: registering the Abseil sink does
  not create a permanent pending runtime task. The consumer task is scheduled
  only when the first record transitions the pending count from zero to non-zero.
- File, UDP, and TCP all skip ordinary producer wakeups while their bound task is
  waiting on runtime IO readiness, so producer notifications do not masquerade as
  IO completions.
- Runtime-thread log lanes now use an SPSC record pool matching their SPSC
  submission queue. External producer shards keep the lock-free shared free-list
  because they still admit multiple producer threads.
- Log record pool ownership is now split into `async_log_record_pool.hpp`.
  `AsyncLogger` wires queues, counters, and consumer draining, while the shared
  MPSC record pool and runtime SPSC record pool live in a focused internal
  module.
- Producer lane ownership is now split into `async_log_lanes.hpp`. External
  producer shards, runtime SPSC lanes, and their cache-line-isolated counters are
  grouped with the queue and record-pool topology they protect.

This keeps runtime log producers on SPSC submission, external producers on MPSC
admission, and network backends without private IO threads. Runtime deployments
can now keep the log consumer inside the framework thread layout; non-runtime
deployments can still use the compatibility dedicated thread.

## Recommended direction

Keep two explicit consumer placements:

- Dedicated consumer thread: the current default, best for applications that log
  before runtime startup, after runtime shutdown, or from many external threads.
- Runtime-bound consumer task: an opt-in mode where the drain loop runs on a
  configured `RuntimeT::Thread`.

The runtime-bound mode is a consumer placement choice, not a property of each
backend. File, UDP, and TCP backends stay batch IO strategies that can be driven
by the same runtime-bound drain task.

## Backend binding model

- File backend: open the file on the bound IO thread and write batches through
  `io_writev_some()` or io_uring `writev` when available. Keep append/truncate
  policy in the backend config, but keep queueing and wakeup in the shared drain
  task.
- UDP backend: keep pre-batched datagrams and use `sendmmsg` on Linux when
  ready; if the socket is not writable, wait on the bound runtime IO thread.
- TCP backend: connect and send on the bound runtime IO thread, with reconnect
  policy owned by the TCP strategy and queue/flush/shutdown owned by the shared
  drain task.

## Correctness constraints

- Runtime-thread log submission must stay explicit: runtime producers use their
  own SPSC lane, external producers use MPSC shards.
- Flush must wait for both the producer-to-consumer queues and backend IO
  completion counters.
- Shutdown must stop admission before draining, then wake the bound consumer
  task, then shut down backends after all pending records are released.
- Runtime-bound consumer tasks must not call backend flush/shutdown from the
  bound runtime thread, because file/TCP/UDP runtime backends may need that same
  thread to complete their own IO tasks. Backend shutdown is driven by the
  external `AsyncLogHandle::stop()` path after the consumer task has drained and
  finished.
- A backend waiting for IO readiness must not be woken by ordinary producer
  notifications as if the IO had completed. This is now enforced consistently
  for file, UDP, and TCP runtime-bound backends.

## Performance constraints

- Producer hot paths should not take locks.
- Runtime-thread producers should remain SPSC and avoid cross-thread MPMC hops.
- Runtime-thread producer record allocation should also stay SPSC, avoiding the
  shared free-list CAS pair used by external producer shards.
- Runtime-bound consumers should wake only on empty-to-nonempty transitions so
  ordinary logging does not schedule a framework task for every record.
- Runtime-bound consumers should cap drain batches per run so one logging burst
  does not monopolize the bound runtime thread.
- Record pool changes should stay isolated from the consumer drain loop so the
  shared MPSC pool and runtime SPSC pool can be tuned independently.
- Lane topology changes should stay isolated from consumer lifecycle code so
  cache layout and false-sharing tuning can be done without touching backend
  flush/shutdown behavior.
- Backend batches should be preallocated and recycled through SPSC free/ready
  queues or a shared runtime drain pool.
- Runtime backend batch pools should keep batch headers contiguous so the
  consumer thread and bound IO task reduce avoidable cache misses while rotating
  through ready/free batches.
- Counters and queue cursors that are touched by different threads should stay
  cache-line isolated.
- Drop policy should remain configurable so bounded queues do not force producer
  threads to block under overload.

## Migration plan

1. Keep the dedicated consumer thread for the non-runtime compatibility mode.
2. Bind `start_async_logging_for_runtime<RuntimeT>()` to a runtime consumer task
   by default, with an overload for explicitly selecting the consumer thread.
3. Add runtime-bound TCP logging so network logs can use framework IO threads.
4. Add runtime-bound file logging so file writes and flushes can use framework IO
   threads.
5. Factor the duplicate file/UDP/TCP runtime backend queue, wake, flush, and
   shutdown state into a common runtime log drain component. The queue, batch
   pool, pending, wake state, task binding, and shutdown wait are now shared;
   backend-specific flush semantics and IO state machines remain local to each
   backend.
6. Continue converging file/UDP/TCP backend strategies toward the shared
   runtime-bound drain component so backend IO and log consumer placement remain
   independently configurable.
