# Runtime-bound logging design notes

## Current state

- `AsyncLogger` owns one dedicated consumer thread. Runtime threads use per-thread
  SPSC lanes, and external threads use sharded MPSC queues.
- `FileLogBackend`, `UdpLogBackend`, and `TcpLogBackend` execute synchronously on
  the consumer thread.
- `RuntimeFileLogBackend`, `RuntimeUdpLogBackend`, and `RuntimeTcpLogBackend`
  keep the consumer thread for batching, but bind the actual backend IO task to a
  configured runtime thread.
- The three runtime backends now share a `RuntimeLogQueueState` drain component
  for batch pooling, ready/free SPSC queues, pending counters, wake flags, and
  bounded flush waits. File, UDP, and TCP keep only their backend-specific IO
  strategy state.

This is a useful intermediate shape: runtime log producers still get SPSC
submission, external producers still have MPSC admission, and network backends do
not create their own IO threads. It does not yet make the whole log consumer a
runtime task.

## Recommended direction

Support two explicit consumer placements:

- Dedicated consumer thread: the current default, best for applications that log
  before runtime startup, after runtime shutdown, or from many external threads.
- Runtime-bound consumer task: an opt-in mode where the drain loop runs on a
  configured `RuntimeT::Thread`.

The runtime-bound mode should be a consumer placement choice, not a property of
each backend. File, UDP, and TCP backends should become batch IO strategies that
can be driven by the same runtime-bound drain task.

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
- A backend waiting for IO readiness must not be woken by ordinary producer
  notifications as if the IO had completed.

## Performance constraints

- Producer hot paths should not take locks.
- Runtime-thread producers should remain SPSC and avoid cross-thread MPMC hops.
- Backend batches should be preallocated and recycled through SPSC free/ready
  queues or a shared runtime drain pool.
- Counters and queue cursors that are touched by different threads should stay
  cache-line isolated.
- Drop policy should remain configurable so bounded queues do not force producer
  threads to block under overload.

## Migration plan

1. Keep the dedicated consumer thread as the default compatibility mode.
2. Add runtime-bound TCP logging so network logs can use framework IO threads.
3. Add runtime-bound file logging so file writes and flushes can use framework IO
   threads.
4. Factor the duplicate file/UDP/TCP runtime backend queue, wake, flush, and
   shutdown state into a common runtime log drain component. The queue, batch
   pool, pending, and wake state are now shared; full shutdown/task placement
   sharing is still a follow-up.
5. Add an opt-in runtime consumer placement API once file/UDP/TCP can all be
   driven by the common drain component.
