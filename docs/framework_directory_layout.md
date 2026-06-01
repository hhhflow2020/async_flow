# Framework Directory Layout

Date: 2026-06-02

## Intent

The framework implementation headers are now grouped by responsibility instead
of keeping every internal header directly under `include/af/detail`.

Public headers remain in `include/af/*.hpp`. The directory split is for internal
implementation ownership and auditability; users should continue to include the
public umbrella headers such as `af/async_flow.hpp`, `af/io.hpp`, and
`af/async_runtime.hpp`.

## Current Layout

- `include/af/detail/config.hpp`: shared compile-time platform/config helpers.
- `include/af/detail/queue/`: bounded SPSC/MPSC/MPMC queue families.
- `include/af/detail/memory/`: object-pool implementation.
- `include/af/detail/task/`: task state, registry hooks, and `BasicTask`.
- `include/af/detail/runtime/`: fixed-thread runtime, executor, scheduling,
  lifecycle, IO handoff, and public-runtime CRTP implementation pieces.
- `include/af/detail/io/common/`: IO status, wait state, target-thread helpers,
  iovec validation, eventfd/timerfd utilities, and deadline helpers.
- `include/af/detail/io/types/`: public IO result/status/fd/provided-buffer
  support types.
- `include/af/detail/io/adapters/`: thin file/stream/listener/datagram/event
  adapter classes.
- `include/af/detail/io/socket/`: socket lifecycle, accept/connect, recv/send,
  zero-copy, vectored, sendfile, splice, and shutdown helpers.
- `include/af/detail/io/file/`: file read/write, fixed-buffer/fixed-file,
  open/close/fsync, metadata, and namespace helpers.
- `include/af/detail/io/filesystem/`: filesystem open, namespace, allocation,
  and directory helpers.
- `include/af/detail/io/datagram/`: datagram recv/send/vectored/zero-copy
  helpers.
- `include/af/detail/io/timeout/`: timeout status, wait submission, and deadline
  arbitration helpers.
- `include/af/detail/io/uring/`: Linux io_uring ABI fallback, opcodes, setup,
  syscall wrappers, and SQE fill helpers.

`include/af/detail` went from 107 root-level files to one root file plus module
subdirectories. The 106 implementation headers now live under focused
responsibility directories.

## Rules For Future Splits

- Keep public include paths stable unless a public API migration is explicitly
  planned.
- Keep implementation files close to the subsystem that owns their invariants:
  runtime scheduler code under `runtime/`, queue algorithms under `queue/`,
  socket helpers under `io/socket/`, and so on.
- Do not split one cohesive class by access section or field block just to lower
  a line count. Split by real operation family, data structure, platform
  backend, or independently auditable helper.
- Avoid compatibility shim headers in `detail/` unless an external downstream
  compatibility need is identified. This keeps the root directory from becoming
  another flat pile of forwarding files.
- Continue to keep hot template paths header-only and inline-visible; directory
  structure should improve ownership without adding virtual dispatch, hidden
  allocation, or extra queues.

## Validation

- `clang-format` was run after include-path updates.
- Local `git diff --check`: passed.
- Remote GCC Release default build on
  `ghcr.io/hhhflow2020/cpp-dev-gcc:bookworm-v2.0.0`: passed for runtime tests
  and benchmarks.
- Remote GCC Release full runtime suite: 161/161 passed; three
  platform/capability tests were skipped by test logic.
- Local macOS Debug kqueue suite on Darwin 25.5.0 arm64:
  `ctest --test-dir build-local/build/Debug -R Kqueue --output-on-failure`
  passed 5/5.
