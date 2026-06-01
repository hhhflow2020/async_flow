#if !defined(AF_TASK_DETAIL_INCLUDE)
#error "task_types.hpp is internal to af/task.hpp"
#endif

template <typename TraitsT> class AsyncRuntime;

namespace detail {
template <typename RuntimeT, typename TraitsT> class Executor;
} // namespace detail

enum class TaskResult : std::uint8_t {
  Done,
  Pending,
  Again,
  Failed,
  Cancelled,
};

enum class QueueFullPolicy : std::uint8_t {
  Reject,
  Yield,
};

enum class ShutdownPolicy : std::uint8_t {
  WaitForTasks,
  StopImmediately,
};

enum class ThreadKind : std::uint8_t {
  Worker,
  Io,
  IoUring,
  Epoll,
  Kqueue,
};

enum class TaskState : std::uint8_t {
  Created,
  Queued,
  Starting,
  Running,
  Pending,
  Done,
};
