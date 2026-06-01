#if !defined(AF_TASK_DETAIL_INCLUDE)
#error "task_registry.hpp is internal to af/task.hpp"
#endif

namespace detail {

enum class ScheduleAction : std::uint8_t {
  Enqueue,
  Deferred,
  Rejected,
};

struct ScheduleRequest {
  ScheduleAction action{ScheduleAction::Rejected};
  TaskState previous{TaskState::Done};
};

inline constexpr std::uint64_t no_requested_thread = 0;

template <typename RuntimeT>
inline constexpr bool task_registry_enabled_v = [] {
  if constexpr (requires { RuntimeT::Traits::enable_task_registry; }) {
    return static_cast<bool>(RuntimeT::Traits::enable_task_registry);
  } else {
    return false;
  }
}();

template <typename TaskT> struct TaskRegistryLinks {
  TaskT *prev{nullptr};
  TaskT *next{nullptr};
  bool registered{false};
};

struct EmptyTaskRegistryLinks {};

} // namespace detail
