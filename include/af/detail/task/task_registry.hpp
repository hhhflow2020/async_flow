#pragma once

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

struct RequestedSchedule {
    std::uint16_t thread_index{0};
    ScheduleMode mode{ScheduleMode::Auto};
};

inline constexpr std::uint64_t no_requested_thread = 0;

template <typename RuntimeT>
inline constexpr bool task_registry_enabled_v = [] {
    constexpr bool explicitly_enabled = [] {
        if constexpr (requires { RuntimeT::Traits::enable_task_registry; }) {
            return static_cast<bool>(RuntimeT::Traits::enable_task_registry);
        } else {
            return false;
        }
    }();
    if constexpr (requires { RuntimeT::Traits::shutdown_policy; }) {
        return explicitly_enabled ||
               RuntimeT::Traits::shutdown_policy == ShutdownPolicy::StopImmediately;
    } else {
        return explicitly_enabled;
    }
}();

template <typename TaskT> struct TaskRegistryLinks {
    TaskT *prev{nullptr};
    TaskT *next{nullptr};
    bool registered{false};
};

struct EmptyTaskRegistryLinks {};

} // namespace detail
