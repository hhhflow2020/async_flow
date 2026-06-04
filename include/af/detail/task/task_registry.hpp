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

template <typename TraitsT, typename = void>
struct TaskRegistryTraitsHasEnableTaskRegistry : std::false_type {};
template <typename TraitsT>
struct TaskRegistryTraitsHasEnableTaskRegistry<TraitsT,
                                               std::void_t<decltype(TraitsT::enable_task_registry)>>
    : std::true_type {};

template <typename TraitsT, typename = void>
struct TaskRegistryTraitsHasShutdownPolicy : std::false_type {};
template <typename TraitsT>
struct TaskRegistryTraitsHasShutdownPolicy<TraitsT, std::void_t<decltype(TraitsT::shutdown_policy)>>
    : std::true_type {};

template <typename RuntimeT>
inline constexpr bool task_registry_enabled_v = [] {
    using Traits = typename RuntimeT::Traits;
    constexpr bool explicitly_enabled = [] {
        if constexpr (TaskRegistryTraitsHasEnableTaskRegistry<Traits>::value) {
            return static_cast<bool>(Traits::enable_task_registry);
        } else {
            return false;
        }
    }();
    if constexpr (TaskRegistryTraitsHasShutdownPolicy<Traits>::value) {
        return explicitly_enabled || Traits::shutdown_policy == ShutdownPolicy::StopImmediately;
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
