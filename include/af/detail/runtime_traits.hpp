#pragma once

#include <cstddef>
#include <cstdint>

#include "af/task.hpp"

namespace af::detail {

template <typename TraitsT>
struct RuntimeTraitsConfig {
    static constexpr std::size_t spsc_queue_capacity = [] {
        if constexpr (requires { TraitsT::spsc_queue_capacity; }) {
            return static_cast<std::size_t>(TraitsT::spsc_queue_capacity);
        } else {
            return static_cast<std::size_t>(1024);
        }
    }();

    static constexpr std::size_t external_queue_capacity = [] {
        if constexpr (requires { TraitsT::external_queue_capacity; }) {
            return static_cast<std::size_t>(TraitsT::external_queue_capacity);
        } else {
            return spsc_queue_capacity;
        }
    }();

    static constexpr QueueFullPolicy queue_full_policy = [] {
        if constexpr (requires { TraitsT::queue_full_policy; }) {
            return TraitsT::queue_full_policy;
        } else {
            return QueueFullPolicy::Reject;
        }
    }();

    static constexpr ShutdownPolicy shutdown_policy = [] {
        if constexpr (requires { TraitsT::shutdown_policy; }) {
            return TraitsT::shutdown_policy;
        } else {
            return ShutdownPolicy::WaitForTasks;
        }
    }();

    static constexpr bool task_registry_enabled = [] {
        if constexpr (requires { TraitsT::enable_task_registry; }) {
            return static_cast<bool>(TraitsT::enable_task_registry);
        } else {
            return false;
        }
    }();

    static constexpr unsigned io_uring_entries = [] {
        if constexpr (requires { TraitsT::io_uring_entries; }) {
            return static_cast<unsigned>(TraitsT::io_uring_entries);
        } else {
            return 1024U;
        }
    }();

    static constexpr unsigned io_uring_submit_batch_threshold = [] {
        if constexpr (requires { TraitsT::io_uring_submit_batch_threshold; }) {
            return static_cast<unsigned>(TraitsT::io_uring_submit_batch_threshold);
        } else {
            return io_uring_entries >= 4U ? io_uring_entries / 4U : 1U;
        }
    }();

    static constexpr std::size_t io_wait_reserve = [] {
        if constexpr (requires { TraitsT::io_wait_reserve; }) {
            return static_cast<std::size_t>(TraitsT::io_wait_reserve);
        } else {
            return spsc_queue_capacity;
        }
    }();

    static constexpr std::size_t io_deferred_delete_reserve = [] {
        if constexpr (requires { TraitsT::io_deferred_delete_reserve; }) {
            return static_cast<std::size_t>(TraitsT::io_deferred_delete_reserve);
        } else {
            return io_wait_reserve;
        }
    }();

    static constexpr std::size_t io_uring_provided_buffer_group_reserve = [] {
        if constexpr (requires { TraitsT::io_uring_provided_buffer_group_reserve; }) {
            return static_cast<std::size_t>(TraitsT::io_uring_provided_buffer_group_reserve);
        } else {
            return static_cast<std::size_t>(4);
        }
    }();
};

} // namespace af::detail
