#pragma once

#include <cstddef>
#include <cstdint>

#include "af/task.hpp"

namespace af::detail {

template <typename TraitsT> struct RuntimeTraitsConfig {
    static_assert(
        !requires { TraitsT::queue_full_policy; },
        "queue_full_policy has been removed; define runtime_queue_full_policy "
        "and external_queue_full_policy explicitly");

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
            return static_cast<std::size_t>(1024);
        }
    }();

    static constexpr QueueFullPolicy runtime_queue_full_policy = [] {
        if constexpr (requires { TraitsT::runtime_queue_full_policy; }) {
            return TraitsT::runtime_queue_full_policy;
        } else {
            return QueueFullPolicy::Reject;
        }
    }();

    static constexpr QueueFullPolicy external_queue_full_policy = [] {
        if constexpr (requires { TraitsT::external_queue_full_policy; }) {
            return TraitsT::external_queue_full_policy;
        } else {
            return QueueFullPolicy::Reject;
        }
    }();

    static constexpr std::size_t queue_full_spin_count = [] {
        if constexpr (requires { TraitsT::queue_full_spin_count; }) {
            return static_cast<std::size_t>(TraitsT::queue_full_spin_count);
        } else {
            return static_cast<std::size_t>(64);
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
        constexpr bool explicitly_enabled = [] {
            if constexpr (requires { TraitsT::enable_task_registry; }) {
                return static_cast<bool>(TraitsT::enable_task_registry);
            } else {
                return false;
            }
        }();
        if constexpr (shutdown_policy == ShutdownPolicy::StopImmediately) {
            return true;
        } else {
            return explicitly_enabled;
        }
    }();

    static constexpr std::size_t task_pool_remote_release_batch_size = [] {
        if constexpr (requires { TraitsT::task_pool_remote_release_batch_size; }) {
            return static_cast<std::size_t>(TraitsT::task_pool_remote_release_batch_size);
        } else {
            return static_cast<std::size_t>(64);
        }
    }();

    static constexpr std::size_t task_pool_chunk_size = [] {
        if constexpr (requires { TraitsT::task_pool_chunk_size; }) {
            return static_cast<std::size_t>(TraitsT::task_pool_chunk_size);
        } else {
            return static_cast<std::size_t>(256);
        }
    }();

    static constexpr bool task_pool_cache_slot_index = [] {
        if constexpr (requires { TraitsT::task_pool_cache_slot_index; }) {
            return static_cast<bool>(TraitsT::task_pool_cache_slot_index);
        } else {
            return false;
        }
    }();

    static constexpr std::size_t task_pool_local_cache_set_size = [] {
        if constexpr (requires { TraitsT::task_pool_local_cache_set_size; }) {
            return static_cast<std::size_t>(TraitsT::task_pool_local_cache_set_size);
        } else {
            return static_cast<std::size_t>(1);
        }
    }();

    static constexpr std::size_t task_pool_direct_release_set_size = [] {
        if constexpr (requires { TraitsT::task_pool_direct_release_set_size; }) {
            return static_cast<std::size_t>(TraitsT::task_pool_direct_release_set_size);
        } else {
            return static_cast<std::size_t>(4);
        }
    }();

    static constexpr std::size_t task_pool_local_cache_capacity = [] {
        if constexpr (requires { TraitsT::task_pool_local_cache_capacity; }) {
            return static_cast<std::size_t>(TraitsT::task_pool_local_cache_capacity);
        } else {
            return static_cast<std::size_t>(64);
        }
    }();

    static constexpr std::size_t io_wait_reserve = [] {
        if constexpr (requires { TraitsT::io_wait_reserve; }) {
            return static_cast<std::size_t>(TraitsT::io_wait_reserve);
        } else {
            return spsc_queue_capacity;
        }
    }();
};

} // namespace af::detail
