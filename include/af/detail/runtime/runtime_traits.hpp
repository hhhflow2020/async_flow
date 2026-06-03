#pragma once

#include <cstddef>
#include <cstdint>

#include "af/task.hpp"

namespace af::detail {

template <typename TraitsT> struct RuntimeTraitsConfig {
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

    static constexpr QueueFullPolicy runtime_queue_full_policy = [] {
        if constexpr (requires { TraitsT::runtime_queue_full_policy; }) {
            return TraitsT::runtime_queue_full_policy;
        } else {
            return queue_full_policy;
        }
    }();

    static constexpr QueueFullPolicy external_queue_full_policy = [] {
        if constexpr (requires { TraitsT::external_queue_full_policy; }) {
            return TraitsT::external_queue_full_policy;
        } else {
            return queue_full_policy;
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

    static constexpr unsigned io_uring_cq_entries = [] {
        if constexpr (requires { TraitsT::io_uring_cq_entries; }) {
            return static_cast<unsigned>(TraitsT::io_uring_cq_entries);
        } else {
            return 0U;
        }
    }();

    static constexpr unsigned io_uring_setup_flags = [] {
        if constexpr (requires { TraitsT::io_uring_setup_flags; }) {
            return static_cast<unsigned>(TraitsT::io_uring_setup_flags);
        } else {
            return 0U;
        }
    }();

    static constexpr bool io_uring_setup_sqpoll = [] {
        if constexpr (requires { TraitsT::io_uring_setup_sqpoll; }) {
            return static_cast<bool>(TraitsT::io_uring_setup_sqpoll);
        } else {
            return false;
        }
    }();

    static constexpr unsigned io_uring_sqpoll_idle_ms = [] {
        if constexpr (requires { TraitsT::io_uring_sqpoll_idle_ms; }) {
            return static_cast<unsigned>(TraitsT::io_uring_sqpoll_idle_ms);
        } else {
            return 1000U;
        }
    }();

    static constexpr int io_uring_sqpoll_cpu = [] {
        if constexpr (requires { TraitsT::io_uring_sqpoll_cpu; }) {
            return static_cast<int>(TraitsT::io_uring_sqpoll_cpu);
        } else {
            return -1;
        }
    }();

    static constexpr bool io_uring_setup_submit_all = [] {
        if constexpr (requires { TraitsT::io_uring_setup_submit_all; }) {
            return static_cast<bool>(TraitsT::io_uring_setup_submit_all);
        } else {
            return false;
        }
    }();

    static constexpr bool io_uring_setup_coop_taskrun = [] {
        if constexpr (requires { TraitsT::io_uring_setup_coop_taskrun; }) {
            return static_cast<bool>(TraitsT::io_uring_setup_coop_taskrun);
        } else {
            return false;
        }
    }();

    static constexpr bool io_uring_setup_single_issuer = [] {
        if constexpr (requires { TraitsT::io_uring_setup_single_issuer; }) {
            return static_cast<bool>(TraitsT::io_uring_setup_single_issuer);
        } else {
            return false;
        }
    }();

    static constexpr bool io_uring_setup_defer_taskrun = [] {
        if constexpr (requires { TraitsT::io_uring_setup_defer_taskrun; }) {
            return static_cast<bool>(TraitsT::io_uring_setup_defer_taskrun);
        } else {
            return false;
        }
    }();

    static constexpr std::size_t io_wait_reserve = [] {
        if constexpr (requires { TraitsT::io_wait_reserve; }) {
            return static_cast<std::size_t>(TraitsT::io_wait_reserve);
        } else {
            return spsc_queue_capacity;
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
