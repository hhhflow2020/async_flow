#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "af/task.hpp"

namespace af::detail {

template <typename TraitsT, typename = void>
struct RuntimeTraitsHasShutdownPolicy : std::false_type {};
template <typename TraitsT>
struct RuntimeTraitsHasShutdownPolicy<TraitsT, std::void_t<decltype(TraitsT::shutdown_policy)>>
    : std::true_type {};

template <typename TraitsT, typename = void>
struct RuntimeTraitsHasEnableTaskRegistry : std::false_type {};
template <typename TraitsT>
struct RuntimeTraitsHasEnableTaskRegistry<TraitsT,
                                          std::void_t<decltype(TraitsT::enable_task_registry)>>
    : std::true_type {};

template <typename TraitsT, typename = void>
struct RuntimeTraitsHasTaskPoolRemoteReleaseBatchSize : std::false_type {};
template <typename TraitsT>
struct RuntimeTraitsHasTaskPoolRemoteReleaseBatchSize<
    TraitsT, std::void_t<decltype(TraitsT::task_pool_remote_release_batch_size)>> : std::true_type {
};

template <typename TraitsT, typename = void>
struct RuntimeTraitsHasTaskPoolChunkSize : std::false_type {};
template <typename TraitsT>
struct RuntimeTraitsHasTaskPoolChunkSize<TraitsT,
                                         std::void_t<decltype(TraitsT::task_pool_chunk_size)>>
    : std::true_type {};

template <typename TraitsT, typename = void>
struct RuntimeTraitsHasTaskPoolCacheSlotIndex : std::false_type {};
template <typename TraitsT>
struct RuntimeTraitsHasTaskPoolCacheSlotIndex<
    TraitsT, std::void_t<decltype(TraitsT::task_pool_cache_slot_index)>> : std::true_type {};

template <typename TraitsT, typename = void>
struct RuntimeTraitsHasTaskPoolLocalCacheSetSize : std::false_type {};
template <typename TraitsT>
struct RuntimeTraitsHasTaskPoolLocalCacheSetSize<
    TraitsT, std::void_t<decltype(TraitsT::task_pool_local_cache_set_size)>> : std::true_type {};

template <typename TraitsT, typename = void>
struct RuntimeTraitsHasTaskPoolDirectReleaseSetSize : std::false_type {};
template <typename TraitsT>
struct RuntimeTraitsHasTaskPoolDirectReleaseSetSize<
    TraitsT, std::void_t<decltype(TraitsT::task_pool_direct_release_set_size)>> : std::true_type {};

template <typename TraitsT, typename = void>
struct RuntimeTraitsHasTaskPoolLocalCacheCapacity : std::false_type {};
template <typename TraitsT>
struct RuntimeTraitsHasTaskPoolLocalCacheCapacity<
    TraitsT, std::void_t<decltype(TraitsT::task_pool_local_cache_capacity)>> : std::true_type {};

template <typename TraitsT, typename = void>
struct RuntimeTraitsHasIoWaitReserve : std::false_type {};
template <typename TraitsT>
struct RuntimeTraitsHasIoWaitReserve<TraitsT, std::void_t<decltype(TraitsT::io_wait_reserve)>>
    : std::true_type {};

template <typename TraitsT, typename = void>
struct RuntimeTraitsHasTimerDrainBudget : std::false_type {};
template <typename TraitsT>
struct RuntimeTraitsHasTimerDrainBudget<TraitsT, std::void_t<decltype(TraitsT::timer_drain_budget)>>
    : std::true_type {};

template <typename TraitsT, typename = void>
struct RuntimeTraitsHasTimerReserve : std::false_type {};
template <typename TraitsT>
struct RuntimeTraitsHasTimerReserve<TraitsT, std::void_t<decltype(TraitsT::timer_reserve)>>
    : std::true_type {};

template <typename TraitsT, typename = void>
struct RuntimeTraitsHasServiceTaskBudget : std::false_type {};
template <typename TraitsT>
struct RuntimeTraitsHasServiceTaskBudget<TraitsT,
                                         std::void_t<decltype(TraitsT::service_task_budget)>>
    : std::true_type {};

template <typename TraitsT> struct RuntimeTraitsConfig {
    static constexpr ShutdownPolicy shutdown_policy = [] {
        if constexpr (RuntimeTraitsHasShutdownPolicy<TraitsT>::value) {
            return TraitsT::shutdown_policy;
        } else {
            return ShutdownPolicy::WaitForTasks;
        }
    }();

    static constexpr bool task_registry_enabled = [] {
        constexpr bool explicitly_enabled = [] {
            if constexpr (RuntimeTraitsHasEnableTaskRegistry<TraitsT>::value) {
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
        if constexpr (RuntimeTraitsHasTaskPoolRemoteReleaseBatchSize<TraitsT>::value) {
            return static_cast<std::size_t>(TraitsT::task_pool_remote_release_batch_size);
        } else {
            return static_cast<std::size_t>(64);
        }
    }();

    static constexpr std::size_t task_pool_chunk_size = [] {
        if constexpr (RuntimeTraitsHasTaskPoolChunkSize<TraitsT>::value) {
            return static_cast<std::size_t>(TraitsT::task_pool_chunk_size);
        } else {
            return static_cast<std::size_t>(256);
        }
    }();

    static constexpr bool task_pool_cache_slot_index = [] {
        if constexpr (RuntimeTraitsHasTaskPoolCacheSlotIndex<TraitsT>::value) {
            return static_cast<bool>(TraitsT::task_pool_cache_slot_index);
        } else {
            return false;
        }
    }();

    static constexpr std::size_t task_pool_local_cache_set_size = [] {
        if constexpr (RuntimeTraitsHasTaskPoolLocalCacheSetSize<TraitsT>::value) {
            return static_cast<std::size_t>(TraitsT::task_pool_local_cache_set_size);
        } else {
            return static_cast<std::size_t>(1);
        }
    }();

    static constexpr std::size_t task_pool_direct_release_set_size = [] {
        if constexpr (RuntimeTraitsHasTaskPoolDirectReleaseSetSize<TraitsT>::value) {
            return static_cast<std::size_t>(TraitsT::task_pool_direct_release_set_size);
        } else {
            return static_cast<std::size_t>(4);
        }
    }();

    static constexpr std::size_t task_pool_local_cache_capacity = [] {
        if constexpr (RuntimeTraitsHasTaskPoolLocalCacheCapacity<TraitsT>::value) {
            return static_cast<std::size_t>(TraitsT::task_pool_local_cache_capacity);
        } else {
            return static_cast<std::size_t>(64);
        }
    }();

    static constexpr std::size_t io_wait_reserve = [] {
        if constexpr (RuntimeTraitsHasIoWaitReserve<TraitsT>::value) {
            return static_cast<std::size_t>(TraitsT::io_wait_reserve);
        } else {
            return static_cast<std::size_t>(1024);
        }
    }();

    static constexpr std::size_t timer_drain_budget = [] {
        if constexpr (RuntimeTraitsHasTimerDrainBudget<TraitsT>::value) {
            return static_cast<std::size_t>(TraitsT::timer_drain_budget);
        } else {
            return static_cast<std::size_t>(256);
        }
    }();

    static constexpr std::size_t timer_reserve = [] {
        if constexpr (RuntimeTraitsHasTimerReserve<TraitsT>::value) {
            return static_cast<std::size_t>(TraitsT::timer_reserve);
        } else {
            return static_cast<std::size_t>(1024);
        }
    }();

    static constexpr std::size_t service_task_budget = [] {
        if constexpr (RuntimeTraitsHasServiceTaskBudget<TraitsT>::value) {
            return static_cast<std::size_t>(TraitsT::service_task_budget);
        } else {
            return static_cast<std::size_t>(32);
        }
    }();
};

} // namespace af::detail
