#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <type_traits>

#include "af/detail/runtime/runtime_traits.hpp"

namespace af::detail {

template <typename TraitsT, typename = void> struct RuntimeTraitsHasThreads : std::false_type {};

template <typename TraitsT>
struct RuntimeTraitsHasThreads<TraitsT, std::void_t<decltype(TraitsT::threads)>> : std::true_type {
};

template <typename TraitsT> struct RuntimeConfig {
    static_assert(RuntimeTraitsHasThreads<TraitsT>::value,
                  "AsyncRuntime traits must define static constexpr auto threads = "
                  "af::thread_layout(...)");

    using ThreadLayout = std::remove_cv_t<decltype(TraitsT::threads)>;
    using Thread = typename ThreadLayout::Thread;
    using TraitConfig = RuntimeTraitsConfig<TraitsT>;

    static constexpr ThreadLayout threads = TraitsT::threads;
    static constexpr std::uint16_t thread_count = ThreadLayout::thread_count;
    static constexpr std::uint16_t invalid_thread_index = ThreadLayout::invalid_thread_index;

    static constexpr std::size_t spsc_queue_capacity = TraitConfig::spsc_queue_capacity;
    static constexpr std::size_t external_queue_capacity = TraitConfig::external_queue_capacity;
    static constexpr QueueFullPolicy runtime_queue_full_policy =
        TraitConfig::runtime_queue_full_policy;
    static constexpr QueueFullPolicy external_queue_full_policy =
        TraitConfig::external_queue_full_policy;
    static constexpr std::size_t queue_full_spin_count = TraitConfig::queue_full_spin_count;
    static constexpr ShutdownPolicy shutdown_policy = TraitConfig::shutdown_policy;
    static constexpr bool task_registry_enabled = TraitConfig::task_registry_enabled;
    static constexpr std::size_t task_pool_remote_release_batch_size =
        TraitConfig::task_pool_remote_release_batch_size;
    static constexpr std::size_t task_pool_chunk_size = TraitConfig::task_pool_chunk_size;
    static constexpr bool task_pool_cache_slot_index = TraitConfig::task_pool_cache_slot_index;
    static constexpr std::size_t task_pool_local_cache_set_size =
        TraitConfig::task_pool_local_cache_set_size;
    static constexpr std::size_t task_pool_direct_release_set_size =
        TraitConfig::task_pool_direct_release_set_size;
    static constexpr std::size_t task_pool_local_cache_capacity =
        TraitConfig::task_pool_local_cache_capacity;
    static constexpr std::size_t io_wait_reserve = TraitConfig::io_wait_reserve;

    static_assert(spsc_queue_capacity > 0, "spsc_queue_capacity must be greater than zero");
    static_assert(external_queue_capacity > 0, "external_queue_capacity must be greater than zero");
    static_assert(task_pool_remote_release_batch_size > 0,
                  "task_pool_remote_release_batch_size must be greater than zero");
    static_assert(task_pool_chunk_size > 0, "task_pool_chunk_size must be greater than zero");
    static_assert(task_pool_chunk_size <
                      static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max()),
                  "task_pool_chunk_size must leave 48 bits for the object-pool "
                  "free-list ABA tag");
    static_assert(task_pool_local_cache_set_size >= 1,
                  "task_pool_local_cache_set_size must be at least one");
    static_assert(task_pool_direct_release_set_size > 0,
                  "task_pool_direct_release_set_size must be greater than zero");
    static_assert(task_pool_local_cache_capacity > 0,
                  "task_pool_local_cache_capacity must be greater than zero");
    static_assert(task_pool_remote_release_batch_size <= task_pool_local_cache_capacity,
                  "task_pool_remote_release_batch_size must not exceed "
                  "task_pool_local_cache_capacity");
    [[nodiscard]] static constexpr af::thread_kind thread_kind(Thread thread) noexcept {
        return threads.thread_kind(thread);
    }

    [[nodiscard]] static std::string_view thread_name(Thread thread) noexcept {
        return threads.thread_name(thread);
    }

    [[nodiscard]] static constexpr std::uint16_t thread_group_offset(Thread thread) noexcept {
        return threads.thread_group_offset(thread);
    }

    template <typename Tag> [[nodiscard]] static constexpr auto thread_group() noexcept {
        return ThreadLayout::template group<Tag>();
    }
};

} // namespace af::detail
