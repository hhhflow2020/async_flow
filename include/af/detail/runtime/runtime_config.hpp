#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "af/detail/runtime/runtime_traits.hpp"

namespace af::detail {

template <typename TraitsT> struct RuntimeConfig {
    using Thread = typename TraitsT::Thread;
    using TraitConfig = RuntimeTraitsConfig<TraitsT>;

    static_assert(TraitsT::thread_count > 0, "AsyncRuntime requires at least one fixed thread");
    static_assert(static_cast<std::uint64_t>(TraitsT::thread_count) <=
                      std::numeric_limits<std::uint16_t>::max(),
                  "AsyncRuntime thread_count must fit in 16-bit thread indexes");

    static constexpr std::uint16_t thread_count = static_cast<std::uint16_t>(TraitsT::thread_count);
    static constexpr std::uint16_t invalid_thread_index = thread_count;

    static constexpr std::size_t spsc_queue_capacity = TraitConfig::spsc_queue_capacity;
    static constexpr std::size_t external_queue_capacity = TraitConfig::external_queue_capacity;
    static constexpr QueueFullPolicy queue_full_policy = TraitConfig::queue_full_policy;
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
    static constexpr unsigned io_uring_entries = TraitConfig::io_uring_entries;
    static constexpr unsigned io_uring_submit_batch_threshold =
        TraitConfig::io_uring_submit_batch_threshold;
    static constexpr unsigned io_uring_cq_entries = TraitConfig::io_uring_cq_entries;
    static constexpr unsigned io_uring_setup_flags = TraitConfig::io_uring_setup_flags;
    static constexpr bool io_uring_setup_sqpoll = TraitConfig::io_uring_setup_sqpoll;
    static constexpr unsigned io_uring_sqpoll_idle_ms = TraitConfig::io_uring_sqpoll_idle_ms;
    static constexpr int io_uring_sqpoll_cpu = TraitConfig::io_uring_sqpoll_cpu;
    static constexpr bool io_uring_setup_submit_all = TraitConfig::io_uring_setup_submit_all;
    static constexpr bool io_uring_setup_coop_taskrun = TraitConfig::io_uring_setup_coop_taskrun;
    static constexpr bool io_uring_setup_single_issuer = TraitConfig::io_uring_setup_single_issuer;
    static constexpr bool io_uring_setup_defer_taskrun = TraitConfig::io_uring_setup_defer_taskrun;
    static constexpr std::size_t io_wait_reserve = TraitConfig::io_wait_reserve;
    static constexpr std::size_t io_uring_provided_buffer_group_reserve =
        TraitConfig::io_uring_provided_buffer_group_reserve;

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
    static_assert(io_uring_entries > 0, "io_uring_entries must be greater than zero");
    static_assert(std::has_single_bit(io_uring_entries), "io_uring_entries must be a power of two");
    static_assert(io_uring_submit_batch_threshold > 0,
                  "io_uring_submit_batch_threshold must be greater than zero");
    static_assert(io_uring_submit_batch_threshold <= io_uring_entries,
                  "io_uring_submit_batch_threshold must not exceed io_uring_entries");
    static_assert(io_uring_cq_entries == 0U || std::has_single_bit(io_uring_cq_entries),
                  "io_uring_cq_entries must be zero or a power of two");
    static_assert(io_uring_cq_entries == 0U || io_uring_cq_entries >= io_uring_entries,
                  "io_uring_cq_entries must be zero or not less than io_uring_entries");
    static_assert(!(io_uring_setup_sqpoll || io_uring_sqpoll_cpu >= 0) ||
                      io_uring_sqpoll_idle_ms > 0U,
                  "io_uring_sqpoll_idle_ms must be greater than zero when SQPOLL "
                  "is enabled");

    [[nodiscard]] static constexpr ThreadKind thread_kind(Thread thread) noexcept {
        if constexpr (requires { TraitsT::thread_kind(thread); }) {
            return TraitsT::thread_kind(thread);
        } else {
            static_cast<void>(thread);
            return ThreadKind::Worker;
        }
    }
};

} // namespace af::detail
