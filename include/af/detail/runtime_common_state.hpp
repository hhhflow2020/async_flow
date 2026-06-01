#pragma once

#include <atomic>
#include <cstdint>

#include "af/detail/config.hpp"

namespace af::detail {

enum class RuntimeStatus : std::uint8_t {
    Stopped,
    Starting,
    Running,
    Stopping,
};

template <typename T> struct alignas(hardware_cache_line_size) CacheLineAtomic {
    std::atomic<T> value;

    constexpr CacheLineAtomic() noexcept = default;
    constexpr explicit CacheLineAtomic(T initial) noexcept : value(initial) {}

    CacheLineAtomic(const CacheLineAtomic &) = delete;
    CacheLineAtomic &operator=(const CacheLineAtomic &) = delete;

    [[nodiscard]] T load(std::memory_order order = std::memory_order_seq_cst) const noexcept {
        return value.load(order);
    }

    void store(T desired, std::memory_order order = std::memory_order_seq_cst) noexcept {
        value.store(desired, order);
    }

    [[nodiscard]] bool compare_exchange_weak(T &expected, T desired, std::memory_order success,
                                             std::memory_order failure) noexcept {
        return value.compare_exchange_weak(expected, desired, success, failure);
    }

    [[nodiscard]] bool compare_exchange_strong(T &expected, T desired, std::memory_order success,
                                               std::memory_order failure) noexcept {
        return value.compare_exchange_strong(expected, desired, success, failure);
    }

    T fetch_add(T arg, std::memory_order order = std::memory_order_seq_cst) noexcept {
        return value.fetch_add(arg, order);
    }

    T fetch_sub(T arg, std::memory_order order = std::memory_order_seq_cst) noexcept {
        return value.fetch_sub(arg, order);
    }

    T fetch_and(T arg, std::memory_order order = std::memory_order_seq_cst) noexcept {
        return value.fetch_and(arg, order);
    }

    void wait(T old, std::memory_order order = std::memory_order_seq_cst) const noexcept {
        value.wait(old, order);
    }

    void notify_one() noexcept {
        value.notify_one();
    }

    void notify_all() noexcept {
        value.notify_all();
    }
};

struct alignas(hardware_cache_line_size) OrderedBatchState {
    std::uint64_t last_applied_batch_id{0};
};

struct ExternalPostCounter {
    CacheLineAtomic<std::uint32_t> value{0};
};

template <typename RuntimeT> struct RuntimeParallelGroup {
    using Task = typename RuntimeT::Task;
    using Thread = typename RuntimeT::Thread;

    std::atomic<std::uint32_t> pending{0};
    Task *owner{nullptr};
    std::uint16_t resume_thread{RuntimeT::invalid_thread_index};
    std::atomic<std::uint32_t> failed{0};

    void init(std::uint32_t target_count, Task *group_owner,
              std::uint16_t group_resume_thread) noexcept {
        pending.store(target_count, std::memory_order_relaxed);
        owner = group_owner;
        resume_thread = group_resume_thread;
        failed.store(0, std::memory_order_relaxed);
    }

    void complete(bool ok, bool resume_owner = true) noexcept {
        if (!ok) {
            failed.fetch_add(1, std::memory_order_relaxed);
        }
        if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
            if (resume_owner && owner != nullptr && resume_thread < RuntimeT::thread_count) {
                RuntimeT::set_task_parallel_failures(owner, failed.load(std::memory_order_acquire));
                RuntimeT::post_blocking(RuntimeT::thread_from_index(resume_thread), owner);
            }
            RuntimeT::destroy_parallel_group(this);
        }
    }
};

} // namespace af::detail
