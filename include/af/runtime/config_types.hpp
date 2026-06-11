#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "af/detail/log/async_log_config.hpp"
#include "af/thread_kind.hpp"

namespace af {

inline constexpr std::uint16_t runtime_invalid_thread_index =
    std::numeric_limits<std::uint16_t>::max();

enum class idle_wait_strategy : std::uint8_t {
    futex,
    spin,
    yield,
};

enum class wake_policy : std::uint8_t {
    always,
    empty_to_non_empty,
};

enum class oom_policy : std::uint8_t {
    fatal,
    throw_exception,
};

enum class timer_kind : std::uint8_t {
    min_heap,
    hierarchical_wheel,
};

enum class reactor_backend : std::uint8_t {
    auto_select,
    epoll,
    kqueue,
    select,
};

enum class thread_selector_kind : std::uint8_t {
    any_io,
    any_cpu,
    io,
    cpu,
    absolute,
};

struct thread_selector {
    thread_selector_kind kind{thread_selector_kind::any_cpu};
    std::uint16_t index{0};

    [[nodiscard]] static constexpr thread_selector any_io() noexcept {
        return {thread_selector_kind::any_io, 0};
    }

    [[nodiscard]] static constexpr thread_selector any_cpu() noexcept {
        return {thread_selector_kind::any_cpu, 0};
    }

    [[nodiscard]] static constexpr thread_selector io(std::uint16_t index_value) noexcept {
        return {thread_selector_kind::io, index_value};
    }

    [[nodiscard]] static constexpr thread_selector cpu(std::uint16_t index_value) noexcept {
        return {thread_selector_kind::cpu, index_value};
    }

    [[nodiscard]] static constexpr thread_selector thread(std::uint16_t index_value) noexcept {
        return {thread_selector_kind::absolute, index_value};
    }
};

struct thread_ref {
    std::uint16_t index{runtime_invalid_thread_index};

    constexpr thread_ref() noexcept = default;
    explicit constexpr thread_ref(std::uint16_t thread_index) noexcept : index(thread_index) {}

    [[nodiscard]] constexpr bool valid() const noexcept {
        return index != runtime_invalid_thread_index;
    }

    [[nodiscard]] explicit constexpr operator bool() const noexcept {
        return valid();
    }

    [[nodiscard]] friend constexpr bool operator==(thread_ref lhs, thread_ref rhs) noexcept {
        return lhs.index == rhs.index;
    }

    [[nodiscard]] friend constexpr bool operator!=(thread_ref lhs, thread_ref rhs) noexcept {
        return !(lhs == rhs);
    }
};

class thread_group_ref {
public:
    using const_iterator = const std::uint16_t *;

    constexpr thread_group_ref() noexcept = default;
    constexpr thread_group_ref(const std::uint16_t *threads, std::size_t count) noexcept
        : threads_(threads), count_(count) {}
    constexpr thread_group_ref(const std::uint16_t *threads, std::size_t count,
                               std::uint16_t begin_index, bool contiguous) noexcept
        : threads_(threads), count_(count), begin_index_(begin_index), contiguous_(contiguous) {}

    [[nodiscard]] constexpr bool empty() const noexcept {
        return count_ == 0;
    }

    [[nodiscard]] explicit constexpr operator bool() const noexcept {
        return !empty();
    }

    [[nodiscard]] constexpr std::size_t size() const noexcept {
        return count_;
    }

    [[nodiscard]] constexpr const std::uint16_t *data() const noexcept {
        return threads_;
    }

    [[nodiscard]] constexpr bool is_contiguous() const noexcept {
        return contiguous_;
    }

    [[nodiscard]] constexpr std::uint16_t begin_index() const noexcept {
        return contiguous_ ? begin_index_ : runtime_invalid_thread_index;
    }

    [[nodiscard]] constexpr const_iterator begin() const noexcept {
        return threads_;
    }

    [[nodiscard]] constexpr const_iterator end() const noexcept {
        return count_ == 0 ? threads_ : threads_ + count_;
    }

    [[nodiscard]] constexpr thread_ref at(std::size_t index) const noexcept {
        if (index >= count_) {
            return {};
        }
        if (contiguous_) {
            return thread_ref(static_cast<std::uint16_t>(begin_index_ + index));
        }
        if (threads_ == nullptr) {
            return {};
        }
        return thread_ref(threads_[index]);
    }

    [[nodiscard]] constexpr thread_ref operator[](std::size_t index) const noexcept {
        return at(index);
    }

    [[nodiscard]] constexpr thread_ref front() const noexcept {
        return at(0);
    }

    [[nodiscard]] constexpr thread_ref shard(std::size_t value) const noexcept {
        if (count_ == 0) {
            return {};
        }
        const std::size_t index = value % count_;
        if (contiguous_) {
            return thread_ref(static_cast<std::uint16_t>(begin_index_ + index));
        }
        if (threads_ == nullptr) {
            return {};
        }
        return thread_ref(threads_[index]);
    }

    [[nodiscard]] constexpr bool contains(thread_ref thread) const noexcept {
        if (!thread.valid()) {
            return false;
        }
        if (contiguous_) {
            const std::size_t offset = thread.index >= begin_index_
                                           ? static_cast<std::size_t>(thread.index - begin_index_)
                                           : count_;
            return offset < count_;
        }
        if (threads_ == nullptr) {
            return false;
        }
        for (std::size_t i = 0; i < count_; ++i) {
            if (threads_[i] == thread.index) {
                return true;
            }
        }
        return false;
    }

private:
    const std::uint16_t *threads_{nullptr};
    std::size_t count_{0};
    std::uint16_t begin_index_{runtime_invalid_thread_index};
    bool contiguous_{false};
};

struct thread_affinity_config {
    std::vector<std::uint32_t> cpu_ids;
};

inline constexpr int thread_priority_min = -20;
inline constexpr int thread_priority_max = 19;

struct thread_priority_config {
    bool enabled{false};
    int value{0};
};

struct thread_group_config {
    std::string name{"worker"};
    af::thread_kind kind{af::thread_kind::cpu};
    std::size_t count{1};
    thread_affinity_config affinity;
    thread_priority_config priority;
    bool set_os_thread_name{true};
};

using thread_layout_config = std::vector<thread_group_config>;

[[nodiscard]] inline thread_group_config io_threads(std::string name, std::size_t count) {
    thread_group_config config;
    config.name = std::move(name);
    config.kind = af::thread_kind::io;
    config.count = count;
    return config;
}

[[nodiscard]] inline thread_group_config cpu_threads(std::string name, std::size_t count) {
    thread_group_config config;
    config.name = std::move(name);
    config.kind = af::thread_kind::cpu;
    config.count = count;
    return config;
}

struct scheduler_config {
    std::size_t task_drain_budget{256};
    std::size_t service_task_budget{32};
    std::chrono::nanoseconds max_task_run_slice{0};
    idle_wait_strategy idle_wait{idle_wait_strategy::futex};
    wake_policy wake{wake_policy::empty_to_non_empty};
};

struct task_pool_config {
    std::size_t local_cache_size{256};
    std::size_t slab_object_count{4096};
    oom_policy oom{oom_policy::fatal};
    bool enable_stats{true};
};

inline constexpr std::size_t runtime_task_pool_min_local_cache_size = 2;
inline constexpr std::size_t runtime_task_pool_max_local_cache_size = 4096;

[[nodiscard]] constexpr std::size_t
normalize_runtime_task_pool_local_cache_size(std::size_t value) noexcept {
    if (value <= runtime_task_pool_min_local_cache_size) {
        return runtime_task_pool_min_local_cache_size;
    }
    if (value <= 4U) {
        return 4U;
    }
    if (value <= 8U) {
        return 8U;
    }
    if (value <= 16U) {
        return 16U;
    }
    if (value <= 32U) {
        return 32U;
    }
    if (value <= 64U) {
        return 64U;
    }
    if (value <= 128U) {
        return 128U;
    }
    if (value <= 256U) {
        return 256U;
    }
    if (value <= 512U) {
        return 512U;
    }
    if (value <= 1024U) {
        return 1024U;
    }
    if (value <= 2048U) {
        return 2048U;
    }
    return runtime_task_pool_max_local_cache_size;
}

struct timer_config {
    timer_kind kind{timer_kind::hierarchical_wheel};
    std::chrono::milliseconds tick{1};
    std::size_t wheel_slots{4096};
    std::size_t drain_budget{256};
    std::size_t initial_reserve{1024};
};

struct reactor_config {
    reactor_backend backend{reactor_backend::auto_select};
    std::size_t event_capacity{1024};
    std::size_t event_budget{1024};
    bool edge_triggered{false};
};

struct log_record_pool_config {
    std::size_t local_cache_size{256};
    std::size_t slab_object_count{4096};
    oom_policy oom{oom_policy::fatal};
    bool enable_stats{true};
};

struct file_log_backend_config {
    std::string path;
    bool append{true};
    bool fsync_on_flush{false};
    std::size_t write_batch_iov{64};
};

struct udp_log_backend_config {
    std::string host;
    std::uint16_t port{0};
    std::size_t max_datagram_size{1400};
    thread_selector io_thread{thread_selector::any_io()};
};

struct tcp_log_backend_config {
    std::string host;
    std::uint16_t port{0};
    std::chrono::milliseconds reconnect_interval{std::chrono::milliseconds(500)};
    thread_selector io_thread{thread_selector::any_io()};
};

using log_backend_config =
    std::variant<file_log_backend_config, udp_log_backend_config, tcp_log_backend_config>;

struct log_config {
    [[nodiscard]] static log_config ordered() noexcept {
        log_config config;
        config.ordering = log_ordering::ordered;
        return config;
    }

    [[nodiscard]] static log_config relaxed() noexcept {
        log_config config;
        config.ordering = log_ordering::relaxed;
        return config;
    }

    log_ordering ordering{log_ordering::ordered};
    thread_selector consumer_thread{thread_selector::cpu(0)};
    std::size_t queue_capacity{1U << 16U};
    std::size_t queue_shard_count{0};
    std::size_t runtime_thread_count{0};
    std::size_t max_batch_records{256};
    std::chrono::microseconds max_batch_delay{1000};
    log_overflow_policy overflow{log_overflow_policy::drop_newest};
    log_record_pool_config record_pool;
    std::vector<log_backend_config> backends;
};

struct shutdown_config {
    std::chrono::seconds drain_timeout{5};
    std::chrono::seconds connection_close_timeout{5};
    std::chrono::seconds log_flush_timeout{5};
    bool stop_accept_first{true};
};

struct diagnostics_config {
    bool enable_task_id{true};
    bool enable_stats{true};
    bool enable_thread_name{true};
    bool enable_queue_metrics{true};
};

struct runtime_config {
    thread_layout_config threads;
    scheduler_config scheduler;
    task_pool_config task_pool;
    timer_config timer;
    reactor_config reactor;
    log_config logger;
    shutdown_config shutdown;
    diagnostics_config diagnostics;
};

} // namespace af
