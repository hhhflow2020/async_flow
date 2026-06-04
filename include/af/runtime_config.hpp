#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "af/detail/log/async_logger.hpp"
#include "af/thread_kind.hpp"

namespace af {

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

struct thread_affinity_config {
    std::vector<std::uint32_t> cpu_ids;
};

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
    std::size_t timer_drain_budget{256};
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

inline constexpr std::uint16_t runtime_invalid_thread_index =
    std::numeric_limits<std::uint16_t>::max();

enum class runtime_config_status : std::uint8_t {
    ok,
    no_threads,
    thread_group_count_zero,
    thread_count_overflow,
    scheduler_task_drain_budget_zero,
    scheduler_timer_drain_budget_zero,
    scheduler_service_task_budget_zero,
    task_pool_local_cache_size_zero,
    task_pool_slab_object_count_zero,
    timer_tick_zero,
    timer_wheel_slots_zero,
    timer_drain_budget_zero,
    timer_initial_reserve_zero,
    reactor_event_capacity_zero,
    reactor_event_budget_zero,
    log_queue_capacity_zero,
    log_max_batch_records_zero,
    log_consumer_thread_not_found,
    log_file_backend_path_empty,
    log_udp_backend_host_empty,
    log_udp_backend_port_zero,
    log_udp_backend_thread_not_found,
    log_tcp_backend_host_empty,
    log_tcp_backend_port_zero,
    log_tcp_backend_thread_not_found,
};

[[nodiscard]] inline std::string_view
runtime_config_status_name(runtime_config_status status) noexcept {
    switch (status) {
    case runtime_config_status::ok:
        return "ok";
    case runtime_config_status::no_threads:
        return "no_threads";
    case runtime_config_status::thread_group_count_zero:
        return "thread_group_count_zero";
    case runtime_config_status::thread_count_overflow:
        return "thread_count_overflow";
    case runtime_config_status::scheduler_task_drain_budget_zero:
        return "scheduler_task_drain_budget_zero";
    case runtime_config_status::scheduler_timer_drain_budget_zero:
        return "scheduler_timer_drain_budget_zero";
    case runtime_config_status::scheduler_service_task_budget_zero:
        return "scheduler_service_task_budget_zero";
    case runtime_config_status::task_pool_local_cache_size_zero:
        return "task_pool_local_cache_size_zero";
    case runtime_config_status::task_pool_slab_object_count_zero:
        return "task_pool_slab_object_count_zero";
    case runtime_config_status::timer_tick_zero:
        return "timer_tick_zero";
    case runtime_config_status::timer_wheel_slots_zero:
        return "timer_wheel_slots_zero";
    case runtime_config_status::timer_drain_budget_zero:
        return "timer_drain_budget_zero";
    case runtime_config_status::timer_initial_reserve_zero:
        return "timer_initial_reserve_zero";
    case runtime_config_status::reactor_event_capacity_zero:
        return "reactor_event_capacity_zero";
    case runtime_config_status::reactor_event_budget_zero:
        return "reactor_event_budget_zero";
    case runtime_config_status::log_queue_capacity_zero:
        return "log_queue_capacity_zero";
    case runtime_config_status::log_max_batch_records_zero:
        return "log_max_batch_records_zero";
    case runtime_config_status::log_consumer_thread_not_found:
        return "log_consumer_thread_not_found";
    case runtime_config_status::log_file_backend_path_empty:
        return "log_file_backend_path_empty";
    case runtime_config_status::log_udp_backend_host_empty:
        return "log_udp_backend_host_empty";
    case runtime_config_status::log_udp_backend_port_zero:
        return "log_udp_backend_port_zero";
    case runtime_config_status::log_udp_backend_thread_not_found:
        return "log_udp_backend_thread_not_found";
    case runtime_config_status::log_tcp_backend_host_empty:
        return "log_tcp_backend_host_empty";
    case runtime_config_status::log_tcp_backend_port_zero:
        return "log_tcp_backend_port_zero";
    case runtime_config_status::log_tcp_backend_thread_not_found:
        return "log_tcp_backend_thread_not_found";
    }
    return "unknown";
}

struct runtime_config_validation_result {
    runtime_config_status status{runtime_config_status::ok};
    std::size_t index{0};

    [[nodiscard]] constexpr bool ok() const noexcept {
        return status == runtime_config_status::ok;
    }

    [[nodiscard]] explicit constexpr operator bool() const noexcept {
        return ok();
    }
};

struct runtime_thread_info {
    std::uint16_t index{runtime_invalid_thread_index};
    std::uint16_t group_index{runtime_invalid_thread_index};
    std::uint16_t group_offset{0};
    std::string name{"worker"};
    af::thread_kind kind{af::thread_kind::cpu};
    thread_affinity_config affinity;
    thread_priority_config priority;
    bool set_os_thread_name{true};
};

struct resolved_runtime_config {
    runtime_config config;
    std::vector<runtime_thread_info> threads;
    std::vector<std::uint16_t> io_threads;
    std::vector<std::uint16_t> cpu_threads;

    [[nodiscard]] std::uint16_t thread_count() const noexcept {
        return static_cast<std::uint16_t>(threads.size());
    }

    [[nodiscard]] std::uint16_t invalid_thread_index() const noexcept {
        return thread_count();
    }

    [[nodiscard]] bool valid_thread(std::uint16_t index) const noexcept {
        return index < thread_count();
    }

    [[nodiscard]] af::thread_kind thread_kind_of(std::uint16_t index) const noexcept {
        if (!valid_thread(index)) {
            return af::thread_kind::cpu;
        }
        return threads[index].kind;
    }

    [[nodiscard]] std::string_view thread_name(std::uint16_t index) const noexcept {
        if (!valid_thread(index)) {
            return "invalid";
        }
        return threads[index].name;
    }

    [[nodiscard]] std::uint16_t thread_group_offset(std::uint16_t index) const noexcept {
        if (!valid_thread(index)) {
            return 0;
        }
        return threads[index].group_offset;
    }

    [[nodiscard]] std::uint16_t select_thread(thread_selector selector) const noexcept {
        switch (selector.kind) {
        case thread_selector_kind::any_io:
            return io_threads.empty() ? invalid_thread_index() : io_threads.front();
        case thread_selector_kind::any_cpu:
            return cpu_threads.empty() ? invalid_thread_index() : cpu_threads.front();
        case thread_selector_kind::io:
            return selector.index < io_threads.size() ? io_threads[selector.index]
                                                      : invalid_thread_index();
        case thread_selector_kind::cpu:
            return selector.index < cpu_threads.size() ? cpu_threads[selector.index]
                                                       : invalid_thread_index();
        case thread_selector_kind::absolute:
            return valid_thread(selector.index) ? selector.index : invalid_thread_index();
        }
        return invalid_thread_index();
    }
};

struct runtime_config_resolution {
    runtime_config_validation_result validation;
    resolved_runtime_config resolved;

    [[nodiscard]] bool ok() const noexcept {
        return validation.ok();
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return ok();
    }
};

namespace detail {

[[nodiscard]] inline runtime_config_validation_result
runtime_config_error(runtime_config_status status, std::size_t index = 0) noexcept {
    return {status, index};
}

[[nodiscard]] inline runtime_config_validation_result
validate_runtime_thread_groups(const runtime_config &config) noexcept {
    if (config.threads.empty()) {
        return runtime_config_error(runtime_config_status::no_threads);
    }

    std::size_t total_threads = 0;
    for (std::size_t index = 0; index < config.threads.size(); ++index) {
        const auto &group = config.threads[index];
        if (group.count == 0) {
            return runtime_config_error(runtime_config_status::thread_group_count_zero, index);
        }
        total_threads += group.count;
        if (total_threads > std::numeric_limits<std::uint16_t>::max()) {
            return runtime_config_error(runtime_config_status::thread_count_overflow, index);
        }
    }
    return {};
}

inline void expand_runtime_threads(resolved_runtime_config &resolved) {
    const auto &groups = resolved.config.threads;
    std::size_t total_threads = 0;
    for (const auto &group : groups) {
        total_threads += group.count;
    }
    resolved.threads.reserve(total_threads);
    resolved.io_threads.reserve(total_threads);
    resolved.cpu_threads.reserve(total_threads);

    std::uint16_t index = 0;
    for (std::size_t group_index = 0; group_index < groups.size(); ++group_index) {
        const thread_group_config &group = groups[group_index];
        for (std::size_t group_offset = 0; group_offset < group.count; ++group_offset) {
            runtime_thread_info info;
            info.index = index;
            info.group_index = static_cast<std::uint16_t>(group_index);
            info.group_offset = static_cast<std::uint16_t>(group_offset);
            info.name = group.name.empty() ? "worker" : group.name;
            info.kind = group.kind;
            info.affinity = group.affinity;
            info.priority = group.priority;
            info.set_os_thread_name = group.set_os_thread_name;
            resolved.threads.push_back(std::move(info));
            if (group.kind == af::thread_kind::io) {
                resolved.io_threads.push_back(index);
            } else {
                resolved.cpu_threads.push_back(index);
            }
            ++index;
        }
    }
}

[[nodiscard]] inline runtime_config_validation_result
validate_log_backend(const resolved_runtime_config &resolved, const log_backend_config &backend,
                     std::size_t index) noexcept {
    if (const auto *file = std::get_if<file_log_backend_config>(&backend)) {
        if (file->path.empty()) {
            return runtime_config_error(runtime_config_status::log_file_backend_path_empty, index);
        }
        return {};
    }
    if (const auto *udp = std::get_if<udp_log_backend_config>(&backend)) {
        if (udp->host.empty()) {
            return runtime_config_error(runtime_config_status::log_udp_backend_host_empty, index);
        }
        if (udp->port == 0) {
            return runtime_config_error(runtime_config_status::log_udp_backend_port_zero, index);
        }
        if (!resolved.valid_thread(resolved.select_thread(udp->io_thread))) {
            return runtime_config_error(runtime_config_status::log_udp_backend_thread_not_found,
                                        index);
        }
        return {};
    }
    const auto *tcp = std::get_if<tcp_log_backend_config>(&backend);
    if (tcp == nullptr) {
        return {};
    }
    if (tcp->host.empty()) {
        return runtime_config_error(runtime_config_status::log_tcp_backend_host_empty, index);
    }
    if (tcp->port == 0) {
        return runtime_config_error(runtime_config_status::log_tcp_backend_port_zero, index);
    }
    if (!resolved.valid_thread(resolved.select_thread(tcp->io_thread))) {
        return runtime_config_error(runtime_config_status::log_tcp_backend_thread_not_found, index);
    }
    return {};
}

[[nodiscard]] inline runtime_config_validation_result
validate_resolved_runtime_config(const resolved_runtime_config &resolved) noexcept {
    const runtime_config &config = resolved.config;
    if (const auto validation = validate_runtime_thread_groups(config); !validation) {
        return validation;
    }

    if (config.scheduler.task_drain_budget == 0) {
        return runtime_config_error(runtime_config_status::scheduler_task_drain_budget_zero);
    }
    if (config.scheduler.timer_drain_budget == 0) {
        return runtime_config_error(runtime_config_status::scheduler_timer_drain_budget_zero);
    }
    if (config.scheduler.service_task_budget == 0) {
        return runtime_config_error(runtime_config_status::scheduler_service_task_budget_zero);
    }
    if (config.task_pool.local_cache_size == 0) {
        return runtime_config_error(runtime_config_status::task_pool_local_cache_size_zero);
    }
    if (config.task_pool.slab_object_count == 0) {
        return runtime_config_error(runtime_config_status::task_pool_slab_object_count_zero);
    }
    if (config.timer.tick.count() <= 0) {
        return runtime_config_error(runtime_config_status::timer_tick_zero);
    }
    if (config.timer.wheel_slots == 0) {
        return runtime_config_error(runtime_config_status::timer_wheel_slots_zero);
    }
    if (config.timer.drain_budget == 0) {
        return runtime_config_error(runtime_config_status::timer_drain_budget_zero);
    }
    if (config.timer.initial_reserve == 0) {
        return runtime_config_error(runtime_config_status::timer_initial_reserve_zero);
    }
    if (config.reactor.event_capacity == 0) {
        return runtime_config_error(runtime_config_status::reactor_event_capacity_zero);
    }
    if (config.reactor.event_budget == 0) {
        return runtime_config_error(runtime_config_status::reactor_event_budget_zero);
    }
    if (config.logger.queue_capacity == 0) {
        return runtime_config_error(runtime_config_status::log_queue_capacity_zero);
    }
    if (config.logger.max_batch_records == 0) {
        return runtime_config_error(runtime_config_status::log_max_batch_records_zero);
    }
    if (!resolved.valid_thread(resolved.select_thread(config.logger.consumer_thread))) {
        return runtime_config_error(runtime_config_status::log_consumer_thread_not_found);
    }

    for (std::size_t index = 0; index < config.logger.backends.size(); ++index) {
        if (const auto validation =
                validate_log_backend(resolved, config.logger.backends[index], index);
            !validation) {
            return validation;
        }
    }
    return {};
}

} // namespace detail

[[nodiscard]] inline runtime_config_resolution resolve_runtime_config(runtime_config config) {
    runtime_config_resolution resolution;
    resolution.resolved.config = std::move(config);
    if (const auto validation = detail::validate_runtime_thread_groups(resolution.resolved.config);
        !validation) {
        resolution.validation = validation;
        return resolution;
    }
    detail::expand_runtime_threads(resolution.resolved);
    resolution.validation = detail::validate_resolved_runtime_config(resolution.resolved);
    return resolution;
}

[[nodiscard]] inline runtime_config_validation_result
validate_runtime_config(runtime_config config) {
    return resolve_runtime_config(std::move(config)).validation;
}

} // namespace af
