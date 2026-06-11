#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "af/runtime/config_types.hpp"

namespace af {

enum class runtime_config_status : std::uint8_t {
    ok,
    no_threads,
    thread_group_count_zero,
    thread_count_overflow,
    thread_priority_value_out_of_range,
    scheduler_task_drain_budget_zero,
    scheduler_service_task_budget_zero,
    task_pool_local_cache_size_zero,
    task_pool_slab_object_count_zero,
    timer_tick_zero,
    timer_wheel_slots_zero,
    timer_drain_budget_zero,
    timer_initial_reserve_zero,
    timer_kind_unsupported,
    reactor_event_capacity_zero,
    reactor_event_budget_zero,
    log_queue_capacity_zero,
    log_max_batch_records_zero,
    log_record_pool_local_cache_size_zero,
    log_record_pool_local_cache_size_too_large,
    log_record_pool_slab_object_count_zero,
    log_consumer_thread_not_found,
    log_file_backend_path_empty,
    log_udp_backend_host_empty,
    log_udp_backend_port_zero,
    log_udp_backend_thread_not_found,
    log_udp_backend_thread_not_io,
    log_tcp_backend_host_empty,
    log_tcp_backend_port_zero,
    log_tcp_backend_thread_not_found,
    log_tcp_backend_thread_not_io,
    scheduler_max_task_run_slice_negative,
    shutdown_drain_timeout_negative,
    shutdown_connection_close_timeout_negative,
    shutdown_log_flush_timeout_negative,
    task_pool_local_cache_size_too_large,
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
    case runtime_config_status::thread_priority_value_out_of_range:
        return "thread_priority_value_out_of_range";
    case runtime_config_status::scheduler_task_drain_budget_zero:
        return "scheduler_task_drain_budget_zero";
    case runtime_config_status::scheduler_service_task_budget_zero:
        return "scheduler_service_task_budget_zero";
    case runtime_config_status::scheduler_max_task_run_slice_negative:
        return "scheduler_max_task_run_slice_negative";
    case runtime_config_status::shutdown_drain_timeout_negative:
        return "shutdown_drain_timeout_negative";
    case runtime_config_status::shutdown_connection_close_timeout_negative:
        return "shutdown_connection_close_timeout_negative";
    case runtime_config_status::shutdown_log_flush_timeout_negative:
        return "shutdown_log_flush_timeout_negative";
    case runtime_config_status::task_pool_local_cache_size_zero:
        return "task_pool_local_cache_size_zero";
    case runtime_config_status::task_pool_local_cache_size_too_large:
        return "task_pool_local_cache_size_too_large";
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
    case runtime_config_status::timer_kind_unsupported:
        return "timer_kind_unsupported";
    case runtime_config_status::reactor_event_capacity_zero:
        return "reactor_event_capacity_zero";
    case runtime_config_status::reactor_event_budget_zero:
        return "reactor_event_budget_zero";
    case runtime_config_status::log_queue_capacity_zero:
        return "log_queue_capacity_zero";
    case runtime_config_status::log_max_batch_records_zero:
        return "log_max_batch_records_zero";
    case runtime_config_status::log_record_pool_local_cache_size_zero:
        return "log_record_pool_local_cache_size_zero";
    case runtime_config_status::log_record_pool_local_cache_size_too_large:
        return "log_record_pool_local_cache_size_too_large";
    case runtime_config_status::log_record_pool_slab_object_count_zero:
        return "log_record_pool_slab_object_count_zero";
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
    case runtime_config_status::log_udp_backend_thread_not_io:
        return "log_udp_backend_thread_not_io";
    case runtime_config_status::log_tcp_backend_host_empty:
        return "log_tcp_backend_host_empty";
    case runtime_config_status::log_tcp_backend_port_zero:
        return "log_tcp_backend_port_zero";
    case runtime_config_status::log_tcp_backend_thread_not_found:
        return "log_tcp_backend_thread_not_found";
    case runtime_config_status::log_tcp_backend_thread_not_io:
        return "log_tcp_backend_thread_not_io";
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

struct runtime_thread_group_info {
    std::string name{"worker"};
    af::thread_kind kind{af::thread_kind::cpu};
    std::uint16_t begin{0};
    std::uint16_t count{0};
};

struct resolved_runtime_config {
    runtime_config config;
    std::vector<runtime_thread_info> threads;
    std::vector<std::uint16_t> all_threads;
    std::vector<std::uint16_t> io_threads;
    std::vector<std::uint16_t> cpu_threads;
    std::vector<runtime_thread_group_info> thread_groups;

    [[nodiscard]] std::uint16_t thread_count() const noexcept {
        return static_cast<std::uint16_t>(threads.size());
    }

    [[nodiscard]] std::uint16_t invalid_thread_index() const noexcept {
        return thread_count();
    }

    [[nodiscard]] bool valid_thread(std::uint16_t index) const noexcept {
        return index < thread_count();
    }

    [[nodiscard]] bool valid_thread(thread_ref thread) const noexcept {
        return valid_thread(thread.index);
    }

    [[nodiscard]] thread_ref thread_at(std::uint16_t index) const noexcept {
        return valid_thread(index) ? thread_ref(index) : thread_ref();
    }

    [[nodiscard]] af::thread_kind thread_kind_of(std::uint16_t index) const noexcept {
        if (!valid_thread(index)) {
            return af::thread_kind::cpu;
        }
        return threads[index].kind;
    }

    [[nodiscard]] af::thread_kind thread_kind_of(thread_ref thread) const noexcept {
        return thread_kind_of(thread.index);
    }

    [[nodiscard]] std::string_view thread_name(std::uint16_t index) const noexcept {
        if (!valid_thread(index)) {
            return "invalid";
        }
        return threads[index].name;
    }

    [[nodiscard]] std::string_view thread_name(thread_ref thread) const noexcept {
        return thread_name(thread.index);
    }

    [[nodiscard]] std::uint16_t thread_group_offset(std::uint16_t index) const noexcept {
        if (!valid_thread(index)) {
            return 0;
        }
        return threads[index].group_offset;
    }

    [[nodiscard]] std::uint16_t thread_group_offset(thread_ref thread) const noexcept {
        return thread_group_offset(thread.index);
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

    [[nodiscard]] thread_ref select_thread_ref(thread_selector selector) const noexcept {
        return thread_at(select_thread(selector));
    }

    [[nodiscard]] thread_group_ref io_thread_group() const noexcept {
        return thread_group_view(io_threads);
    }

    [[nodiscard]] thread_group_ref cpu_thread_group() const noexcept {
        return thread_group_view(cpu_threads);
    }

    [[nodiscard]] thread_group_ref thread_group(std::size_t group_index) const noexcept {
        if (group_index >= thread_groups.size()) {
            return {};
        }
        const runtime_thread_group_info &group = thread_groups[group_index];
        return thread_group_ref(all_threads.data() + group.begin, group.count, group.begin, true);
    }

    [[nodiscard]] thread_group_ref thread_group(std::string_view name) const noexcept {
        for (std::size_t i = 0; i < thread_groups.size(); ++i) {
            if (thread_groups[i].name == name) {
                return thread_group(i);
            }
        }
        return {};
    }

private:
    [[nodiscard]] static bool
    thread_indices_are_contiguous(const std::vector<std::uint16_t> &threads) noexcept {
        if (threads.empty()) {
            return false;
        }
        const std::uint16_t begin = threads.front();
        for (std::size_t index = 1; index < threads.size(); ++index) {
            if (threads[index] != static_cast<std::uint16_t>(begin + index)) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] static thread_group_ref
    thread_group_view(const std::vector<std::uint16_t> &threads) noexcept {
        if (thread_indices_are_contiguous(threads)) {
            return thread_group_ref(threads.data(), threads.size(), threads.front(), true);
        }
        return thread_group_ref(threads.data(), threads.size());
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
        if (group.priority.enabled && (group.priority.value < thread_priority_min ||
                                       group.priority.value > thread_priority_max)) {
            return runtime_config_error(runtime_config_status::thread_priority_value_out_of_range,
                                        index);
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
    resolved.all_threads.reserve(total_threads);
    resolved.io_threads.reserve(total_threads);
    resolved.cpu_threads.reserve(total_threads);
    resolved.thread_groups.reserve(groups.size());

    std::uint16_t index = 0;
    for (std::size_t group_index = 0; group_index < groups.size(); ++group_index) {
        const thread_group_config &group = groups[group_index];
        runtime_thread_group_info group_info;
        group_info.name = group.name.empty() ? "worker" : group.name;
        group_info.kind = group.kind;
        group_info.begin = index;
        group_info.count = static_cast<std::uint16_t>(group.count);
        resolved.thread_groups.push_back(std::move(group_info));
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
            resolved.all_threads.push_back(index);
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
validate_log_backend_io_thread(const resolved_runtime_config &resolved, thread_selector selector,
                               runtime_config_status not_found_status,
                               runtime_config_status not_io_status, std::size_t index) noexcept {
    const std::uint16_t thread = resolved.select_thread(selector);
    if (!resolved.valid_thread(thread)) {
        return runtime_config_error(not_found_status, index);
    }
    if (resolved.thread_kind_of(thread) != af::thread_kind::io) {
        return runtime_config_error(not_io_status, index);
    }
    return {};
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
        if (const auto validation = validate_log_backend_io_thread(
                resolved, udp->io_thread, runtime_config_status::log_udp_backend_thread_not_found,
                runtime_config_status::log_udp_backend_thread_not_io, index);
            !validation) {
            return validation;
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
    if (const auto validation = validate_log_backend_io_thread(
            resolved, tcp->io_thread, runtime_config_status::log_tcp_backend_thread_not_found,
            runtime_config_status::log_tcp_backend_thread_not_io, index);
        !validation) {
        return validation;
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
    if (config.scheduler.service_task_budget == 0) {
        return runtime_config_error(runtime_config_status::scheduler_service_task_budget_zero);
    }
    if (config.scheduler.max_task_run_slice.count() < 0) {
        return runtime_config_error(runtime_config_status::scheduler_max_task_run_slice_negative);
    }
    if (config.task_pool.local_cache_size == 0) {
        return runtime_config_error(runtime_config_status::task_pool_local_cache_size_zero);
    }
    if (config.task_pool.local_cache_size > runtime_task_pool_max_local_cache_size) {
        return runtime_config_error(runtime_config_status::task_pool_local_cache_size_too_large);
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
    switch (config.timer.kind) {
    case timer_kind::min_heap:
    case timer_kind::hierarchical_wheel:
        break;
    default:
        return runtime_config_error(runtime_config_status::timer_kind_unsupported);
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
    if (config.logger.record_pool.local_cache_size == 0) {
        return runtime_config_error(runtime_config_status::log_record_pool_local_cache_size_zero);
    }
    if (config.logger.record_pool.local_cache_size > async_log_record_pool_max_local_cache_size) {
        return runtime_config_error(
            runtime_config_status::log_record_pool_local_cache_size_too_large);
    }
    if (config.logger.record_pool.slab_object_count == 0) {
        return runtime_config_error(runtime_config_status::log_record_pool_slab_object_count_zero);
    }
    if (!resolved.valid_thread(resolved.select_thread(config.logger.consumer_thread))) {
        return runtime_config_error(runtime_config_status::log_consumer_thread_not_found);
    }
    if (config.shutdown.drain_timeout.count() < 0) {
        return runtime_config_error(runtime_config_status::shutdown_drain_timeout_negative);
    }
    if (config.shutdown.connection_close_timeout.count() < 0) {
        return runtime_config_error(
            runtime_config_status::shutdown_connection_close_timeout_negative);
    }
    if (config.shutdown.log_flush_timeout.count() < 0) {
        return runtime_config_error(runtime_config_status::shutdown_log_flush_timeout_negative);
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
    if (resolution.resolved.config.task_pool.local_cache_size != 0 &&
        resolution.resolved.config.task_pool.local_cache_size <=
            runtime_task_pool_max_local_cache_size) {
        resolution.resolved.config.task_pool.local_cache_size =
            normalize_runtime_task_pool_local_cache_size(
                resolution.resolved.config.task_pool.local_cache_size);
    }
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
