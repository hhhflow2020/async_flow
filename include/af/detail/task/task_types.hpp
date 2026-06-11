#pragma once

#include <cstdint>

enum class task_result : std::uint8_t {
    done,
    pending,
    again,
    failed,
    cancelled,
};

enum class shutdown_policy : std::uint8_t {
    wait_for_tasks,
    stop_immediately,
};

enum class task_state : std::uint8_t {
    created,
    queued,
    timer_arming,
    timer_pending,
    starting,
    running,
    pending,
    done,
};
