#pragma once

#include <chrono>

namespace af::detail {

class async_log_consumer_wake_target {
public:
    async_log_consumer_wake_target() = default;
    async_log_consumer_wake_target(const async_log_consumer_wake_target &) = delete;
    async_log_consumer_wake_target &operator=(const async_log_consumer_wake_target &) = delete;
    virtual ~async_log_consumer_wake_target() = default;

    [[nodiscard]] virtual bool wake_async_log_consumer() noexcept = 0;
};

class async_log_consumer_controller {
public:
    async_log_consumer_controller() = default;
    async_log_consumer_controller(const async_log_consumer_controller &) = delete;
    async_log_consumer_controller &operator=(const async_log_consumer_controller &) = delete;
    virtual ~async_log_consumer_controller() = default;

    void shutdown() noexcept {
        shutdown(std::chrono::seconds(5));
    }

    virtual void shutdown(std::chrono::milliseconds timeout) noexcept = 0;
};

class runtime_instance_async_log_consumer_controller;

} // namespace af::detail
