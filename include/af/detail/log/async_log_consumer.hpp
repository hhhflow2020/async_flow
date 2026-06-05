#pragma once

#include <chrono>

namespace af::detail {

class AsyncLogConsumerWakeTarget {
public:
    AsyncLogConsumerWakeTarget() = default;
    AsyncLogConsumerWakeTarget(const AsyncLogConsumerWakeTarget &) = delete;
    AsyncLogConsumerWakeTarget &operator=(const AsyncLogConsumerWakeTarget &) = delete;
    virtual ~AsyncLogConsumerWakeTarget() = default;

    [[nodiscard]] virtual bool wake_async_log_consumer() noexcept = 0;
};

class AsyncLogConsumerController {
public:
    AsyncLogConsumerController() = default;
    AsyncLogConsumerController(const AsyncLogConsumerController &) = delete;
    AsyncLogConsumerController &operator=(const AsyncLogConsumerController &) = delete;
    virtual ~AsyncLogConsumerController() = default;

    void shutdown() noexcept {
        shutdown(std::chrono::seconds(5));
    }

    virtual void shutdown(std::chrono::milliseconds timeout) noexcept = 0;
};

class RuntimeInstanceAsyncLogConsumerController;

} // namespace af::detail
