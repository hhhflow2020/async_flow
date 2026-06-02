#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>

#include "absl/base/log_severity.h"
#include "absl/log/initialize.h"
#include "absl/log/internal/globals.h"
#include "absl/log/log_entry.h"
#include "absl/log/log_sink.h"
#include "absl/log/log_sink_registry.h"

#include "af/detail/log/async_logger.hpp"

namespace af {

class AbslAsyncLogSink final : public absl::LogSink {
public:
    explicit AbslAsyncLogSink(std::shared_ptr<AsyncLogger> logger) : logger_(std::move(logger)) {}

    void Send(const absl::LogEntry &entry) override {
        const bool accepted = logger_->try_log(entry.text_message_with_prefix_and_newline());
        if (accepted && entry.log_severity() == absl::LogSeverity::kFatal) {
            static_cast<void>(logger_->flush(logger_->fatal_flush_timeout()));
        }
    }

    void Flush() override {
        static_cast<void>(logger_->flush(std::chrono::seconds(5)));
    }

private:
    std::shared_ptr<AsyncLogger> logger_;
};

class AsyncLogHandle {
public:
    explicit AsyncLogHandle(std::shared_ptr<AsyncLogger> logger)
        : logger_(std::move(logger)), sink_(logger_) {}

    AsyncLogHandle(const AsyncLogHandle &) = delete;
    AsyncLogHandle &operator=(const AsyncLogHandle &) = delete;

    ~AsyncLogHandle() {
        stop();
    }

    [[nodiscard]] bool flush(std::chrono::milliseconds timeout = std::chrono::seconds(5)) noexcept {
        return logger_->flush(timeout);
    }

    [[nodiscard]] AsyncLogStats stats() const noexcept {
        return logger_->stats();
    }

    void stop() noexcept {
        bool expected = true;
        if (!registered_.compare_exchange_strong(expected, false, std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
            return;
        }
        absl::RemoveLogSink(&sink_);
        logger_->shutdown();
    }

private:
    friend std::unique_ptr<AsyncLogHandle> start_async_logging(AsyncLogConfig config);

    void register_sink() {
        absl::AddLogSink(&sink_);
        registered_.store(true, std::memory_order_release);
    }

    std::shared_ptr<AsyncLogger> logger_;
    AbslAsyncLogSink sink_;
    std::atomic<bool> registered_{false};
};

inline void initialize_absl_log_once() {
    if (absl::log_internal::IsInitialized()) {
        return;
    }

    static std::once_flag once;
    std::call_once(once, [] {
        if (!absl::log_internal::IsInitialized()) {
            absl::InitializeLog();
        }
    });
}

[[nodiscard]] inline std::unique_ptr<AsyncLogHandle> start_async_logging(AsyncLogConfig config) {
    const bool initialize_absl_log = config.initialize_absl_log;
    auto logger = std::make_shared<AsyncLogger>(std::move(config));
    logger->start();
    if (initialize_absl_log) {
        initialize_absl_log_once();
    }

    auto handle = std::make_unique<AsyncLogHandle>(logger);
    handle->register_sink();
    return handle;
}

} // namespace af
