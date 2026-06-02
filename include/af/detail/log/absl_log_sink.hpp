#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string_view>

#include "absl/base/log_severity.h"
#include "absl/log/initialize.h"
#include "absl/log/internal/globals.h"
#include "absl/log/log_entry.h"
#include "absl/log/log_sink.h"
#include "absl/log/log_sink_registry.h"

#include "af/detail/config.hpp"
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

template <typename RuntimeT> class RuntimeAbslAsyncLogSink final : public absl::LogSink {
public:
    explicit RuntimeAbslAsyncLogSink(std::shared_ptr<AsyncLogger> logger)
        : logger_(std::move(logger)) {}

    void Send(const absl::LogEntry &entry) override {
        const auto message = entry.text_message_with_prefix_and_newline();
        const bool accepted =
            RuntimeT::is_runtime_thread()
                ? logger_->try_log_from_runtime_thread(RuntimeT::current_thread_index(), message)
                : logger_->try_log(message);
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
    AsyncLogHandle(std::shared_ptr<AsyncLogger> logger, std::unique_ptr<absl::LogSink> sink)
        : logger_(std::move(logger)), sink_(std::move(sink)) {
        AF_ASSERT(sink_ != nullptr);
    }

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
        absl::RemoveLogSink(sink_.get());
        logger_->shutdown();
    }

private:
    friend std::unique_ptr<AsyncLogHandle> start_async_logging(AsyncLogConfig config);

    template <typename RuntimeT>
    friend std::unique_ptr<AsyncLogHandle> start_async_logging_for_runtime(AsyncLogConfig config);

    void register_sink() {
        absl::AddLogSink(sink_.get());
        registered_.store(true, std::memory_order_release);
    }

    std::shared_ptr<AsyncLogger> logger_;
    std::unique_ptr<absl::LogSink> sink_;
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

    auto handle =
        std::make_unique<AsyncLogHandle>(logger, std::make_unique<AbslAsyncLogSink>(logger));
    handle->register_sink();
    return handle;
}

template <typename RuntimeT>
[[nodiscard]] inline std::unique_ptr<AsyncLogHandle>
start_async_logging_for_runtime(AsyncLogConfig config) {
    const bool initialize_absl_log = config.initialize_absl_log;
    if (config.runtime_thread_count == 0U) {
        config.runtime_thread_count = RuntimeT::thread_count;
    }

    auto logger = std::make_shared<AsyncLogger>(std::move(config));
    logger->start();
    if (initialize_absl_log) {
        initialize_absl_log_once();
    }

    auto handle = std::make_unique<AsyncLogHandle>(
        logger, std::make_unique<RuntimeAbslAsyncLogSink<RuntimeT>>(logger));
    handle->register_sink();
    return handle;
}

} // namespace af
