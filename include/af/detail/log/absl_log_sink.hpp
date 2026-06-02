#pragma once

#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

#include "absl/base/log_severity.h"
#include "absl/log/initialize.h"
#include "absl/log/internal/globals.h"
#include "absl/log/log_entry.h"
#include "absl/log/log_sink.h"
#include "absl/log/log_sink_registry.h"

#include "af/detail/config.hpp"
#include "af/detail/log/async_logger.hpp"
#include "af/detail/log/runtime_async_log_consumer.hpp"

namespace af {

namespace detail {

template <typename TaskId>
[[nodiscard]] std::string task_id_prefixed_log_message(std::string_view message, TaskId task_id) {
    std::array<char, 32> digits{};
    const auto converted = std::to_chars(digits.data(), digits.data() + digits.size(), task_id);
    AF_ASSERT(converted.ec == std::errc{});

    std::string tagged;
    tagged.reserve(message.size() + static_cast<std::size_t>(converted.ptr - digits.data()) + 8U);
    tagged.append("[task=");
    tagged.append(digits.data(), converted.ptr);
    tagged.append("] ");
    tagged.append(message.data(), message.size());
    return tagged;
}

} // namespace detail

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
        bool accepted = false;
        if (RuntimeT::is_runtime_thread()) {
            const auto task_id = RuntimeT::current_task_id();
            if (task_id == RuntimeT::invalid_task_id) {
                accepted =
                    logger_->try_log_from_runtime_thread(RuntimeT::current_thread_index(), message);
            } else {
                const std::string tagged_message =
                    detail::task_id_prefixed_log_message(message, task_id);
                accepted = logger_->try_log_from_runtime_thread(RuntimeT::current_thread_index(),
                                                                tagged_message);
            }
        } else {
            accepted = logger_->try_log(message);
        }
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
    AsyncLogHandle(
        std::shared_ptr<AsyncLogger> logger, std::unique_ptr<absl::LogSink> sink,
        std::unique_ptr<detail::AsyncLogConsumerController> consumer_controller = nullptr)
        : logger_(std::move(logger)), sink_(std::move(sink)),
          consumer_controller_(std::move(consumer_controller)) {
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
        if (consumer_controller_ != nullptr) {
            consumer_controller_->shutdown();
        } else {
            logger_->shutdown();
        }
    }

private:
    friend std::unique_ptr<AsyncLogHandle> start_async_logging(AsyncLogConfig config);

    template <typename RuntimeT>
    friend std::unique_ptr<AsyncLogHandle> start_async_logging_for_runtime(AsyncLogConfig config);

    template <typename RuntimeT>
    friend std::unique_ptr<AsyncLogHandle>
    start_async_logging_for_runtime(AsyncLogConfig config,
                                    typename RuntimeT::Thread consumer_thread);

    void register_sink() {
        absl::AddLogSink(sink_.get());
        registered_.store(true, std::memory_order_release);
    }

    std::shared_ptr<AsyncLogger> logger_;
    std::unique_ptr<absl::LogSink> sink_;
    std::unique_ptr<detail::AsyncLogConsumerController> consumer_controller_;
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
start_async_logging_for_runtime(AsyncLogConfig config, typename RuntimeT::Thread consumer_thread) {
    const bool initialize_absl_log = config.initialize_absl_log;
    if (config.runtime_thread_count == 0U) {
        config.runtime_thread_count = RuntimeT::thread_count;
    }
    const std::size_t max_consumer_batches_per_run = config.max_consumer_batches_per_run;

    auto logger = std::make_shared<AsyncLogger>(std::move(config));
    auto consumer_controller =
        std::make_unique<detail::RuntimeAsyncLogConsumerController<RuntimeT>>(
            logger, consumer_thread, max_consumer_batches_per_run);
    if (!consumer_controller->start()) {
        throw std::runtime_error("failed to start runtime async log consumer");
    }
    if (initialize_absl_log) {
        initialize_absl_log_once();
    }

    auto handle = std::make_unique<AsyncLogHandle>(
        logger, std::make_unique<RuntimeAbslAsyncLogSink<RuntimeT>>(logger),
        std::move(consumer_controller));
    handle->register_sink();
    return handle;
}

template <typename RuntimeT>
[[nodiscard]] inline std::unique_ptr<AsyncLogHandle>
start_async_logging_for_runtime(AsyncLogConfig config) {
    return start_async_logging_for_runtime<RuntimeT>(std::move(config),
                                                     RuntimeT::thread_from_index(0));
}

} // namespace af
