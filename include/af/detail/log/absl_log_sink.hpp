#pragma once

#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
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
#include "af/thread_kind.hpp"

namespace af {

namespace detail {

[[nodiscard]] inline std::size_t user_log_field_offset(std::string_view message,
                                                       std::string_view user_message) noexcept {
    const auto message_begin = reinterpret_cast<std::uintptr_t>(message.data());
    const auto user_begin = reinterpret_cast<std::uintptr_t>(user_message.data());
    if (message_begin != 0U && user_begin >= message_begin) {
        const auto offset = static_cast<std::size_t>(user_begin - message_begin);
        if (offset <= message.size() && user_message.size() <= message.size() - offset &&
            message.substr(offset, user_message.size()) == user_message) {
            return offset;
        }
    }

    const bool user_message_is_suffix =
        user_message.size() <= message.size() &&
        message.substr(message.size() - user_message.size()) == user_message;
    if (user_message_is_suffix) {
        return message.size() - user_message.size();
    }
    return std::string_view::npos;
}

template <typename TaskId>
[[nodiscard]] std::string task_id_tagged_user_log_message(std::string_view message,
                                                          std::string_view user_message,
                                                          TaskId task_id) {
    const std::size_t prefix_size = user_log_field_offset(message, user_message);
    if (prefix_size == std::string_view::npos) [[unlikely]] {
        return std::string(message);
    }

    const std::string_view prefix = message.substr(0, prefix_size);
    std::array<char, 32> digits{};
    const auto converted = std::to_chars(digits.data(), digits.data() + digits.size(), task_id);
    AF_ASSERT(converted.ec == std::errc{});
    const std::string_view task_digits(digits.data(),
                                       static_cast<std::size_t>(converted.ptr - digits.data()));

    std::size_t line_count = 0;
    for (const char character : user_message) {
        line_count += character == '\n' ? 1U : 0U;
    }
    if (user_message.empty() || user_message.back() != '\n') {
        ++line_count;
    }

    std::string tagged;
    tagged.reserve(message.size() + (prefix.size() + task_digits.size() + 8U) * line_count);

    std::size_t line_begin = 0;
    while (line_begin < user_message.size()) {
        const std::size_t newline = user_message.find('\n', line_begin);
        const std::size_t line_end =
            newline == std::string_view::npos ? user_message.size() : newline + 1U;
        tagged.append(prefix.data(), prefix.size());
        tagged.append("[task=");
        tagged.append(task_digits.data(), task_digits.size());
        tagged.append("] ");
        tagged.append(user_message.data() + line_begin, line_end - line_begin);
        line_begin = line_end;
    }
    if (user_message.empty()) {
        tagged.append(prefix.data(), prefix.size());
        tagged.append("[task=");
        tagged.append(task_digits.data(), task_digits.size());
        tagged.append("] ");
    }
    return tagged;
}

} // namespace detail

namespace detail {

[[nodiscard]] constexpr bool async_log_consumer_prefers_io_thread_kind(ThreadKind kind) noexcept {
    switch (kind) {
    case ThreadKind::Io:
    case ThreadKind::IoUring:
    case ThreadKind::Epoll:
    case ThreadKind::Kqueue:
        return true;
    case ThreadKind::Worker:
    case ThreadKind::Log:
        return false;
    }
    return false;
}

template <typename RuntimeT>
[[nodiscard]] constexpr typename RuntimeT::Thread
select_default_async_log_consumer_thread() noexcept {
    for (std::uint32_t i = 0; i < RuntimeT::thread_count; ++i) {
        const auto thread = RuntimeT::thread_from_index(static_cast<std::uint16_t>(i));
        if (RuntimeT::thread_kind(thread) == ThreadKind::Log) {
            return thread;
        }
    }

    for (std::uint32_t i = 0; i < RuntimeT::thread_count; ++i) {
        const auto thread = RuntimeT::thread_from_index(static_cast<std::uint16_t>(i));
        if (async_log_consumer_prefers_io_thread_kind(RuntimeT::thread_kind(thread))) {
            return thread;
        }
    }

    return RuntimeT::thread_from_index(0);
}

} // namespace detail

template <typename RuntimeT>
[[nodiscard]] constexpr typename RuntimeT::Thread default_async_log_consumer_thread() noexcept {
    return detail::select_default_async_log_consumer_thread<RuntimeT>();
}

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
                const std::string tagged_message = detail::task_id_tagged_user_log_message(
                    message, entry.text_message_with_newline(), task_id);
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
    AsyncLogHandle(std::shared_ptr<AsyncLogger> logger, std::unique_ptr<absl::LogSink> sink,
                   std::unique_ptr<detail::AsyncLogConsumerController> consumer_controller)
        : logger_(std::move(logger)), sink_(std::move(sink)),
          consumer_controller_(std::move(consumer_controller)) {
        AF_ASSERT(sink_ != nullptr);
        AF_ASSERT(consumer_controller_ != nullptr);
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
        consumer_controller_->shutdown();
    }

private:
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
                                                     default_async_log_consumer_thread<RuntimeT>());
}

} // namespace af
