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
#include <variant>

#include "absl/base/log_severity.h"
#include "absl/log/initialize.h"
#include "absl/log/internal/globals.h"
#include "absl/log/log_entry.h"
#include "absl/log/log_sink.h"
#include "absl/log/log_sink_registry.h"

#include "af/detail/config.hpp"
#include "af/detail/log/async_logger.hpp"
#include "af/detail/log/file_log_backend.hpp"
#include "af/detail/log/network_log_backend.hpp"
#include "af/detail/log/runtime_async_log_consumer.hpp"
#include "af/detail/log/runtime_instance_async_log_consumer.hpp"
#include "af/thread_kind.hpp"

namespace af {

namespace detail {

[[nodiscard]] inline std::chrono::milliseconds
async_log_flush_poll_interval_from_batch_delay(std::chrono::microseconds delay) noexcept {
    if (delay <= std::chrono::microseconds::zero()) {
        return std::chrono::milliseconds(1);
    }

    auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(delay);
    if (milliseconds == std::chrono::milliseconds::zero() ||
        std::chrono::duration_cast<std::chrono::microseconds>(milliseconds) < delay) {
        ++milliseconds;
    }
    return milliseconds;
}

inline void append_async_log_backend(AsyncLogConfig &target,
                                     const file_log_backend_config &source) {
    FileLogBackendConfig backend_config;
    backend_config.path = source.path;
    backend_config.append = source.append;
    backend_config.fsync_on_flush = source.fsync_on_flush;
    backend_config.write_batch_iov = source.write_batch_iov;
    target.backends.push_back(make_file_log_backend(std::move(backend_config)));
}

inline void append_async_log_backend(AsyncLogConfig &target, const udp_log_backend_config &source) {
    UdpLogBackendConfig backend_config;
    backend_config.host = source.host;
    backend_config.port = source.port;
    backend_config.max_datagram_size = source.max_datagram_size;
    target.backends.push_back(std::make_unique<UdpLogBackend>(std::move(backend_config)));
}

inline void append_async_log_backend(AsyncLogConfig &target, const tcp_log_backend_config &source) {
    TcpLogBackendConfig backend_config;
    backend_config.host = source.host;
    backend_config.port = source.port;
    backend_config.reconnect_interval = source.reconnect_interval;
    target.backends.push_back(std::make_unique<TcpLogBackend>(std::move(backend_config)));
}

inline void append_async_log_backend(AsyncLogConfig &target, const log_backend_config &source) {
    if (const auto *file = std::get_if<file_log_backend_config>(&source)) {
        append_async_log_backend(target, *file);
        return;
    }
    if (const auto *udp = std::get_if<udp_log_backend_config>(&source)) {
        append_async_log_backend(target, *udp);
        return;
    }
    if (const auto *tcp = std::get_if<tcp_log_backend_config>(&source)) {
        append_async_log_backend(target, *tcp);
    }
}

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

[[nodiscard]] inline AsyncLogConfig make_async_log_config(
    const log_config &source,
    std::size_t runtime_thread_count = AsyncLogConfig::auto_runtime_thread_count) {
    AsyncLogConfig target;
    if (source.ordering == log_ordering::relaxed) {
        const std::size_t resolved_thread_count =
            source.runtime_thread_count == 0U ? runtime_thread_count : source.runtime_thread_count;
        target.use_relaxed(resolved_thread_count, source.queue_shard_count);
    } else {
        target.use_ordered(source.queue_shard_count);
    }

    target.queue_capacity = source.queue_capacity;
    target.max_batch_size = source.max_batch_records;
    target.overflow_policy = source.overflow;
    target.flush_poll_interval =
        detail::async_log_flush_poll_interval_from_batch_delay(source.max_batch_delay);
    target.backends.reserve(source.backends.size());
    for (const log_backend_config &backend : source.backends) {
        detail::append_async_log_backend(target, backend);
    }
    return target;
}

[[nodiscard]] inline AsyncLogConfig make_async_log_config(runtime &owner) {
    return make_async_log_config(owner.config().logger, owner.thread_count());
}

namespace detail {

[[nodiscard]] constexpr bool
async_log_consumer_prefers_io_thread_kind(af::thread_kind kind) noexcept {
    return kind == af::thread_kind::io;
}

template <typename RuntimeT>
[[nodiscard]] constexpr typename RuntimeT::Thread
select_default_async_log_consumer_thread() noexcept {
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

[[nodiscard]] inline runtime::thread_index
default_async_log_consumer_thread(runtime &owner) noexcept {
    for (runtime::thread_index i = 0; i < owner.thread_count(); ++i) {
        if (detail::async_log_consumer_prefers_io_thread_kind(owner.thread_kind_of(i))) {
            return i;
        }
    }
    return owner.valid_thread(0U) ? 0U : owner.invalid_thread_index();
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

class RuntimeInstanceAbslAsyncLogSink final : public absl::LogSink {
public:
    RuntimeInstanceAbslAsyncLogSink(runtime &owner, std::shared_ptr<AsyncLogger> logger)
        : owner_(owner), logger_(std::move(logger)) {}

    void Send(const absl::LogEntry &entry) override {
        const auto message = entry.text_message_with_prefix_and_newline();
        bool accepted = false;
        if (runtime::current() == &owner_) {
            const auto task_id = runtime::current_task_id();
            if (task_id == runtime::invalid_task_id) {
                accepted =
                    logger_->try_log_from_runtime_thread(runtime::current_thread_index(), message);
            } else {
                const std::string tagged_message = detail::task_id_tagged_user_log_message(
                    message, entry.text_message_with_newline(), task_id);
                accepted = logger_->try_log_from_runtime_thread(runtime::current_thread_index(),
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
    runtime &owner_;
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

    friend std::unique_ptr<AsyncLogHandle> start_async_logging_for_runtime(runtime &owner,
                                                                           AsyncLogConfig config);

    friend std::unique_ptr<AsyncLogHandle>
    start_async_logging_for_runtime(runtime &owner, AsyncLogConfig config,
                                    runtime::thread_index consumer_thread);

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
    initialize_absl_log_once();

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

[[nodiscard]] inline std::unique_ptr<AsyncLogHandle>
start_async_logging_for_runtime(runtime &owner, AsyncLogConfig config,
                                runtime::thread_index consumer_thread) {
    if (config.runtime_thread_count == 0U) {
        config.runtime_thread_count = owner.thread_count();
    }
    const std::size_t max_consumer_batches_per_run = config.max_consumer_batches_per_run;

    auto logger = std::make_shared<AsyncLogger>(std::move(config));
    auto consumer_controller = std::make_unique<detail::RuntimeInstanceAsyncLogConsumerController>(
        owner, logger, consumer_thread, max_consumer_batches_per_run);
    if (!consumer_controller->start()) {
        throw std::runtime_error("failed to start runtime async log consumer");
    }
    initialize_absl_log_once();

    auto handle = std::make_unique<AsyncLogHandle>(
        logger, std::make_unique<RuntimeInstanceAbslAsyncLogSink>(owner, logger),
        std::move(consumer_controller));
    handle->register_sink();
    return handle;
}

[[nodiscard]] inline std::unique_ptr<AsyncLogHandle>
start_async_logging_for_runtime(runtime &owner, AsyncLogConfig config) {
    return start_async_logging_for_runtime(owner, std::move(config),
                                           default_async_log_consumer_thread(owner));
}

[[nodiscard]] inline std::unique_ptr<AsyncLogHandle>
start_async_logging_for_runtime(runtime &owner) {
    const runtime::thread_index consumer_thread =
        owner.select_thread(owner.config().logger.consumer_thread);
    return start_async_logging_for_runtime(owner, make_async_log_config(owner), consumer_thread);
}

} // namespace af
