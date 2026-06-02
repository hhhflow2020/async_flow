#pragma once

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/log_backend.hpp"
#include "af/detail/log/network_log_backend.hpp"
#include "af/detail/log/runtime_log_backend_common.hpp"
#include "af/io_socket.hpp"
#include "af/task.hpp"

#if !defined(_WIN32)
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace af {

template <typename RuntimeT> struct RuntimeTcpLogBackendConfig {
    typename RuntimeT::Thread thread{};
    std::string host;
    std::uint16_t port{0};
    std::chrono::milliseconds reconnect_interval{std::chrono::milliseconds(500)};
    std::size_t batch_queue_capacity{1024};
    std::size_t max_batch_records{64};
    std::size_t max_batches_per_run{64};
};

struct RuntimeTcpLogBackendStats {
    std::uint64_t queued_records{0};
    std::uint64_t sent_records{0};
    std::uint64_t dropped_records{0};
    int last_error{0};
    int last_error_stage{0};
};

namespace detail {

class RuntimeTcpLogBatch {
public:
    explicit RuntimeTcpLogBatch(std::size_t max_records) {
        payload.reserve(max_records * default_log_inline_message_bytes);
    }

    void reset() noexcept {
        record_count = 0;
        payload.clear();
    }

    [[nodiscard]] bool append(std::string_view message, std::size_t max_records) {
        if (message.empty()) {
            return true;
        }
        if (record_count >= max_records) {
            return false;
        }

        payload.insert(payload.end(), message.data(), message.data() + message.size());
        ++record_count;
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return record_count == 0U;
    }

    std::uint32_t record_count{0};
    std::vector<char> payload;
};

template <typename RuntimeT>
class RuntimeTcpLogState : public RuntimeLogQueueState<RuntimeTcpLogBatch> {
public:
    using Thread = typename RuntimeT::Thread;
    using Batch = RuntimeTcpLogBatch;
    using QueueState = RuntimeLogQueueState<Batch>;

    explicit RuntimeTcpLogState(RuntimeTcpLogBackendConfig<RuntimeT> config)
        : QueueState(config.batch_queue_capacity,
                     normalize_max_batch_records(config.max_batch_records),
                     config.max_batches_per_run),
          thread(config.thread), reconnect_interval(config.reconnect_interval), sent_records(0),
          last_error(0), last_error_stage(0), io_waiting(false) {
        resolve(config.host, config.port);
    }

    RuntimeTcpLogState(const RuntimeTcpLogState &) = delete;
    RuntimeTcpLogState &operator=(const RuntimeTcpLogState &) = delete;

    ~RuntimeTcpLogState() {
#if !defined(_WIN32)
        close_socket();
#endif
    }

    [[nodiscard]] RuntimeTcpLogBackendStats stats() const noexcept {
        return RuntimeTcpLogBackendStats{
            queued_records.load(std::memory_order_acquire),
            sent_records.load(std::memory_order_acquire),
            dropped_records.load(std::memory_order_acquire),
            last_error.load(std::memory_order_acquire),
            last_error_stage.load(std::memory_order_acquire),
        };
    }

#if !defined(_WIN32)
    enum class ConnectResult : std::uint8_t {
        Connected,
        Pending,
        Failed,
    };

    [[nodiscard]] bool open_socket() noexcept {
        if (fd >= 0) {
            return true;
        }
        if (!resolved) {
            last_error.store(EDESTADDRREQ, std::memory_order_release);
            last_error_stage.store(1, std::memory_order_release);
            return false;
        }

        const auto now = std::chrono::steady_clock::now();
        if (now < next_connect_time) {
            last_error.store(EAGAIN, std::memory_order_release);
            last_error_stage.store(2, std::memory_order_release);
            return false;
        }
        next_connect_time = now + reconnect_interval;

        int candidate = ::socket(address.ss_family, SOCK_STREAM, protocol);
        if (candidate < 0) {
            last_error.store(errno == 0 ? EIO : errno, std::memory_order_release);
            last_error_stage.store(3, std::memory_order_release);
            return false;
        }

        set_log_socket_nonblocking(candidate);
        fd = candidate;
        return true;
    }

    void close_socket() noexcept {
        close_log_socket(fd);
    }
#endif

    Thread thread;
    const std::chrono::milliseconds reconnect_interval;
    CacheLineAtomic<std::uint64_t> sent_records;
    CacheLineAtomic<int> last_error;
    CacheLineAtomic<int> last_error_stage;
    CacheLineAtomic<bool> io_waiting;

#if !defined(_WIN32)
    sockaddr_storage address{};
    socklen_t address_size{0};
    int protocol{0};
    int fd{-1};
    bool resolved{false};
    std::chrono::steady_clock::time_point next_connect_time{};
#endif

private:
    [[nodiscard]] static std::size_t normalize_max_batch_records(std::size_t requested) noexcept {
        constexpr std::size_t max_supported_records = 1024;
        if (requested == 0U) {
            return 1U;
        }
        return std::min(requested, max_supported_records);
    }

    void resolve(const std::string &host, std::uint16_t port) noexcept {
#if defined(_WIN32)
        static_cast<void>(host);
        static_cast<void>(port);
#else
        addrinfo hints{};
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_family = AF_UNSPEC;
        const std::string port_value = log_port_string(port);

        addrinfo *result = nullptr;
        if (::getaddrinfo(host.c_str(), port_value.c_str(), &hints, &result) != 0) {
            return;
        }

        for (addrinfo *entry = result; entry != nullptr; entry = entry->ai_next) {
            if (entry->ai_addrlen > sizeof(address)) {
                continue;
            }
            std::memcpy(&address, entry->ai_addr, entry->ai_addrlen);
            address_size = static_cast<socklen_t>(entry->ai_addrlen);
            protocol = entry->ai_protocol;
            resolved = true;
            break;
        }

        ::freeaddrinfo(result);
#endif
    }
};

template <typename RuntimeT> class RuntimeTcpLogSenderTask final : public RuntimeT::Task {
public:
    using TaskBase = typename RuntimeT::Task;
    using State = RuntimeTcpLogState<RuntimeT>;
    using Batch = RuntimeTcpLogBatch;

    explicit RuntimeTcpLogSenderTask(typename TaskBase::FactoryToken token) : TaskBase(token) {}

    [[nodiscard]] bool start(State *state) noexcept {
        state_ = state;
        return this->schedule(state_->thread);
    }

    [[nodiscard]] bool wake() noexcept {
        return state_ != nullptr && this->schedule(state_->thread);
    }

private:
    TaskResult run() override {
        if (state_ == nullptr) [[unlikely]] {
            return this->done();
        }

        state_->wake_queued.store(false, std::memory_order_release);
        if (state_->stopping.load(std::memory_order_acquire)) {
            drop_current();
            drop_ready_batches();
            state_->finished.store(true, std::memory_order_release);
            state_->finished.notify_all();
            return this->done();
        }
#if !defined(_WIN32)
        if (state_->io_waiting.load(std::memory_order_acquire) && !io_wait_ready()) {
            return this->pending();
        }
#endif

        std::size_t drained_batches = 0;
        for (;;) {
            if (current_ == nullptr) {
                current_ = state_->ready_batches.try_pop();
                current_byte_ = 0;
                if (current_ == nullptr) {
                    if (state_->stopping.load(std::memory_order_acquire)) {
                        state_->finished.store(true, std::memory_order_release);
                        state_->finished.notify_all();
                        return this->done();
                    }
                    return this->pending();
                }
            }

            const SendResult result = send_current();
            if (result == SendResult::Pending) {
                return this->pending();
            }

            state_->complete_batch(current_);
            current_ = nullptr;
            ++drained_batches;
            if (drained_batches >= state_->max_batches_per_run) {
                return this->again();
            }
        }
    }

    enum class SendResult : std::uint8_t {
        Complete,
        Pending,
    };

    [[nodiscard]] SendResult send_current() noexcept {
#if defined(_WIN32)
        state_->dropped_records.fetch_add(current_->record_count, std::memory_order_relaxed);
        current_byte_ = current_->payload.size();
        return SendResult::Complete;
#else
        const typename State::ConnectResult connect_result = ensure_connected();
        if (connect_result == State::ConnectResult::Pending) {
            return SendResult::Pending;
        }
        if (connect_result == State::ConnectResult::Failed) {
            state_->dropped_records.fetch_add(current_->record_count, std::memory_order_relaxed);
            current_byte_ = current_->payload.size();
            return SendResult::Complete;
        }

        while (current_byte_ < current_->payload.size()) {
            const char *data = current_->payload.data() + current_byte_;
            const std::size_t size = current_->payload.size() - current_byte_;
            state_->io_waiting.store(true, std::memory_order_release);
            const IoStatus status =
                io_send_some(*this, state_->thread, state_->fd, data, size, send_state_);
            if (status.pending()) {
                return SendResult::Pending;
            }

            state_->io_waiting.store(false, std::memory_order_release);
            send_state_.reset();
            if (status.ready() && status.bytes > 0U) {
                current_byte_ += status.bytes;
                continue;
            }
            if (status.ready()) {
                close_socket();
                state_->last_error.store(EPIPE, std::memory_order_release);
                state_->last_error_stage.store(5, std::memory_order_release);
                drop_current_records();
                return SendResult::Complete;
            }

            close_socket();
            state_->last_error.store(status.failed() ? status.error : ECONNRESET,
                                     std::memory_order_release);
            state_->last_error_stage.store(6, std::memory_order_release);
            drop_current_records();
            return SendResult::Complete;
        }

        state_->sent_records.fetch_add(current_->record_count, std::memory_order_relaxed);
        return SendResult::Complete;
#endif
    }

#if !defined(_WIN32)
    [[nodiscard]] bool io_wait_ready() const noexcept {
        return io_wait_result_ready(connect_state_) || io_wait_result_ready(send_state_);
    }

    [[nodiscard]] typename State::ConnectResult ensure_connected() noexcept {
        if (connected_) {
            return State::ConnectResult::Connected;
        }
        if (!state_->open_socket()) {
            return State::ConnectResult::Failed;
        }

        state_->io_waiting.store(true, std::memory_order_release);
        const IoStatus status = io_connect(*this, state_->thread, state_->fd,
                                           reinterpret_cast<const sockaddr *>(&state_->address),
                                           state_->address_size, connect_state_);
        if (status.pending()) {
            return State::ConnectResult::Pending;
        }

        state_->io_waiting.store(false, std::memory_order_release);
        connect_state_.reset();
        if (status.ready()) {
            connected_ = true;
            return State::ConnectResult::Connected;
        }

        close_socket();
        state_->last_error.store(status.failed() ? status.error : ECONNRESET,
                                 std::memory_order_release);
        state_->last_error_stage.store(4, std::memory_order_release);
        return State::ConnectResult::Failed;
    }

    void close_socket() noexcept {
        connected_ = false;
        state_->io_waiting.store(false, std::memory_order_release);
        state_->close_socket();
        connect_state_.reset();
        send_state_.reset();
    }

    IoOpState connect_state_{};
    IoOpState send_state_{};
    bool connected_{false};
#endif

    void drop_current_records() noexcept {
        if (current_ != nullptr) {
            state_->dropped_records.fetch_add(current_->record_count, std::memory_order_relaxed);
            current_byte_ = current_->payload.size();
        }
    }

    void drop_current() noexcept {
        if (current_ == nullptr) {
            return;
        }
#if !defined(_WIN32)
        close_socket();
#endif
        drop_current_records();
        state_->complete_batch(current_);
        current_ = nullptr;
    }

    void drop_ready_batches() noexcept {
        while (Batch *batch = state_->ready_batches.try_pop()) {
            state_->dropped_records.fetch_add(batch->record_count, std::memory_order_relaxed);
            state_->complete_batch(batch);
        }
    }

    State *state_{nullptr};
    Batch *current_{nullptr};
    std::size_t current_byte_{0};
};

} // namespace detail

template <typename RuntimeT> class RuntimeTcpLogBackend final : public LogBackend {
public:
    using Config = RuntimeTcpLogBackendConfig<RuntimeT>;
    using State = detail::RuntimeTcpLogState<RuntimeT>;
    using SenderTask = detail::RuntimeTcpLogSenderTask<RuntimeT>;

    explicit RuntimeTcpLogBackend(Config config)
        : state_(std::make_unique<State>(std::move(config))),
          sender_(RuntimeT::template make_task<SenderTask>()) {}

    ~RuntimeTcpLogBackend() override {
        shutdown();
    }

    void write_batch(std::span<detail::LogRecord *const> records) noexcept override {
        if (state_->enqueue(records)) {
            static_cast<void>(wake_sender());
        }
    }

    void flush() noexcept override {
        static_cast<void>(flush(std::chrono::seconds(5)));
    }

    [[nodiscard]] bool flush(std::chrono::milliseconds timeout) noexcept override {
        if (state_->pending_batches.load(std::memory_order_acquire) == 0U) {
            return true;
        }
        if (!wake_sender()) {
            return false;
        }
        return state_->flush_until(std::chrono::steady_clock::now() + timeout);
    }

    void shutdown() noexcept override {
        bool expected = false;
        if (!shutdown_started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
            return;
        }

        static_cast<void>(flush(std::chrono::seconds(5)));
        state_->stopping.store(true, std::memory_order_release);
        if (!sender_started_.load(std::memory_order_acquire) &&
            state_->pending_batches.load(std::memory_order_acquire) == 0U) {
            state_->finished.store(true, std::memory_order_release);
            sender_.reset();
            return;
        }
        if (wake_sender()) {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (!state_->finished.load(std::memory_order_acquire) &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::yield();
            }
        }
        sender_.reset();
    }

    [[nodiscard]] RuntimeTcpLogBackendStats stats() const noexcept {
        return state_->stats();
    }

private:
    [[nodiscard]] bool wake_sender() noexcept {
        if (!sender_) {
            return false;
        }
        if (state_->finished.load(std::memory_order_acquire)) {
            return true;
        }
        if (state_->io_waiting.load(std::memory_order_acquire) &&
            !state_->stopping.load(std::memory_order_acquire)) {
            return true;
        }

        bool wake_expected = false;
        if (!state_->wake_queued.compare_exchange_strong(
                wake_expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return true;
        }

        bool expected = false;
        if (sender_started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
            if (sender_->start(state_.get())) {
                return true;
            }
            sender_started_.store(false, std::memory_order_release);
            state_->wake_queued.store(false, std::memory_order_release);
            return false;
        }

        if (sender_->wake()) {
            return true;
        }
        state_->wake_queued.store(false, std::memory_order_release);
        return false;
    }

    std::unique_ptr<State> state_;
    typename RuntimeT::template TaskHandle<SenderTask> sender_;
    std::atomic<bool> sender_started_{false};
    std::atomic<bool> shutdown_started_{false};
};

template <typename RuntimeT>
[[nodiscard]] inline std::unique_ptr<LogBackend>
make_runtime_tcp_log_backend(RuntimeTcpLogBackendConfig<RuntimeT> config) {
    return std::make_unique<RuntimeTcpLogBackend<RuntimeT>>(std::move(config));
}

} // namespace af
