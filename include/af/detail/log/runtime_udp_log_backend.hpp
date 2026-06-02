#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/log_backend.hpp"
#include "af/detail/log/network_log_backend.hpp"
#include "af/detail/log/runtime_log_backend_common.hpp"
#include "af/task.hpp"

#if !defined(_WIN32)
#include <netdb.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace af {

template <typename RuntimeT> struct RuntimeUdpLogBackendConfig {
    typename RuntimeT::Thread thread{};
    std::string host;
    std::uint16_t port{0};
    std::size_t batch_queue_capacity{1024};
    std::size_t max_batch_records{64};
    std::size_t max_datagram_size{1400};
    std::size_t max_batches_per_run{64};
};

struct RuntimeUdpLogBackendStats {
    std::uint64_t queued_records{0};
    std::uint64_t sent_records{0};
    std::uint64_t dropped_records{0};
};

namespace detail {

struct RuntimeUdpLogMessage {
    std::uint32_t offset{0};
    std::uint32_t size{0};
};

class RuntimeUdpLogBatch {
public:
    RuntimeUdpLogBatch(std::size_t max_records, std::size_t max_datagram_size)
        : max_datagram_size_(max_datagram_size) {
        messages.reserve(max_records);
        payload.reserve(max_records * max_datagram_size);
    }

    void reset() noexcept {
        record_count = 0;
        messages.clear();
        payload.clear();
    }

    [[nodiscard]] bool append(std::string_view message, std::size_t max_records) {
        if (message.empty()) {
            return true;
        }
        if (record_count >= max_records) {
            return false;
        }

        const std::size_t size = std::min(message.size(), max_datagram_size_);
        const std::size_t offset = payload.size();
        payload.insert(payload.end(), message.data(), message.data() + size);
        messages.push_back(RuntimeUdpLogMessage{static_cast<std::uint32_t>(offset),
                                                static_cast<std::uint32_t>(size)});
        ++record_count;
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return record_count == 0U;
    }

    std::uint32_t record_count{0};
    std::vector<RuntimeUdpLogMessage> messages;
    std::vector<char> payload;

private:
    const std::size_t max_datagram_size_;
};

template <typename RuntimeT>
class RuntimeUdpLogState : public RuntimeLogQueueState<RuntimeUdpLogBatch> {
public:
    using Thread = typename RuntimeT::Thread;
    using Batch = RuntimeUdpLogBatch;
    using QueueState = RuntimeLogQueueState<Batch>;

    explicit RuntimeUdpLogState(RuntimeUdpLogBackendConfig<RuntimeT> config)
        : QueueState(
              config.batch_queue_capacity, normalize_max_batch_records(config.max_batch_records),
              config.max_batches_per_run, normalize_max_datagram_size(config.max_datagram_size)),
          thread(config.thread), sent_records(0), io_waiting(false) {
        resolve(config.host, config.port);
    }

    RuntimeUdpLogState(const RuntimeUdpLogState &) = delete;
    RuntimeUdpLogState &operator=(const RuntimeUdpLogState &) = delete;

    ~RuntimeUdpLogState() {
#if !defined(_WIN32)
        close_socket();
#endif
    }

    [[nodiscard]] RuntimeUdpLogBackendStats stats() const noexcept {
        return RuntimeUdpLogBackendStats{
            queued_records.load(std::memory_order_acquire),
            sent_records.load(std::memory_order_acquire),
            dropped_records.load(std::memory_order_acquire),
        };
    }

#if !defined(_WIN32)
    [[nodiscard]] bool ensure_socket() noexcept {
        if (fd >= 0) {
            return true;
        }
        if (!resolved) {
            return false;
        }

        int candidate = ::socket(address.ss_family, SOCK_DGRAM, protocol);
        if (candidate < 0) {
            return false;
        }
        set_log_socket_nonblocking(candidate);
        if (::connect(candidate, reinterpret_cast<const sockaddr *>(&address), address_size) != 0) {
            static_cast<void>(::close(candidate));
            return false;
        }
        fd = candidate;
        return true;
    }

    void close_socket() noexcept {
        close_log_socket(fd);
    }
#endif

    Thread thread;
    CacheLineAtomic<std::uint64_t> sent_records;
    CacheLineAtomic<bool> io_waiting;

#if !defined(_WIN32)
    sockaddr_storage address{};
    socklen_t address_size{0};
    int protocol{0};
    int fd{-1};
    bool resolved{false};
#endif

private:
    [[nodiscard]] static std::size_t normalize_max_batch_records(std::size_t requested) noexcept {
        constexpr std::size_t max_supported_records = 64;
        if (requested == 0U) {
            return 1U;
        }
        return std::min(requested, max_supported_records);
    }

    [[nodiscard]] static std::size_t normalize_max_datagram_size(std::size_t requested) noexcept {
        constexpr std::size_t max_udp_payload_size = 65507;
        if (requested == 0U) {
            return 1U;
        }
        return std::min(requested, max_udp_payload_size);
    }

    void resolve(const std::string &host, std::uint16_t port) noexcept {
#if defined(_WIN32)
        static_cast<void>(host);
        static_cast<void>(port);
#else
        addrinfo hints{};
        hints.ai_socktype = SOCK_DGRAM;
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

template <typename RuntimeT> class RuntimeUdpLogSenderTask final : public RuntimeT::Task {
public:
    using TaskBase = typename RuntimeT::Task;
    using State = RuntimeUdpLogState<RuntimeT>;

    explicit RuntimeUdpLogSenderTask(typename TaskBase::FactoryToken token) : TaskBase(token) {}

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

        if (state_->stopping.load(std::memory_order_acquire)) {
            drop_current();
            drop_ready_batches();
#if !defined(_WIN32)
            state_->io_waiting.store(false, std::memory_order_release);
            state_->close_socket();
#endif
            return finish();
        }
#if !defined(_WIN32)
        if (state_->io_waiting.load(std::memory_order_acquire) && !io_wait_ready()) {
            return io_pending();
        }
        state_->io_waiting.store(false, std::memory_order_release);
#endif
        std::size_t drained_batches = 0;
        for (;;) {
            if (current_ == nullptr) {
                current_ = state_->ready_batches.try_pop();
                current_message_ = 0;
                if (current_ == nullptr) {
                    if (state_->stopping.load(std::memory_order_acquire)) {
                        return finish();
                    }
                    return idle();
                }
            }

            const SendResult result = send_current();
            if (result == SendResult::Pending) {
                return io_pending();
            }

            state_->complete_batch(current_);
            current_ = nullptr;
            ++drained_batches;
            if (drained_batches >= state_->max_batches_per_run) {
                return this->again();
            }
        }
    }

    TaskResult idle() noexcept {
        state_->wake_queued.store(false, std::memory_order_release);
        if (state_->stopping.load(std::memory_order_acquire) ||
            state_->pending_batches.load(std::memory_order_acquire) != 0U) {
            state_->wake_queued.store(true, std::memory_order_release);
            return this->again();
        }
        return this->pending();
    }

    TaskResult io_pending() noexcept {
        return this->pending();
    }

    TaskResult finish() noexcept {
        state_->wake_queued.store(false, std::memory_order_release);
        state_->mark_finished();
        return this->done();
    }

    enum class SendResult : std::uint8_t {
        Complete,
        Pending,
    };

    [[nodiscard]] SendResult send_current() noexcept {
#if defined(_WIN32)
        state_->dropped_records.fetch_add(current_->messages.size() - current_message_,
                                          std::memory_order_relaxed);
        current_message_ = current_->messages.size();
        return SendResult::Complete;
#else
        if (!state_->ensure_socket()) {
            state_->dropped_records.fetch_add(current_->messages.size() - current_message_,
                                              std::memory_order_relaxed);
            current_message_ = current_->messages.size();
            return SendResult::Complete;
        }

        while (current_message_ < current_->messages.size()) {
#if defined(__linux__)
            const SendResult result = send_current_mmsg();
#else
            const SendResult result = send_current_one();
#endif
            if (result == SendResult::Pending) {
                return SendResult::Pending;
            }
        }
        return SendResult::Complete;
#endif
    }

#if !defined(_WIN32)
    [[nodiscard]] SendResult arm_writable_wait() noexcept {
        wait_result_ = IoResult{};
        state_->io_waiting.store(true, std::memory_order_release);
        if (this->wait_io(state_->thread, state_->fd, io_writable, &wait_result_)) {
            return SendResult::Pending;
        }

        state_->io_waiting.store(false, std::memory_order_release);
        state_->close_socket();
        state_->dropped_records.fetch_add(current_->messages.size() - current_message_,
                                          std::memory_order_relaxed);
        current_message_ = current_->messages.size();
        return SendResult::Complete;
    }

    [[nodiscard]] bool io_wait_ready() const noexcept {
        return wait_result_.events != 0U || wait_result_.error != 0 || wait_result_.result != 0;
    }

    [[nodiscard]] SendResult send_current_one() noexcept {
        const RuntimeUdpLogMessage &message = current_->messages[current_message_];
        const char *data = current_->payload.data() + message.offset;
        for (;;) {
            const ssize_t sent = ::send(state_->fd, data, message.size, log_send_flags());
            if (sent >= 0) {
                state_->sent_records.fetch_add(1U, std::memory_order_relaxed);
                ++current_message_;
                return SendResult::Complete;
            }

            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            if (error == EAGAIN || error == EWOULDBLOCK) {
                return arm_writable_wait();
            }
            state_->close_socket();
            state_->dropped_records.fetch_add(1U, std::memory_order_relaxed);
            ++current_message_;
            return SendResult::Complete;
        }
    }

#if defined(__linux__)
    [[nodiscard]] SendResult send_current_mmsg() noexcept {
        std::size_t count = 0;
        const std::size_t remaining = current_->messages.size() - current_message_;
        const std::size_t limit = std::min(remaining, messages_.size());
        while (count < limit) {
            const RuntimeUdpLogMessage &message = current_->messages[current_message_ + count];
            iovecs_[count].iov_base = const_cast<char *>(current_->payload.data() + message.offset);
            iovecs_[count].iov_len = message.size;
            messages_[count] = {};
            messages_[count].msg_hdr.msg_iov = &iovecs_[count];
            messages_[count].msg_hdr.msg_iovlen = 1;
            ++count;
        }

        for (;;) {
            const int sent = log_sendmmsg(state_->fd, messages_.data(),
                                          static_cast<unsigned int>(count), log_send_flags());
            if (sent > 0) {
                current_message_ += static_cast<std::size_t>(sent);
                state_->sent_records.fetch_add(static_cast<std::uint64_t>(sent),
                                               std::memory_order_relaxed);
                return SendResult::Complete;
            }
            if (sent == 0) {
                return arm_writable_wait();
            }

            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            if (error == EAGAIN || error == EWOULDBLOCK) {
                return arm_writable_wait();
            }
            state_->close_socket();
            state_->dropped_records.fetch_add(count, std::memory_order_relaxed);
            current_message_ += count;
            return SendResult::Complete;
        }
    }

    std::array<iovec, 64> iovecs_{};
    std::array<LogMmsgHeader, 64> messages_{};
#endif

    IoResult wait_result_{};
#endif

    void drop_current() noexcept {
        if (current_ == nullptr) {
            return;
        }
#if !defined(_WIN32)
        state_->io_waiting.store(false, std::memory_order_release);
        wait_result_ = IoResult{};
        state_->close_socket();
#endif
        state_->dropped_records.fetch_add(current_->messages.size() - current_message_,
                                          std::memory_order_relaxed);
        state_->complete_batch(current_);
        current_ = nullptr;
        current_message_ = 0;
    }

    void drop_ready_batches() noexcept {
        while (RuntimeUdpLogBatch *batch = state_->ready_batches.try_pop()) {
            state_->dropped_records.fetch_add(batch->messages.size(), std::memory_order_relaxed);
            state_->complete_batch(batch);
        }
    }

    State *state_{nullptr};
    RuntimeUdpLogBatch *current_{nullptr};
    std::size_t current_message_{0};
};

} // namespace detail

template <typename RuntimeT> class RuntimeUdpLogBackend final : public LogBackend {
public:
    using Config = RuntimeUdpLogBackendConfig<RuntimeT>;
    using State = detail::RuntimeUdpLogState<RuntimeT>;
    using SenderTask = detail::RuntimeUdpLogSenderTask<RuntimeT>;

    explicit RuntimeUdpLogBackend(Config config)
        : binding_(std::make_unique<State>(std::move(config))) {}

    ~RuntimeUdpLogBackend() override {
        shutdown();
    }

    void write_batch(std::span<detail::LogRecord *const> records) noexcept override {
        static_cast<void>(binding_.enqueue_and_wake(records, true));
    }

    void flush() noexcept override {
        static_cast<void>(flush(std::chrono::seconds(5)));
    }

    [[nodiscard]] bool flush(std::chrono::milliseconds timeout) noexcept override {
        State &state = binding_.state();
        if (state.pending_batches.load(std::memory_order_acquire) == 0U) {
            return true;
        }
        if (!binding_.wake(false)) {
            return false;
        }
        return state.flush_until(std::chrono::steady_clock::now() + timeout);
    }

    void shutdown() noexcept override {
        bool expected = false;
        if (!shutdown_started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
            return;
        }

        static_cast<void>(flush(std::chrono::seconds(5)));
        binding_.stop_and_wait(std::chrono::steady_clock::now() + std::chrono::seconds(5), false);
    }

    [[nodiscard]] RuntimeUdpLogBackendStats stats() const noexcept {
        return binding_.state().stats();
    }

private:
    detail::RuntimeLogTaskBinding<RuntimeT, State, SenderTask> binding_;
    std::atomic<bool> shutdown_started_{false};
};

template <typename RuntimeT>
[[nodiscard]] inline std::unique_ptr<LogBackend>
make_runtime_udp_log_backend(RuntimeUdpLogBackendConfig<RuntimeT> config) {
    return std::make_unique<RuntimeUdpLogBackend<RuntimeT>>(std::move(config));
}

} // namespace af
