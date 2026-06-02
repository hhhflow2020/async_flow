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
#include <thread>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/log_backend.hpp"
#include "af/detail/log/network_log_backend.hpp"
#include "af/detail/queue/bounded_spsc_queue.hpp"
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
    RuntimeUdpLogBatch(std::size_t max_records, std::size_t max_datagram_size) {
        messages.reserve(max_records);
        payload.reserve(max_records * max_datagram_size);
    }

    void reset() noexcept {
        messages.clear();
        payload.clear();
    }

    [[nodiscard]] bool append(std::string_view message, std::size_t max_records,
                              std::size_t max_datagram_size) {
        if (message.empty()) {
            return true;
        }
        if (messages.size() >= max_records) {
            return false;
        }

        const std::size_t size = std::min(message.size(), max_datagram_size);
        const std::size_t offset = payload.size();
        payload.insert(payload.end(), message.data(), message.data() + size);
        messages.push_back(RuntimeUdpLogMessage{static_cast<std::uint32_t>(offset),
                                                static_cast<std::uint32_t>(size)});
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return messages.empty();
    }

    std::vector<RuntimeUdpLogMessage> messages;
    std::vector<char> payload;
};

template <typename RuntimeT> class RuntimeUdpLogState {
public:
    using Thread = typename RuntimeT::Thread;
    using Batch = RuntimeUdpLogBatch;

    explicit RuntimeUdpLogState(RuntimeUdpLogBackendConfig<RuntimeT> config)
        : thread(config.thread),
          max_batch_records(normalize_max_batch_records(config.max_batch_records)),
          max_datagram_size(normalize_max_datagram_size(config.max_datagram_size)),
          max_batches_per_run(config.max_batches_per_run == 0U ? 1U : config.max_batches_per_run),
          ready_batches(config.batch_queue_capacity), free_batches(config.batch_queue_capacity) {
        resolve(config.host, config.port);
        reserve_batches(config.batch_queue_capacity);
    }

    RuntimeUdpLogState(const RuntimeUdpLogState &) = delete;
    RuntimeUdpLogState &operator=(const RuntimeUdpLogState &) = delete;

    ~RuntimeUdpLogState() {
#if !defined(_WIN32)
        close_socket();
#endif
    }

    [[nodiscard]] bool enqueue(std::span<LogRecord *const> records) noexcept {
        if (records.empty() || stopping.load(std::memory_order_acquire)) {
            return false;
        }

        bool enqueued_any = false;
        std::size_t index = 0;
        while (index < records.size()) {
            Batch *batch = free_batches.try_pop();
            if (batch == nullptr) {
                dropped_records.fetch_add(records.size() - index, std::memory_order_relaxed);
                return enqueued_any;
            }

            batch->reset();
            const std::size_t begin = index;
            while (index < records.size()) {
                const std::string_view message = records[index]->message();
                if (!batch->append(message, max_batch_records, max_datagram_size)) {
                    break;
                }
                ++index;
                if (batch->messages.size() >= max_batch_records) {
                    break;
                }
            }

            if (batch->empty()) {
                recycle_batch(batch);
                if (index == begin) {
                    ++index;
                }
                continue;
            }

            queued_records.fetch_add(batch->messages.size(), std::memory_order_relaxed);
            pending_batches.fetch_add(1U, std::memory_order_acq_rel);
            if (!ready_batches.try_push(batch)) [[unlikely]] {
                complete_batch(batch);
                dropped_records.fetch_add(index - begin, std::memory_order_relaxed);
                return enqueued_any;
            }
            enqueued_any = true;
        }
        return enqueued_any;
    }

    [[nodiscard]] bool flush_until(std::chrono::steady_clock::time_point deadline) noexcept {
        while (pending_batches.load(std::memory_order_acquire) != 0U) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::yield();
        }
        return true;
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

    void complete_batch(Batch *batch) noexcept {
        if (batch == nullptr) {
            return;
        }
        recycle_batch(batch);
        if (pending_batches.fetch_sub(1U, std::memory_order_acq_rel) == 1U) {
            pending_batches.notify_all();
        }
    }

    void recycle_batch(Batch *batch) noexcept {
        const bool recycled = free_batches.try_push(batch);
        AF_ASSERT(recycled);
        static_cast<void>(recycled);
    }

    Thread thread;
    const std::size_t max_batch_records;
    const std::size_t max_datagram_size;
    const std::size_t max_batches_per_run;
    BoundedSpscQueue<Batch> ready_batches;
    BoundedSpscQueue<Batch> free_batches;
    std::vector<std::unique_ptr<Batch>> storage;
    std::atomic<std::uint64_t> queued_records{0};
    std::atomic<std::uint64_t> sent_records{0};
    std::atomic<std::uint64_t> dropped_records{0};
    std::atomic<std::size_t> pending_batches{0};
    std::atomic<bool> wake_queued{false};
    std::atomic<bool> stopping{false};
    std::atomic<bool> finished{false};

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

    void reserve_batches(std::size_t queue_capacity) {
        const std::size_t capacity = queue_capacity == 0U ? 1U : queue_capacity;
        storage.reserve(capacity);
        for (std::size_t i = 0; i < capacity; ++i) {
            auto batch = std::make_unique<Batch>(max_batch_records, max_datagram_size);
            Batch *ptr = batch.get();
            storage.push_back(std::move(batch));
            const bool ok = free_batches.try_push(ptr);
            AF_ASSERT(ok);
            static_cast<void>(ok);
        }
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

        state_->wake_queued.store(false, std::memory_order_release);
        std::size_t drained_batches = 0;
        for (;;) {
            if (current_ == nullptr) {
                current_ = state_->ready_batches.try_pop();
                current_message_ = 0;
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
        if (this->wait_io(state_->thread, state_->fd, io_writable, &wait_result_)) {
            return SendResult::Pending;
        }

        state_->close_socket();
        state_->dropped_records.fetch_add(current_->messages.size() - current_message_,
                                          std::memory_order_relaxed);
        current_message_ = current_->messages.size();
        return SendResult::Complete;
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
        : state_(std::make_unique<State>(std::move(config))),
          sender_(RuntimeT::template make_task<SenderTask>()) {}

    ~RuntimeUdpLogBackend() override {
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

    [[nodiscard]] RuntimeUdpLogBackendStats stats() const noexcept {
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
make_runtime_udp_log_backend(RuntimeUdpLogBackendConfig<RuntimeT> config) {
    return std::make_unique<RuntimeUdpLogBackend<RuntimeT>>(std::move(config));
}

} // namespace af
