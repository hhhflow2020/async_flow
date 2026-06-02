#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/log_backend.hpp"
#include "af/detail/queue/bounded_mpsc_queue.hpp"

namespace af {

enum class LogOverflowPolicy : std::uint8_t {
    DropNewest,
    Block,
};

struct AsyncLogConfig {
    std::size_t queue_capacity{1U << 16U};
    std::size_t max_batch_size{256};
    LogOverflowPolicy overflow_policy{LogOverflowPolicy::DropNewest};
    std::chrono::milliseconds flush_poll_interval{std::chrono::milliseconds(1)};
    std::chrono::milliseconds fatal_flush_timeout{std::chrono::milliseconds(200)};
    bool initialize_absl_log{true};
    std::vector<std::unique_ptr<LogBackend>> backends;
};

struct AsyncLogStats {
    std::uint64_t accepted{0};
    std::uint64_t dropped{0};
};

class AsyncLogger {
public:
    explicit AsyncLogger(AsyncLogConfig config)
        : queue_(config.queue_capacity),
          max_batch_size_(config.max_batch_size == 0U ? 1U : config.max_batch_size),
          overflow_policy_(config.overflow_policy),
          flush_poll_interval_(config.flush_poll_interval),
          fatal_flush_timeout_(config.fatal_flush_timeout), backends_(std::move(config.backends)) {}

    AsyncLogger(const AsyncLogger &) = delete;
    AsyncLogger &operator=(const AsyncLogger &) = delete;

    ~AsyncLogger() {
        shutdown();
    }

    void start() {
        bool expected = false;
        if (!started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
            return;
        }
        accepting_.store(true, std::memory_order_release);
        worker_ = std::thread([this] { worker_main(); });
    }

    [[nodiscard]] bool try_log(std::string_view message) noexcept {
        if (!accepting_.load(std::memory_order_acquire)) {
            dropped_.fetch_add(1U, std::memory_order_relaxed);
            return false;
        }

        auto *record = allocate_record(message);
        if (record == nullptr) {
            dropped_.fetch_add(1U, std::memory_order_relaxed);
            return false;
        }

        if (!push_record(record)) {
            delete record;
            dropped_.fetch_add(1U, std::memory_order_relaxed);
            return false;
        }

        accepted_.fetch_add(1U, std::memory_order_relaxed);
        const auto previous = pending_.fetch_add(1U, std::memory_order_release);
        if (previous == 0U) {
            wake_cv_.notify_one();
        }
        return true;
    }

    [[nodiscard]] bool flush(std::chrono::milliseconds timeout) noexcept {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::unique_lock lock(wait_mutex_);
        while (pending_.load(std::memory_order_acquire) != 0U) {
            if (drained_cv_.wait_until(lock, deadline) == std::cv_status::timeout &&
                pending_.load(std::memory_order_acquire) != 0U) {
                return false;
            }
        }
        lock.unlock();
        flush_backends();
        return true;
    }

    void shutdown() noexcept {
        const bool was_accepting = accepting_.exchange(false, std::memory_order_acq_rel);
        stopping_.store(true, std::memory_order_release);
        wake_cv_.notify_one();
        if (worker_.joinable()) {
            worker_.join();
        }
        if (was_accepting || started_.load(std::memory_order_acquire)) {
            flush_backends();
        }
    }

    [[nodiscard]] AsyncLogStats stats() const noexcept {
        return {
            accepted_.load(std::memory_order_acquire),
            dropped_.load(std::memory_order_acquire),
        };
    }

    [[nodiscard]] std::chrono::milliseconds fatal_flush_timeout() const noexcept {
        return fatal_flush_timeout_;
    }

private:
    [[nodiscard]] detail::LogRecord *allocate_record(std::string_view message) noexcept {
        try {
            return new detail::LogRecord(message);
        } catch (...) {
            return nullptr;
        }
    }

    [[nodiscard]] bool push_record(detail::LogRecord *record) noexcept {
        if (overflow_policy_ == LogOverflowPolicy::DropNewest) {
            return queue_.try_push(record);
        }

        while (accepting_.load(std::memory_order_acquire)) {
            if (queue_.try_push(record)) {
                return true;
            }
            std::this_thread::yield();
        }
        return false;
    }

    void worker_main() noexcept {
        std::vector<detail::LogRecord *> batch;
        batch.reserve(max_batch_size_);

        for (;;) {
            drain_available(batch);
            if (stopping_.load(std::memory_order_acquire) &&
                pending_.load(std::memory_order_acquire) == 0U) {
                break;
            }

            std::unique_lock lock(wait_mutex_);
            wake_cv_.wait_for(lock, flush_poll_interval_, [this] {
                return stopping_.load(std::memory_order_acquire) ||
                       pending_.load(std::memory_order_acquire) != 0U;
            });
        }

        drain_available(batch);
        flush_backends();
    }

    void drain_available(std::vector<detail::LogRecord *> &batch) noexcept {
        for (;;) {
            batch.clear();
            while (batch.size() < max_batch_size_) {
                detail::LogRecord *record = queue_.try_pop();
                if (record == nullptr) {
                    break;
                }
                batch.push_back(record);
            }

            if (batch.empty()) {
                return;
            }

            for (auto &backend : backends_) {
                backend->write_batch(
                    std::span<detail::LogRecord *const>(batch.data(), batch.size()));
            }

            for (detail::LogRecord *record : batch) {
                delete record;
            }

            const auto drained = batch.size();
            const auto previous = pending_.fetch_sub(drained, std::memory_order_acq_rel);
            AF_ASSERT(previous >= drained);
            if (previous == drained) {
                drained_cv_.notify_all();
            }
        }
    }

    void flush_backends() noexcept {
        for (auto &backend : backends_) {
            backend->flush();
        }
    }

    detail::BoundedMpscQueue<detail::LogRecord> queue_;
    const std::size_t max_batch_size_;
    const LogOverflowPolicy overflow_policy_;
    const std::chrono::milliseconds flush_poll_interval_;
    const std::chrono::milliseconds fatal_flush_timeout_;
    std::vector<std::unique_ptr<LogBackend>> backends_;

    alignas(detail::hardware_cache_line_size) std::atomic<bool> started_{false};
    alignas(detail::hardware_cache_line_size) std::atomic<bool> accepting_{false};
    alignas(detail::hardware_cache_line_size) std::atomic<bool> stopping_{false};
    alignas(detail::hardware_cache_line_size) std::atomic<std::uint64_t> accepted_{0};
    alignas(detail::hardware_cache_line_size) std::atomic<std::uint64_t> dropped_{0};
    alignas(detail::hardware_cache_line_size) std::atomic<std::size_t> pending_{0};

    std::mutex wait_mutex_;
    std::condition_variable wake_cv_;
    std::condition_variable drained_cv_;
    std::thread worker_;
};

} // namespace af
