#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

#include "af/detail/log/async_logger.hpp"
#include "af/task.hpp"

namespace af::detail {

template <typename RuntimeT> class RuntimeAsyncLogConsumerTask final : public RuntimeT::Task {
public:
    using TaskBase = typename RuntimeT::Task;
    using Controller = RuntimeAsyncLogConsumerController<RuntimeT>;

    explicit RuntimeAsyncLogConsumerTask(typename TaskBase::FactoryToken token) : TaskBase(token) {}

    [[nodiscard]] bool start(Controller *controller) noexcept {
        controller_ = controller;
        if (!reserve_batch()) {
            return false;
        }
        return this->schedule(controller_->thread());
    }

    [[nodiscard]] bool wake() noexcept {
        return controller_ != nullptr && this->schedule(controller_->thread());
    }

private:
    [[nodiscard]] bool reserve_batch() noexcept {
        try {
            batch_.reserve(controller_->max_batch_size());
            return true;
        } catch (...) {
            return false;
        }
    }

    TaskResult run() override {
        if (controller_ == nullptr) [[unlikely]] {
            return this->done();
        }

        controller_->drain_some(batch_);

        if (controller_->stop_requested() && controller_->pending_record_count() == 0U) {
            controller_->mark_finished();
            return this->done();
        }

        if (controller_->ready_record_count() != 0U) {
            return this->again();
        }
        if (controller_->mark_idle_or_continue()) {
            return this->again();
        }
        return this->pending();
    }

    Controller *controller_{nullptr};
    std::vector<LogRecord *> batch_;
};

template <typename RuntimeT>
class RuntimeAsyncLogConsumerController final : public AsyncLogConsumerWakeTarget,
                                                public AsyncLogConsumerController {
public:
    using Thread = typename RuntimeT::Thread;
    using Task = RuntimeAsyncLogConsumerTask<RuntimeT>;

    RuntimeAsyncLogConsumerController(std::shared_ptr<AsyncLogger> logger, Thread thread,
                                      std::size_t max_batches_per_run)
        : logger_(std::move(logger)), thread_(thread),
          max_batches_per_run_(max_batches_per_run == 0U ? 1U : max_batches_per_run),
          task_(RuntimeT::template make_task<Task>()) {
        AF_ASSERT(logger_ != nullptr);
    }

    RuntimeAsyncLogConsumerController(const RuntimeAsyncLogConsumerController &) = delete;
    RuntimeAsyncLogConsumerController &
    operator=(const RuntimeAsyncLogConsumerController &) = delete;

    ~RuntimeAsyncLogConsumerController() override {
        shutdown();
    }

    [[nodiscard]] bool start() noexcept {
        return logger_->start_bound_consumer(*this);
    }

    [[nodiscard]] bool wake_async_log_consumer() noexcept override {
        if (!task_ || finished_.load(std::memory_order_acquire)) {
            return false;
        }

        bool wake_expected = false;
        if (!wake_queued_.compare_exchange_strong(wake_expected, true, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            return true;
        }

        bool start_expected = false;
        if (task_started_.compare_exchange_strong(start_expected, true, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            if (task_->start(this)) {
                return true;
            }
            task_started_.store(false, std::memory_order_release);
            wake_queued_.store(false, std::memory_order_release);
            return false;
        }

        if (task_->wake()) {
            return true;
        }
        wake_queued_.store(false, std::memory_order_release);
        return false;
    }

    void shutdown() noexcept override {
        bool expected = false;
        if (!shutdown_started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
            return;
        }

        logger_->stop_bound_consumer_admission();
        if (!task_started_.load(std::memory_order_acquire) &&
            logger_->pending_record_count() == 0U) {
            mark_finished();
            task_.reset();
            logger_->finish_bound_consumer_shutdown();
            return;
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!finished_.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
            static_cast<void>(wake_async_log_consumer());
            auto retry_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(1);
            if (retry_deadline > deadline) {
                retry_deadline = deadline;
            }
            static_cast<void>(wait_until_finished(retry_deadline));
        }
        task_.reset();
        logger_->finish_bound_consumer_shutdown();
    }

    [[nodiscard]] Thread thread() const noexcept {
        return thread_;
    }

    [[nodiscard]] std::size_t max_batches_per_run() const noexcept {
        return max_batches_per_run_;
    }

    [[nodiscard]] std::size_t max_batch_size() const noexcept {
        return logger_->max_batch_size();
    }

    [[nodiscard]] std::size_t pending_record_count() const noexcept {
        return logger_->pending_record_count();
    }

    [[nodiscard]] std::size_t ready_record_count() const noexcept {
        return logger_->ready_record_count();
    }

    [[nodiscard]] bool stop_requested() const noexcept {
        return logger_->consumer_stop_requested();
    }

    void drain_some(std::vector<LogRecord *> &batch) noexcept {
        static_cast<void>(logger_->drain_some(batch, max_batches_per_run_));
    }

    [[nodiscard]] bool mark_idle_or_continue() noexcept {
        wake_queued_.store(false, std::memory_order_release);
        if (ready_record_count() == 0U && !(stop_requested() && pending_record_count() == 0U)) {
            return false;
        }
        wake_queued_.store(true, std::memory_order_release);
        return true;
    }

    void mark_finished() noexcept {
        wake_queued_.store(false, std::memory_order_release);
        finished_.store(true, std::memory_order_release);
        finished_.notify_all();
        finished_cv_.notify_all();
    }

private:
    [[nodiscard]] bool
    wait_until_finished(std::chrono::steady_clock::time_point deadline) noexcept {
        std::unique_lock lock(finished_mutex_);
        return finished_cv_.wait_until(
            lock, deadline, [this] { return finished_.load(std::memory_order_acquire); });
    }

    std::shared_ptr<AsyncLogger> logger_;
    Thread thread_;
    std::size_t max_batches_per_run_;
    typename RuntimeT::template TaskHandle<Task> task_;
    std::atomic<bool> wake_queued_{false};
    std::atomic<bool> task_started_{false};
    std::atomic<bool> shutdown_started_{false};
    std::atomic<bool> finished_{false};
    std::mutex finished_mutex_;
    std::condition_variable finished_cv_;
};

} // namespace af::detail
