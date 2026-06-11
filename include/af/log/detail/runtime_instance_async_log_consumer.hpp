#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "af/log/detail/async_logger.hpp"
#include "af/detail/runtime/runtime_service_task.hpp"
#include "af/detail/runtime/timed_atomic_wait.hpp"
#include "af/memory/cache_line.hpp"

namespace af::detail {

enum class runtime_instance_async_log_consumer_control_operation : std::uint8_t {
    register_consumer,
    unregister_consumer,
};

struct runtime_instance_async_log_consumer_control_completion {
    std::atomic<bool> done{false};
    std::atomic<bool> ok{false};
};

class runtime_instance_async_log_consumer_controller;

class runtime_instance_async_log_consumer_control_task final : public runtime_task {
public:
    runtime_instance_async_log_consumer_control_task(runtime_task::factory_token token,
                                                     runtime &owner)
        : runtime_task(token, owner) {}

    [[nodiscard]] bool
    do_it(runtime_instance_async_log_consumer_controller *controller,
          runtime_instance_async_log_consumer_control_operation operation,
          runtime_instance_async_log_consumer_control_completion *completion) noexcept;

private:
    task_result run_task() noexcept override;

    runtime_instance_async_log_consumer_controller *controller_{nullptr};
    runtime_instance_async_log_consumer_control_operation operation_{
        runtime_instance_async_log_consumer_control_operation::register_consumer};
    runtime_instance_async_log_consumer_control_completion *completion_{nullptr};
};

class runtime_instance_async_log_consumer_controller final : public runtime_service_task,
                                                             public async_log_consumer_wake_target,
                                                             public async_log_consumer_controller {
public:
    runtime_instance_async_log_consumer_controller(runtime &owner,
                                                   std::shared_ptr<async_logger> logger,
                                                   runtime::thread_index thread,
                                                   std::size_t max_batches_per_run)
        : owner_(owner), logger_(std::move(logger)), thread_(thread),
          max_batches_per_run_(max_batches_per_run == 0U ? 1U : max_batches_per_run) {
        AF_ASSERT(logger_ != nullptr);
    }

    runtime_instance_async_log_consumer_controller(
        const runtime_instance_async_log_consumer_controller &) = delete;
    runtime_instance_async_log_consumer_controller &
    operator=(const runtime_instance_async_log_consumer_controller &) = delete;

    ~runtime_instance_async_log_consumer_controller() override {
        shutdown(std::chrono::seconds(5));
    }

    [[nodiscard]] bool start() noexcept {
        if (!owner_.valid_thread(thread_)) [[unlikely]] {
            return false;
        }
        if (!reserve_batch()) {
            return false;
        }
        if (!logger_->start_bound_consumer(*this)) {
            return false;
        }
        if (!run_control_and_wait(
                runtime_instance_async_log_consumer_control_operation::register_consumer)) {
            logger_->stop_bound_consumer_admission();
            logger_->finish_bound_consumer_shutdown();
            return false;
        }
        static_cast<void>(wake_async_log_consumer());
        return true;
    }

    [[nodiscard]] bool wake_async_log_consumer() noexcept override {
        if (!registered_.load(std::memory_order_acquire) ||
            finished_.load(std::memory_order_acquire)) {
            return false;
        }
        return owner_.wake_service_tasks(thread_);
    }

    [[nodiscard]] bool run_service(std::size_t budget) noexcept override {
        if (finished_.load(std::memory_order_acquire)) {
            return false;
        }

        const std::size_t effective_budget = effective_drain_budget(budget);
        const bool did_work = logger_->drain_some(batch_, effective_budget);
        if (logger_->consumer_stop_requested() && logger_->pending_record_count() == 0U) {
            mark_finished();
            return did_work;
        }

        return did_work || logger_->ready_record_count() != 0U;
    }

    void shutdown() noexcept {
        shutdown(std::chrono::seconds(5));
    }

    void shutdown(std::chrono::milliseconds timeout) noexcept override {
        bool expected = false;
        if (!shutdown_started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
            return;
        }

        logger_->stop_bound_consumer_admission();
        if (!registered_.load(std::memory_order_acquire) && logger_->pending_record_count() == 0U) {
            mark_finished();
            logger_->finish_bound_consumer_shutdown();
            return;
        }

        const auto deadline = std::chrono::steady_clock::now() + timeout;
        drain_until_finished(deadline);

        if (registered_.load(std::memory_order_acquire)) {
            static_cast<void>(run_control_and_wait(
                runtime_instance_async_log_consumer_control_operation::unregister_consumer));
        }
        logger_->finish_bound_consumer_shutdown();
    }

    [[nodiscard]] runtime::thread_index thread() const noexcept {
        return thread_;
    }

    [[nodiscard]] bool
    run_control(runtime_instance_async_log_consumer_control_operation operation) noexcept {
        switch (operation) {
        case runtime_instance_async_log_consumer_control_operation::register_consumer: {
            const bool ok = owner_.register_service_task(thread_, this);
            if (ok) {
                registered_.store(true, std::memory_order_release);
            }
            return ok;
        }
        case runtime_instance_async_log_consumer_control_operation::unregister_consumer: {
            const bool ok = owner_.unregister_service_task(thread_, this);
            if (ok) {
                registered_.store(false, std::memory_order_release);
            }
            return ok;
        }
        }
        return false;
    }

private:
    [[nodiscard]] bool reserve_batch() noexcept {
        try {
            batch_.reserve(logger_->max_batch_size());
            return true;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] std::size_t effective_drain_budget(std::size_t service_budget) const noexcept {
        if (service_budget == 0U || service_budget > max_batches_per_run_) {
            return max_batches_per_run_;
        }
        return service_budget;
    }

    [[nodiscard]] bool is_owner_runtime_thread() const noexcept {
        return runtime::current() == &owner_ && runtime::current_thread_index() == thread_;
    }

    void drain_until_finished(std::chrono::steady_clock::time_point deadline) noexcept {
        if (is_owner_runtime_thread()) {
            while (!finished_.load(std::memory_order_acquire) &&
                   std::chrono::steady_clock::now() < deadline) {
                const bool did_work = run_service(max_batches_per_run_);
                if (!did_work && !finished_.load(std::memory_order_acquire)) {
                    auto retry_deadline =
                        std::chrono::steady_clock::now() + std::chrono::milliseconds(1);
                    if (retry_deadline > deadline) {
                        retry_deadline = deadline;
                    }
                    static_cast<void>(wait_until_finished(retry_deadline));
                }
            }
            return;
        }

        while (!finished_.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
            static_cast<void>(wake_async_log_consumer());
            auto retry_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(1);
            if (retry_deadline > deadline) {
                retry_deadline = deadline;
            }
            static_cast<void>(wait_until_finished(retry_deadline));
        }
    }

    [[nodiscard]] bool
    run_control_and_wait(runtime_instance_async_log_consumer_control_operation operation) noexcept {
        if (is_owner_runtime_thread()) {
            return run_control(operation);
        }

        auto task = try_make_task<runtime_instance_async_log_consumer_control_task>(owner_);
        if (!task) {
            return false;
        }

        runtime_instance_async_log_consumer_control_completion completion;
        if (!task->do_it(this, operation, &completion)) {
            return false;
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!wait_until_atomic_flag_true(completion.done, deadline)) {
            return false;
        }
        return completion.ok.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool
    wait_until_finished(std::chrono::steady_clock::time_point deadline) noexcept {
        return wait_until_atomic_flag_true(finished_, deadline);
    }

    void mark_finished() noexcept {
        finished_.store(true, std::memory_order_release);
    }

    runtime &owner_;
    std::shared_ptr<async_logger> logger_;
    runtime::thread_index thread_;
    std::size_t max_batches_per_run_;
    std::vector<log_record *> batch_;
    cache_line_atomic<bool> registered_{false};
    cache_line_atomic<bool> shutdown_started_{false};
    cache_line_atomic<bool> finished_{false};
};

inline bool runtime_instance_async_log_consumer_control_task::do_it(
    runtime_instance_async_log_consumer_controller *controller,
    runtime_instance_async_log_consumer_control_operation operation,
    runtime_instance_async_log_consumer_control_completion *completion) noexcept {
    controller_ = controller;
    operation_ = operation;
    completion_ = completion;
    if (controller_ == nullptr || completion_ == nullptr) [[unlikely]] {
        return false;
    }
    return schedule_to(controller_->thread());
}

inline task_result runtime_instance_async_log_consumer_control_task::run_task() noexcept {
    bool ok = false;
    if (controller_ != nullptr) [[likely]] {
        ok = controller_->run_control(operation_);
    }
    if (completion_ != nullptr) {
        completion_->ok.store(ok, std::memory_order_release);
        completion_->done.store(true, std::memory_order_release);
    }
    return done();
}

} // namespace af::detail
