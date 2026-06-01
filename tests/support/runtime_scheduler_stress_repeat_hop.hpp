#pragma once

enum class RepeatHopThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    Logic_1,
    enum_thread_index_end,
};

struct RepeatHopRuntimeTraits {
    using Thread = RepeatHopThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(RepeatHopThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 65536;
    static constexpr std::size_t external_queue_capacity = 65536;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;
};

using RepeatHopRuntime = af::AsyncRuntime<RepeatHopRuntimeTraits>;
using RepeatHopTaskBase = RepeatHopRuntime::Task;

class RepeatHopTask final : public RepeatHopTaskBase {
public:
    explicit RepeatHopTask(RepeatHopTaskBase::FactoryToken token) : RepeatHopTaskBase(token) {}

    bool do_it(int hops, int id, std::atomic<int> *remaining, std::atomic<int> *runs,
               std::atomic<int> *post_failures, std::atomic<int> *progress,
               std::atomic<int> *last_thread) {
        hops_ = hops;
        id_ = id;
        remaining_ = remaining;
        runs_ = runs;
        post_failures_ = post_failures;
        progress_ = progress;
        last_thread_ = last_thread;
        return schedule(RepeatHopThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        runs_->fetch_add(1, std::memory_order_relaxed);
        progress_[id_].fetch_add(1, std::memory_order_relaxed);
        last_thread_[id_].store(RepeatHopRuntime::current_thread() == RepeatHopThread::Logic_0 ? 0
                                                                                               : 1,
                                std::memory_order_relaxed);
        if (hops_-- > 0) {
            const auto next = RepeatHopRuntime::current_thread() == RepeatHopThread::Logic_0
                                  ? RepeatHopThread::Logic_1
                                  : RepeatHopThread::Logic_0;
            if (!schedule(next)) {
                post_failures_->fetch_add(1, std::memory_order_relaxed);
                if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    remaining_->notify_one();
                }
                return failed();
            }
            return pending();
        }

        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            remaining_->notify_one();
        }
        return done();
    }

    int hops_{0};
    int id_{0};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *runs_{nullptr};
    std::atomic<int> *post_failures_{nullptr};
    std::atomic<int> *progress_{nullptr};
    std::atomic<int> *last_thread_{nullptr};
};
