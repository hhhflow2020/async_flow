#pragma once

enum class WideHopThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    Logic_64 = 64,
    enum_thread_index_end,
};

struct WideHopRuntimeTraits {
    using Thread = WideHopThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(WideHopThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 256;
    static constexpr std::size_t external_queue_capacity = 256;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;
};

using WideHopRuntime = af::AsyncRuntime<WideHopRuntimeTraits>;
using WideHopTaskBase = WideHopRuntime::Task;

class WideHopTask final : public WideHopTaskBase {
public:
    explicit WideHopTask(WideHopTaskBase::FactoryToken token) : WideHopTaskBase(token) {}

    bool do_it(int hops, std::atomic<int> *remaining, std::atomic<int> *runs,
               std::atomic<int> *post_failures) {
        hops_ = hops;
        remaining_ = remaining;
        runs_ = runs;
        post_failures_ = post_failures;
        return schedule(WideHopThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        runs_->fetch_add(1, std::memory_order_relaxed);
        if (hops_-- > 0) {
            const auto next = WideHopRuntime::current_thread_index() == 0 ? WideHopThread::Logic_64
                                                                          : WideHopThread::Logic_0;
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
    std::atomic<int> *remaining_{nullptr};
    std::atomic<int> *runs_{nullptr};
    std::atomic<int> *post_failures_{nullptr};
};
