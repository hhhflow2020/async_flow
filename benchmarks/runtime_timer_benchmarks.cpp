#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

#include <benchmark/benchmark.h>

#include "af/detail/runtime/atomic_wait.hpp"
#include "af/runtime.hpp"
#include "af/runtime_config.hpp"

namespace {

class RuntimeTimerTask final : public af::runtime_task {
public:
    RuntimeTimerTask(factory_token token, af::runtime &owner) : af::runtime_task(token, owner) {}

    bool do_it(af::thread_ref thread, std::atomic<int> &remaining) noexcept {
        remaining_ = &remaining;
        return schedule_after(thread, std::chrono::nanoseconds(0));
    }

private:
    af::task_result run_task() noexcept override {
        if (remaining_->fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af::detail::atomic_notify_one(*remaining_);
        }
        return done();
    }

    std::atomic<int> *remaining_{nullptr};
};

[[nodiscard]] af::runtime_config make_timer_runtime_config(af::timer_kind timer_kind) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("timer", 1)};
    config.logger.consumer_thread = af::thread_selector::cpu(0);
    config.scheduler.task_drain_budget = 1024;
    config.timer.kind = timer_kind;
    config.timer.tick = std::chrono::milliseconds(1);
    config.timer.wheel_slots = 1024;
    config.timer.drain_budget = 1024;
    config.timer.initial_reserve = 8192;
    return config;
}

void wait_for_active_threads(af::runtime &runtime, std::uint16_t expected) {
    while (runtime.active_thread_count() != expected) {
        std::this_thread::yield();
    }
}

void wait_zero(std::atomic<int> &remaining) {
    while (remaining.load(std::memory_order_acquire) != 0) {
        const int observed = remaining.load(std::memory_order_acquire);
        if (observed != 0) {
            af::detail::atomic_wait_value(remaining, observed, std::memory_order_acquire);
        }
    }
}

void BM_RuntimeTimerScheduleAfter(benchmark::State &state, af::timer_kind timer_kind) {
    af::runtime runtime(make_timer_runtime_config(timer_kind));
    if (!runtime.start()) {
        state.SkipWithError("af::runtime::start failed");
        return;
    }
    wait_for_active_threads(runtime, 1);
    const af::thread_ref target = runtime.cpu_threads().at(0);

    for (auto _ : state) {
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{task_count};
        for (int i = 0; i < task_count; ++i) {
            auto task = af::make_task<RuntimeTimerTask>(runtime);
            if (!task->do_it(target, remaining)) {
                state.SkipWithError("RuntimeTimerTask::do_it failed");
                break;
            }
        }
        wait_zero(remaining);
    }

    runtime.stop();
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_RuntimeMinHeapTimerScheduleAfter(benchmark::State &state) {
    BM_RuntimeTimerScheduleAfter(state, af::timer_kind::min_heap);
}

void BM_RuntimeHierarchicalWheelTimerScheduleAfter(benchmark::State &state) {
    BM_RuntimeTimerScheduleAfter(state, af::timer_kind::hierarchical_wheel);
}

BENCHMARK(BM_RuntimeMinHeapTimerScheduleAfter)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeHierarchicalWheelTimerScheduleAfter)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

} // namespace
