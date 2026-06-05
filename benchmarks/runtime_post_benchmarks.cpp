#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>

#include <benchmark/benchmark.h>

#include "af/detail/runtime/atomic_wait.hpp"
#include "af/runtime.hpp"
#include "af/runtime_config.hpp"

namespace {

[[nodiscard]] af::runtime_config make_post_runtime_config(std::size_t local_cache_size) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("post", 1)};
    config.logger.consumer_thread = af::thread_selector::cpu(0);
    config.scheduler.task_drain_budget = 4096;
    config.task_pool.local_cache_size = local_cache_size;
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

void mark_one_done(std::atomic<int> &remaining) noexcept {
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        af::detail::atomic_notify_one(remaining);
    }
}

void BM_RuntimePostCallable(benchmark::State &state, std::size_t local_cache_size) {
    af::runtime runtime(make_post_runtime_config(local_cache_size));
    if (!runtime.start()) {
        state.SkipWithError("af::runtime::start failed");
        return;
    }
    wait_for_active_threads(runtime, 1);
    const af::thread_ref target = runtime.cpu_threads().front();

    for (auto _ : state) {
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{task_count};
        bool launch_failed = false;
        for (int i = 0; i < task_count; ++i) {
            if (!runtime.post(target, [&remaining]() noexcept { mark_one_done(remaining); })) {
                mark_one_done(remaining);
                state.SkipWithError("af::runtime::post callable failed");
                launch_failed = true;
                break;
            }
        }
        wait_zero(remaining);
        if (launch_failed) {
            break;
        }
    }

    runtime.stop();
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_RuntimePostCallableCache4(benchmark::State &state) {
    BM_RuntimePostCallable(state, 4);
}

void BM_RuntimePostCallableCache256(benchmark::State &state) {
    BM_RuntimePostCallable(state, 256);
}

void BM_RuntimePostCallableCache1024(benchmark::State &state) {
    BM_RuntimePostCallable(state, 1024);
}

BENCHMARK(BM_RuntimePostCallableCache4)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimePostCallableCache256)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimePostCallableCache1024)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

} // namespace
