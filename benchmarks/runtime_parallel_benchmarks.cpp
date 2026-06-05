#include "runtime_benchmark_support.hpp"

#include <atomic>
#include <cstdint>

#include <benchmark/benchmark.h>

namespace {

void BM_RuntimeParallelShards(benchmark::State &state) {
    af::runtime runtime(af_bench::runtime::make_runtime_config());
    if (!runtime.start()) {
        state.SkipWithError("af::runtime::start failed");
        return;
    }
    af_bench::runtime::wait_for_active_threads(runtime);
    const auto threads = af_bench::runtime::select_threads(runtime);

    for (auto _ : state) {
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{0};
        std::atomic<std::uint64_t> sum{0};
        bool launch_failed = false;
        for (int i = 0; i < task_count; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            auto task = af::make_task<af_bench::runtime::ParallelShardTask>(runtime);
            if (!task->do_it(threads, &remaining, &sum)) {
                af_bench::runtime::mark_one_done(remaining);
                state.SkipWithError("ParallelShardTask::do_it failed");
                launch_failed = true;
                break;
            }
        }
        af_bench::runtime::wait_zero(remaining);
        benchmark::DoNotOptimize(sum.load(std::memory_order_relaxed));
        if (launch_failed) {
            break;
        }
    }
    runtime.stop();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_RuntimeParallelShards)
    ->Arg(128)
    ->Arg(512)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

} // namespace
