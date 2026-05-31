#include "runtime_benchmark_support.hpp"

#include <atomic>
#include <cstdint>

#include <benchmark/benchmark.h>

namespace {

void BM_RuntimeParallelShards(benchmark::State& state) {
    af_bench::runtime::Runtime::init();
    for (auto _ : state) {
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{0};
        std::atomic<std::uint64_t> sum{0};
        bool launch_failed = false;
        for (int i = 0; i < task_count; ++i) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            const bool ok =
                af_bench::runtime::Runtime::start_task<af_bench::runtime::ParallelShardTask>(
                    &remaining,
                    &sum);
            if (!ok) {
                af_bench::runtime::undo_remaining(remaining);
                state.SkipWithError("Runtime::start_task<ParallelShardTask> failed");
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
    af_bench::runtime::Runtime::shutdown();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_RuntimeParallelShards)
    ->Arg(128)
    ->Arg(512)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

} // namespace
