#include "runtime_benchmark_support.hpp"

#include <atomic>

#include <benchmark/benchmark.h>

namespace {

void BM_RuntimeExternalStart(benchmark::State &state) {
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
        bool launch_failed = false;
        for (int i = 0; i < task_count; ++i) {
            const auto thread = threads.logic.shard(static_cast<std::size_t>(i));
            remaining.fetch_add(1, std::memory_order_relaxed);
            auto task = af::make_task<af_bench::runtime::CountTask>(runtime);
            if (!task->do_it(thread, &remaining)) {
                af_bench::runtime::mark_one_done(remaining);
                state.SkipWithError("CountTask::do_it failed");
                launch_failed = true;
                break;
            }
        }
        af_bench::runtime::wait_zero(remaining);
        if (launch_failed) {
            break;
        }
    }
    runtime.stop();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_RuntimeExternalStart)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

} // namespace
