#include "runtime_benchmark_support.hpp"

#include <atomic>

#include <benchmark/benchmark.h>

namespace {

void BM_RuntimeExternalStart(benchmark::State& state) {
    af_bench::runtime::Runtime::init();
    for (auto _ : state) {
        const int task_count = static_cast<int>(state.range(0));
        std::atomic<int> remaining{0};
        bool launch_failed = false;
        for (int i = 0; i < task_count; ++i) {
            const auto thread = static_cast<af_bench::runtime::BenchThread>(i & 3);
            remaining.fetch_add(1, std::memory_order_relaxed);
            const bool ok =
                af_bench::runtime::Runtime::start_task<af_bench::runtime::CountTask>(
                    thread,
                    &remaining);
            if (!ok) {
                af_bench::runtime::undo_remaining(remaining);
                state.SkipWithError("Runtime::start_task<CountTask> failed");
                launch_failed = true;
                break;
            }
        }
        af_bench::runtime::wait_zero(remaining);
        if (launch_failed) {
            break;
        }
    }
    af_bench::runtime::Runtime::shutdown();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_RuntimeExternalStart)->Arg(1024)->Arg(8192)->Unit(benchmark::kMillisecond);

} // namespace
