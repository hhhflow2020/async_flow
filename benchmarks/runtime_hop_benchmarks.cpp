#include "runtime_benchmark_support.hpp"

#include <atomic>
#include <cstddef>

#include <benchmark/benchmark.h>

namespace {

void run_cross_thread_hop_benchmark(benchmark::State &state, std::size_t local_cache_size,
                                    std::size_t slab_object_count) {
    af::runtime runtime(
        af_bench::runtime::make_runtime_config(local_cache_size, slab_object_count));
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
            remaining.fetch_add(1, std::memory_order_relaxed);
            auto task = af::make_task<af_bench::runtime::HopTask>(runtime);
            if (!task->do_it(threads, 8, &remaining)) {
                af_bench::runtime::mark_one_done(remaining);
                state.SkipWithError("HopTask::do_it failed");
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

void run_same_thread_benchmark(benchmark::State &state) {
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
            remaining.fetch_add(1, std::memory_order_relaxed);
            auto task = af::make_task<af_bench::runtime::SameThreadTask>(runtime);
            if (!task->do_it(threads.logic_0, 8, &remaining)) {
                af_bench::runtime::mark_one_done(remaining);
                state.SkipWithError("SameThreadTask::do_it failed");
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

void run_io_thread_hop_benchmark(benchmark::State &state) {
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
            remaining.fetch_add(1, std::memory_order_relaxed);
            auto task = af::make_task<af_bench::runtime::IoHopTask>(runtime);
            if (!task->do_it(threads, &remaining)) {
                af_bench::runtime::mark_one_done(remaining);
                state.SkipWithError("IoHopTask::do_it failed");
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

void BM_RuntimeCrossThreadHop(benchmark::State &state) {
    run_cross_thread_hop_benchmark(state, 256, 4096);
}

void BM_RuntimeCrossThreadHopCache4(benchmark::State &state) {
    run_cross_thread_hop_benchmark(state, 4, 4096);
}

void BM_RuntimeCrossThreadHopCache64(benchmark::State &state) {
    run_cross_thread_hop_benchmark(state, 64, 4096);
}

void BM_RuntimeCrossThreadHopCache1024(benchmark::State &state) {
    run_cross_thread_hop_benchmark(state, 1024, 4096);
}

void BM_RuntimeCrossThreadHopSlab1024(benchmark::State &state) {
    run_cross_thread_hop_benchmark(state, 256, 1024);
}

void BM_RuntimeCrossThreadHopSlab8192(benchmark::State &state) {
    run_cross_thread_hop_benchmark(state, 256, 8192);
}

void BM_RuntimeSameThreadReschedule(benchmark::State &state) {
    run_same_thread_benchmark(state);
}

void BM_RuntimeIoThreadHop(benchmark::State &state) {
    run_io_thread_hop_benchmark(state);
}

BENCHMARK(BM_RuntimeCrossThreadHop)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeCrossThreadHopCache4)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeCrossThreadHopCache64)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeCrossThreadHopCache1024)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeCrossThreadHopSlab1024)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeCrossThreadHopSlab8192)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeSameThreadReschedule)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_RuntimeIoThreadHop)
    ->Arg(1024)
    ->Arg(8192)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

} // namespace
