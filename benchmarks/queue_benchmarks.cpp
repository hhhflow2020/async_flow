#include <cstdint>

#include <benchmark/benchmark.h>

#include "af/detail/bounded_queues.hpp"
#include "af/detail/object_pool.hpp"

namespace {

void BM_SpscQueuePushPop(benchmark::State& state) {
    af::detail::BoundedSpscQueue<int> queue(65536);
    int value = 42;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            benchmark::DoNotOptimize(queue.try_push(&value));
            benchmark::DoNotOptimize(queue.try_pop());
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_MpscQueuePushPop(benchmark::State& state) {
    af::detail::BoundedMpscQueue<int> queue(65536);
    int value = 42;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            benchmark::DoNotOptimize(queue.try_push(&value));
            benchmark::DoNotOptimize(queue.try_pop());
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_ObjectPoolCreateDestroy(benchmark::State& state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    af::detail::ObjectPool<Payload> pool;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto* object = pool.create();
            benchmark::DoNotOptimize(object);
            pool.destroy(object);
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

BENCHMARK(BM_SpscQueuePushPop)->Arg(1024)->Arg(16384);
BENCHMARK(BM_MpscQueuePushPop)->Arg(1024)->Arg(16384);
BENCHMARK(BM_ObjectPoolCreateDestroy)->Arg(1024)->Arg(16384);

} // namespace
