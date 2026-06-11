#include <atomic>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include <benchmark/benchmark.h>

#include "af/detail/queue/bounded_queues.hpp"
#include "af/detail/memory/object_pool.hpp"

namespace {

[[nodiscard]] std::vector<std::size_t> shuffled_indices(std::size_t count) {
    std::vector<std::size_t> indices(count);
    for (std::size_t i = 0; i < count; ++i) {
        indices[i] = i;
    }

    std::uint64_t state = 0x9e3779b97f4a7c15ULL ^ count;
    for (std::size_t i = count; i > 1U; --i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        const auto swap_index = static_cast<std::size_t>(state % i);
        std::swap(indices[i - 1U], indices[swap_index]);
    }
    return indices;
}

void BM_MpscQueuePushPop(benchmark::State &state) {
    af::detail::bounded_mpsc_queue<int> queue(65536);
    int value = 42;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            benchmark::DoNotOptimize(queue.try_push(&value));
            benchmark::DoNotOptimize(queue.try_pop());
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_MpscQueuePushManyPopMany(benchmark::State &state) {
    af::detail::bounded_mpsc_queue<int> queue(65536);
    std::vector<int> values(static_cast<std::size_t>(state.range(0)));
    std::vector<int *> inputs(values.size());
    std::vector<int *> outputs(values.size());
    for (std::size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<int>(i);
        inputs[i] = &values[i];
    }

    for (auto _ : state) {
        std::size_t pushed = 0;
        while (pushed < inputs.size()) {
            pushed += queue.try_push_many(inputs.data() + pushed, inputs.size() - pushed);
        }

        std::size_t popped = 0;
        while (popped < outputs.size()) {
            popped += queue.try_pop_many(outputs.data() + popped, outputs.size() - popped);
        }
        benchmark::DoNotOptimize(outputs.data());
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_MpscQueueConcurrentProducers(benchmark::State &state) {
    constexpr int producer_count = 4;
    const int values_per_producer = static_cast<int>(state.range(0));
    const int total_values = producer_count * values_per_producer;

    af::detail::bounded_mpsc_queue<int> queue(65536);
    std::vector<std::vector<int>> values(
        producer_count, std::vector<int>(static_cast<std::size_t>(values_per_producer)));
    for (int producer = 0; producer < producer_count; ++producer) {
        for (int i = 0; i < values_per_producer; ++i) {
            values[static_cast<std::size_t>(producer)][static_cast<std::size_t>(i)] =
                producer * values_per_producer + i;
        }
    }

    std::atomic<int> ready{0};
    std::atomic<int> start_epoch{0};
    std::atomic<int> finished_producers{0};
    std::atomic<bool> stop{false};
    std::array<std::thread, producer_count> producers;

    for (int producer = 0; producer < producer_count; ++producer) {
        producers[static_cast<std::size_t>(producer)] = std::thread([&, producer] {
            int seen_epoch = 0;
            ready.fetch_add(1, std::memory_order_release);
            for (;;) {
                int target_epoch = start_epoch.load(std::memory_order_acquire);
                while (target_epoch == seen_epoch && !stop.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                    target_epoch = start_epoch.load(std::memory_order_acquire);
                }
                if (stop.load(std::memory_order_acquire)) {
                    break;
                }

                seen_epoch = target_epoch;
                auto &producer_values = values[static_cast<std::size_t>(producer)];
                for (int i = 0; i < values_per_producer; ++i) {
                    int *value = &producer_values[static_cast<std::size_t>(i)];
                    while (!queue.try_push(value)) {
                        std::this_thread::yield();
                    }
                }
                finished_producers.fetch_add(1, std::memory_order_release);
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != producer_count) {
        std::this_thread::yield();
    }

    int epoch = 0;
    for (auto _ : state) {
        finished_producers.store(0, std::memory_order_relaxed);
        start_epoch.store(++epoch, std::memory_order_release);

        int popped = 0;
        while (popped < total_values) {
            if (queue.try_pop() != nullptr) {
                ++popped;
            } else {
                std::this_thread::yield();
            }
        }

        while (finished_producers.load(std::memory_order_acquire) != producer_count) {
            std::this_thread::yield();
        }
    }

    stop.store(true, std::memory_order_release);
    start_epoch.store(epoch + 1, std::memory_order_release);
    for (std::thread &producer : producers) {
        producer.join();
    }

    state.SetItemsProcessed(state.iterations() * total_values);
}

void BM_ObjectPoolCreateDestroy(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    af::detail::ObjectPool<Payload> pool;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto *object = pool.create();
            benchmark::DoNotOptimize(object);
            pool.destroy(object);
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <std::size_t ChunkSize>
void BM_ObjectPoolTunedChunkCreateDestroy(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    af::detail::ObjectPool<Payload, ChunkSize> pool;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto *object = pool.create();
            benchmark::DoNotOptimize(object);
            pool.destroy(object);
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <std::size_t LocalCacheSetSize>
void BM_ObjectPoolTunedCacheSetCreateDestroy(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    using Pool = af::detail::ObjectPool<Payload, 256, 1, false, LocalCacheSetSize>;
    Pool pool;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto *object = pool.create();
            benchmark::DoNotOptimize(object);
            pool.destroy(object);
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_ObjectPoolBatchCreateDestroy(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    af::detail::ObjectPool<Payload> pool;
    std::vector<Payload *> objects(static_cast<std::size_t>(state.range(0)));

    for (auto _ : state) {
        for (std::size_t i = 0; i < objects.size(); ++i) {
            objects[i] = pool.create();
            benchmark::DoNotOptimize(objects[i]);
        }
        for (Payload *object : objects) {
            pool.destroy(object);
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <std::size_t ChunkSize>
void BM_ObjectPoolTunedChunkBatchCreateDestroy(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    af::detail::ObjectPool<Payload, ChunkSize> pool;
    std::vector<Payload *> objects(static_cast<std::size_t>(state.range(0)));

    for (auto _ : state) {
        for (std::size_t i = 0; i < objects.size(); ++i) {
            objects[i] = pool.create();
            benchmark::DoNotOptimize(objects[i]);
        }
        for (Payload *object : objects) {
            pool.destroy(object);
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <std::size_t LocalCacheCapacity>
void BM_ObjectPoolTunedCacheCapacityBatchCreateDestroy(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    using Pool = af::detail::ObjectPool<Payload, 256, 1, false, 8, 4, LocalCacheCapacity>;
    Pool pool;
    std::vector<Payload *> objects(static_cast<std::size_t>(state.range(0)));

    for (auto _ : state) {
        for (std::size_t i = 0; i < objects.size(); ++i) {
            objects[i] = pool.create();
            benchmark::DoNotOptimize(objects[i]);
        }
        for (Payload *object : objects) {
            pool.destroy(object);
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <bool Reserve> void BM_ObjectPoolColdBurstCreateDestroy(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    const auto object_count = static_cast<std::size_t>(state.range(0));
    std::vector<Payload *> objects(object_count);

    for (auto _ : state) {
        state.PauseTiming();
        auto pool = std::make_unique<af::detail::ObjectPool<Payload>>();
        if constexpr (Reserve) {
            pool->reserve_slots(object_count);
        }
        state.ResumeTiming();

        for (std::size_t i = 0; i < objects.size(); ++i) {
            objects[i] = pool->create();
            benchmark::DoNotOptimize(objects[i]);
        }
        for (Payload *object : objects) {
            pool->destroy(object);
        }

        state.PauseTiming();
        pool.reset();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <typename Payload, typename Pool>
void RunObjectPoolCrossThreadDestroyBatch(benchmark::State &state) {
    Pool pool;
    std::vector<Payload *> objects(static_cast<std::size_t>(state.range(0)));
    std::atomic<std::uint64_t> published{0};
    std::atomic<std::uint64_t> consumed{0};
    std::atomic<bool> stop{false};

    std::thread destroyer([&] {
        std::uint64_t seen = 0;
        while (!stop.load(std::memory_order_acquire)) {
            const std::uint64_t target = published.load(std::memory_order_acquire);
            if (target == seen) {
                std::this_thread::yield();
                continue;
            }

            for (Payload *object : objects) {
                pool.destroy(object);
            }

            seen = target;
            consumed.store(target, std::memory_order_release);
        }
    });

    std::uint64_t iteration = 0;
    for (auto _ : state) {
        for (std::size_t i = 0; i < objects.size(); ++i) {
            objects[i] = pool.create();
            benchmark::DoNotOptimize(objects[i]);
        }

        published.store(++iteration, std::memory_order_release);
        while (consumed.load(std::memory_order_acquire) != iteration) {
            std::this_thread::yield();
        }
    }

    stop.store(true, std::memory_order_release);
    destroyer.join();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <typename Payload, typename Pool>
void RunObjectPoolShuffledCrossThreadDestroyBatch(benchmark::State &state) {
    Pool pool;
    const auto object_count = static_cast<std::size_t>(state.range(0));
    std::vector<Payload *> objects(object_count);
    const std::vector<std::size_t> destroy_order = shuffled_indices(object_count);
    std::atomic<std::uint64_t> published{0};
    std::atomic<std::uint64_t> consumed{0};
    std::atomic<bool> stop{false};

    std::thread destroyer([&] {
        std::uint64_t seen = 0;
        while (!stop.load(std::memory_order_acquire)) {
            const std::uint64_t target = published.load(std::memory_order_acquire);
            if (target == seen) {
                std::this_thread::yield();
                continue;
            }

            for (std::size_t index : destroy_order) {
                pool.destroy(objects[index]);
            }

            seen = target;
            consumed.store(target, std::memory_order_release);
        }
    });

    std::uint64_t iteration = 0;
    for (auto _ : state) {
        for (std::size_t i = 0; i < objects.size(); ++i) {
            objects[i] = pool.create();
            benchmark::DoNotOptimize(objects[i]);
        }

        published.store(++iteration, std::memory_order_release);
        while (consumed.load(std::memory_order_acquire) != iteration) {
            std::this_thread::yield();
        }
    }

    stop.store(true, std::memory_order_release);
    destroyer.join();

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <typename Payload, typename Pool>
void RunObjectPoolFanInDestroyBatch(benchmark::State &state) {
    Pool pool;
    const auto object_count = static_cast<std::size_t>(state.range(0));
    const auto destroyer_count = static_cast<std::size_t>(state.range(1));
    std::vector<Payload *> objects(object_count);
    std::vector<std::atomic<std::uint64_t>> consumed(destroyer_count);
    std::atomic<std::uint64_t> published{0};
    std::atomic<bool> stop{false};

    for (auto &value : consumed) {
        value.store(0, std::memory_order_relaxed);
    }

    std::vector<std::thread> destroyers;
    destroyers.reserve(destroyer_count);
    for (std::size_t destroyer_index = 0; destroyer_index < destroyer_count; ++destroyer_index) {
        destroyers.emplace_back([&, destroyer_index] {
            const std::size_t begin = object_count * destroyer_index / destroyer_count;
            const std::size_t end = object_count * (destroyer_index + 1U) / destroyer_count;
            std::uint64_t seen = 0;
            while (!stop.load(std::memory_order_acquire)) {
                const std::uint64_t target = published.load(std::memory_order_acquire);
                if (target == seen) {
                    std::this_thread::yield();
                    continue;
                }

                for (std::size_t i = begin; i < end; ++i) {
                    pool.destroy(objects[i]);
                }

                seen = target;
                consumed[destroyer_index].store(target, std::memory_order_release);
            }
        });
    }

    std::uint64_t iteration = 0;
    for (auto _ : state) {
        for (std::size_t i = 0; i < objects.size(); ++i) {
            objects[i] = pool.create();
            benchmark::DoNotOptimize(objects[i]);
        }

        published.store(++iteration, std::memory_order_release);
        for (std::size_t destroyer_index = 0; destroyer_index < destroyer_count;
             ++destroyer_index) {
            while (consumed[destroyer_index].load(std::memory_order_acquire) != iteration) {
                std::this_thread::yield();
            }
        }
    }

    stop.store(true, std::memory_order_release);
    for (std::thread &destroyer : destroyers) {
        destroyer.join();
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <typename Payload, typename Pool, std::size_t PoolCount>
void RunObjectPoolRoundRobinCrossThreadDestroyBatch(benchmark::State &state) {
    std::array<Pool, PoolCount> pools;
    const auto object_count = static_cast<std::size_t>(state.range(0));
    std::array<std::vector<Payload *>, PoolCount> objects;
    for (auto &pool_objects : objects) {
        pool_objects.resize(object_count);
    }

    std::atomic<std::uint64_t> published{0};
    std::atomic<std::uint64_t> consumed{0};
    std::atomic<bool> stop{false};

    std::thread destroyer([&] {
        std::uint64_t seen = 0;
        while (!stop.load(std::memory_order_acquire)) {
            const std::uint64_t target = published.load(std::memory_order_acquire);
            if (target == seen) {
                std::this_thread::yield();
                continue;
            }

            for (std::size_t i = 0; i < object_count; ++i) {
                for (std::size_t pool_index = 0; pool_index < PoolCount; ++pool_index) {
                    pools[pool_index].destroy(objects[pool_index][i]);
                }
            }

            seen = target;
            consumed.store(target, std::memory_order_release);
        }
    });

    std::uint64_t iteration = 0;
    for (auto _ : state) {
        for (std::size_t pool_index = 0; pool_index < PoolCount; ++pool_index) {
            for (std::size_t i = 0; i < object_count; ++i) {
                objects[pool_index][i] = pools[pool_index].create();
                benchmark::DoNotOptimize(objects[pool_index][i]);
            }
        }

        published.store(++iteration, std::memory_order_release);
        while (consumed.load(std::memory_order_acquire) != iteration) {
            std::this_thread::yield();
        }
    }

    stop.store(true, std::memory_order_release);
    destroyer.join();

    state.SetItemsProcessed(state.iterations() * state.range(0) *
                            static_cast<std::int64_t>(PoolCount));
}

void BM_ObjectPoolCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    RunObjectPoolCrossThreadDestroyBatch<Payload, af::detail::ObjectPool<Payload>>(state);
}

void BM_ObjectPoolFanInCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    RunObjectPoolFanInDestroyBatch<Payload, af::detail::ObjectPool<Payload>>(state);
}

template <std::size_t PoolCount>
void BM_ObjectPoolRoundRobinCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    RunObjectPoolRoundRobinCrossThreadDestroyBatch<Payload, af::detail::ObjectPool<Payload>,
                                                   PoolCount>(state);
}

template <std::size_t PoolCount, std::size_t DirectReleaseSetSize>
void BM_ObjectPoolTunedDirectSetRoundRobinCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    using Pool = af::detail::ObjectPool<Payload, 256, 1, false, 8, DirectReleaseSetSize>;
    RunObjectPoolRoundRobinCrossThreadDestroyBatch<Payload, Pool, PoolCount>(state);
}

void BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    RunObjectPoolCrossThreadDestroyBatch<Payload, af::detail::ObjectPool<Payload, 256, 64>>(state);
}

template <std::size_t ChunkSize>
void BM_ObjectPoolTunedChunkCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    RunObjectPoolCrossThreadDestroyBatch<Payload, af::detail::ObjectPool<Payload, ChunkSize>>(
        state);
}

template <std::size_t ChunkSize>
void BM_ObjectPoolTunedChunkRemoteBatchCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    RunObjectPoolCrossThreadDestroyBatch<Payload, af::detail::ObjectPool<Payload, ChunkSize, 64>>(
        state);
}

template <std::size_t ChunkSize>
void BM_ObjectPoolTunedChunkRemoteBatchShuffledCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    RunObjectPoolShuffledCrossThreadDestroyBatch<Payload,
                                                 af::detail::ObjectPool<Payload, ChunkSize, 64>>(
        state);
}

void BM_ObjectPoolCachedIndexRemoteBatchCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    RunObjectPoolCrossThreadDestroyBatch<Payload, af::detail::ObjectPool<Payload, 256, 64, true>>(
        state);
}

void BM_ObjectPoolRemoteBatchFanInCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    RunObjectPoolFanInDestroyBatch<Payload, af::detail::ObjectPool<Payload, 256, 64>>(state);
}

template <std::size_t RemoteBatchSize>
void BM_ObjectPoolRemoteBatchSizeCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    RunObjectPoolCrossThreadDestroyBatch<Payload,
                                         af::detail::ObjectPool<Payload, 256, RemoteBatchSize>>(
        state);
}

template <std::size_t RemoteBatchSize, std::size_t LocalCacheCapacity>
void BM_ObjectPoolRemoteBatchCapacityCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    using Pool =
        af::detail::ObjectPool<Payload, 256, RemoteBatchSize, false, 8, 4, LocalCacheCapacity>;
    RunObjectPoolCrossThreadDestroyBatch<Payload, Pool>(state);
}

template <std::size_t RemoteBatchSize, std::size_t LocalCacheCapacity>
void BM_ObjectPoolRemoteBatchCapacityFanInCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    using Pool =
        af::detail::ObjectPool<Payload, 256, RemoteBatchSize, false, 8, 4, LocalCacheCapacity>;
    RunObjectPoolFanInDestroyBatch<Payload, Pool>(state);
}

void BM_ObjectPoolTinyCreateDestroy(benchmark::State &state) {
    struct Payload {
        std::uint64_t value{0};
    };

    af::detail::ObjectPool<Payload> pool;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto *object = pool.create();
            benchmark::DoNotOptimize(object);
            pool.destroy(object);
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_ObjectPoolTinyBatchCreateDestroy(benchmark::State &state) {
    struct Payload {
        std::uint64_t value{0};
    };

    af::detail::ObjectPool<Payload> pool;
    std::vector<Payload *> objects(static_cast<std::size_t>(state.range(0)));

    for (auto _ : state) {
        for (std::size_t i = 0; i < objects.size(); ++i) {
            objects[i] = pool.create();
            benchmark::DoNotOptimize(objects[i]);
        }
        for (Payload *object : objects) {
            pool.destroy(object);
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_ObjectPoolTinyCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t value{0};
    };

    RunObjectPoolCrossThreadDestroyBatch<Payload, af::detail::ObjectPool<Payload>>(state);
}

void BM_ObjectPoolTinyRemoteBatchCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t value{0};
    };

    RunObjectPoolCrossThreadDestroyBatch<Payload, af::detail::ObjectPool<Payload, 256, 64>>(state);
}

template <std::size_t RemoteBatchSize>
void BM_ObjectPoolTinyRemoteBatchSizeCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t value{0};
    };

    RunObjectPoolCrossThreadDestroyBatch<Payload,
                                         af::detail::ObjectPool<Payload, 256, RemoteBatchSize>>(
        state);
}

template <std::size_t RemoteBatchSize, std::size_t LocalCacheCapacity>
void BM_ObjectPoolTinyRemoteBatchCapacityCrossThreadDestroyBatch(benchmark::State &state) {
    struct Payload {
        std::uint64_t value{0};
    };

    using Pool =
        af::detail::ObjectPool<Payload, 256, RemoteBatchSize, false, 8, 4, LocalCacheCapacity>;
    RunObjectPoolCrossThreadDestroyBatch<Payload, Pool>(state);
}

void BM_ObjectPoolAlternatingPools(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    af::detail::ObjectPool<Payload> first_pool;
    af::detail::ObjectPool<Payload> second_pool;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto *first = first_pool.create();
            benchmark::DoNotOptimize(first);
            first_pool.destroy(first);

            auto *second = second_pool.create();
            benchmark::DoNotOptimize(second);
            second_pool.destroy(second);
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0) * 2);
}

template <std::size_t PoolCount> void BM_ObjectPoolAlternatingPoolSet(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    std::array<af::detail::ObjectPool<Payload>, PoolCount> pools;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            for (auto &pool : pools) {
                auto *object = pool.create();
                benchmark::DoNotOptimize(object);
                pool.destroy(object);
            }
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0) *
                            static_cast<std::int64_t>(PoolCount));
}

template <std::size_t PoolCount, std::size_t LocalCacheSetSize>
void BM_ObjectPoolTunedCacheSetAlternatingPoolSet(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    using Pool = af::detail::ObjectPool<Payload, 256, 1, false, LocalCacheSetSize>;
    std::array<Pool, PoolCount> pools;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            for (auto &pool : pools) {
                auto *object = pool.create();
                benchmark::DoNotOptimize(object);
                pool.destroy(object);
            }
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0) *
                            static_cast<std::int64_t>(PoolCount));
}

template <std::size_t PoolCount>
void BM_ObjectPoolReverseAlternatingPoolSet(benchmark::State &state) {
    struct Payload {
        std::uint64_t values[8]{};
    };

    std::array<af::detail::ObjectPool<Payload>, PoolCount> pools;

    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            for (std::size_t pool_index = PoolCount; pool_index-- > 0U;) {
                auto *object = pools[pool_index].create();
                benchmark::DoNotOptimize(object);
                pools[pool_index].destroy(object);
            }
        }
    }

    state.SetItemsProcessed(state.iterations() * state.range(0) *
                            static_cast<std::int64_t>(PoolCount));
}

BENCHMARK(BM_MpscQueuePushPop)->Arg(1024)->Arg(16384);
BENCHMARK(BM_MpscQueuePushManyPopMany)->Arg(1024)->Arg(16384);
BENCHMARK(BM_MpscQueueConcurrentProducers)
    ->Arg(256)
    ->Arg(4096)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_ObjectPoolCreateDestroy)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkCreateDestroy, 256)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkCreateDestroy, 128)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkCreateDestroy, 512)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkCreateDestroy, 1024)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedCacheSetCreateDestroy, 1)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedCacheSetCreateDestroy, 2)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedCacheSetCreateDestroy, 4)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedCacheSetCreateDestroy, 8)->Arg(1024)->Arg(16384);
BENCHMARK(BM_ObjectPoolBatchCreateDestroy)
    ->Arg(64)
    ->Arg(65)
    ->Arg(96)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkBatchCreateDestroy, 256)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkBatchCreateDestroy, 128)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkBatchCreateDestroy, 512)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkBatchCreateDestroy, 1024)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedCacheCapacityBatchCreateDestroy, 128)
    ->Arg(64)
    ->Arg(65)
    ->Arg(96)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedCacheCapacityBatchCreateDestroy, 256)
    ->Arg(128)
    ->Arg(256)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolColdBurstCreateDestroy, false)->Arg(256)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolColdBurstCreateDestroy, true)->Arg(256)->Arg(1024)->Arg(16384);
BENCHMARK(BM_ObjectPoolCrossThreadDestroyBatch)->Arg(1024)->Arg(16384);
BENCHMARK(BM_ObjectPoolRemoteBatchCrossThreadDestroyBatch)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkCrossThreadDestroyBatch, 256)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkCrossThreadDestroyBatch, 128)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkCrossThreadDestroyBatch, 512)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkCrossThreadDestroyBatch, 1024)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkRemoteBatchCrossThreadDestroyBatch, 256)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkRemoteBatchCrossThreadDestroyBatch, 128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkRemoteBatchCrossThreadDestroyBatch, 512)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkRemoteBatchCrossThreadDestroyBatch, 1024)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkRemoteBatchShuffledCrossThreadDestroyBatch, 256)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkRemoteBatchShuffledCrossThreadDestroyBatch, 512)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedChunkRemoteBatchShuffledCrossThreadDestroyBatch, 1024)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK(BM_ObjectPoolCachedIndexRemoteBatchCrossThreadDestroyBatch)->Arg(1024)->Arg(16384);
BENCHMARK(BM_ObjectPoolFanInCrossThreadDestroyBatch)
    ->Args({4096, 4})
    ->Args({16384, 4})
    ->Args({16384, 8});
BENCHMARK_TEMPLATE(BM_ObjectPoolRoundRobinCrossThreadDestroyBatch, 2)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolRoundRobinCrossThreadDestroyBatch, 4)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolRoundRobinCrossThreadDestroyBatch, 8)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolRoundRobinCrossThreadDestroyBatch, 16)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedDirectSetRoundRobinCrossThreadDestroyBatch, 8, 8)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedDirectSetRoundRobinCrossThreadDestroyBatch, 16, 16)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK(BM_ObjectPoolRemoteBatchFanInCrossThreadDestroyBatch)
    ->Args({4096, 4})
    ->Args({16384, 4})
    ->Args({16384, 8});
BENCHMARK_TEMPLATE(BM_ObjectPoolRemoteBatchSizeCrossThreadDestroyBatch, 4)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolRemoteBatchSizeCrossThreadDestroyBatch, 8)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolRemoteBatchSizeCrossThreadDestroyBatch, 16)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolRemoteBatchSizeCrossThreadDestroyBatch, 32)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolRemoteBatchSizeCrossThreadDestroyBatch, 64)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolRemoteBatchCapacityCrossThreadDestroyBatch, 128, 128)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolRemoteBatchCapacityFanInCrossThreadDestroyBatch, 128, 128)
    ->Args({4096, 4})
    ->Args({16384, 4})
    ->Args({16384, 8});
BENCHMARK(BM_ObjectPoolTinyCreateDestroy)->Arg(1024)->Arg(16384);
BENCHMARK(BM_ObjectPoolTinyBatchCreateDestroy)->Arg(1024)->Arg(16384);
BENCHMARK(BM_ObjectPoolTinyCrossThreadDestroyBatch)->Arg(1024)->Arg(16384);
BENCHMARK(BM_ObjectPoolTinyRemoteBatchCrossThreadDestroyBatch)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTinyRemoteBatchSizeCrossThreadDestroyBatch, 4)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTinyRemoteBatchSizeCrossThreadDestroyBatch, 8)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTinyRemoteBatchSizeCrossThreadDestroyBatch, 16)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTinyRemoteBatchSizeCrossThreadDestroyBatch, 32)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTinyRemoteBatchSizeCrossThreadDestroyBatch, 64)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTinyRemoteBatchCapacityCrossThreadDestroyBatch, 128, 128)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Arg(1024)
    ->Arg(16384);
BENCHMARK(BM_ObjectPoolAlternatingPools)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolAlternatingPoolSet, 2)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolAlternatingPoolSet, 4)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolAlternatingPoolSet, 8)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolAlternatingPoolSet, 16)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolTunedCacheSetAlternatingPoolSet, 16, 16)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolReverseAlternatingPoolSet, 2)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolReverseAlternatingPoolSet, 4)->Arg(1024)->Arg(16384);
BENCHMARK_TEMPLATE(BM_ObjectPoolReverseAlternatingPoolSet, 8)->Arg(1024)->Arg(16384);

} // namespace
