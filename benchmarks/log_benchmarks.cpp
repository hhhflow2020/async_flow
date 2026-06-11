#include "runtime_benchmark_support.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>

#include <benchmark/benchmark.h>

#include "af/log.hpp"
#include "af/detail/log/async_log_record_pool.hpp"

namespace {

class CountingLogBackend final : public af::log_backend {
public:
    void write_batch(af::span<af::detail::LogRecord *const> records) noexcept override {
        records_.fetch_add(records.size(), std::memory_order_relaxed);
    }

    [[nodiscard]] std::uint64_t record_count() const noexcept {
        return records_.load(std::memory_order_acquire);
    }

private:
    std::atomic<std::uint64_t> records_{0};
};

void run_async_logger_external_producer_benchmark(benchmark::State &state,
                                                  af::log_ordering ordering) {
    const int producer_count = static_cast<int>(state.range(0));
    const int records_per_producer = static_cast<int>(state.range(1));
    const auto total_records = static_cast<std::uint64_t>(producer_count) *
                               static_cast<std::uint64_t>(records_per_producer);
    constexpr std::string_view message = "asyncflow benchmark log message";

    af::runtime runtime(af_bench::runtime::make_runtime_config());
    if (!runtime.start()) {
        state.SkipWithError("af::runtime::start failed");
        return;
    }
    af_bench::runtime::wait_for_active_threads(runtime);
    const auto threads = af_bench::runtime::select_threads(runtime);

    auto backend = std::make_unique<CountingLogBackend>();
    auto *counting_backend = backend.get();

    const auto producer_shard_count = static_cast<std::size_t>(producer_count);
    af::async_log_config config = ordering == af::log_ordering::ordered
                                      ? af::async_log_config::ordered(producer_shard_count)
                                      : af::async_log_config::relaxed(0U, producer_shard_count);
    config.queue_capacity = static_cast<std::size_t>(total_records + 1024U);
    config.max_batch_size = 256;
    config.max_consumer_batches_per_run = 1024;
    config.overflow_policy = af::log_overflow_policy::drop_newest;
    config.backends.push_back(std::move(backend));

    auto logger = std::make_shared<af::async_logger>(std::move(config));
    af::detail::RuntimeInstanceAsyncLogConsumerController consumer(runtime, logger,
                                                                   threads.io_0.index, 1024);
    if (!consumer.start()) {
        runtime.stop();
        state.SkipWithError("failed to start async log consumer");
        return;
    }

    std::atomic<int> ready{0};
    std::atomic<int> start_epoch{0};
    std::atomic<int> finished{0};
    std::atomic<int> failures{0};
    std::atomic<bool> stop{false};
    std::vector<std::thread> producers;
    producers.reserve(static_cast<std::size_t>(producer_count));
    for (int producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            static_cast<void>(producer);
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
                for (int index = 0; index < records_per_producer; ++index) {
                    if (!logger->try_log(message)) {
                        failures.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                finished.fetch_add(1, std::memory_order_release);
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != producer_count) {
        std::this_thread::yield();
    }

    int epoch = 0;
    bool failed = false;
    for (auto _ : state) {
        const std::uint64_t before = counting_backend->record_count();
        failures.store(0, std::memory_order_relaxed);
        finished.store(0, std::memory_order_relaxed);
        start_epoch.store(++epoch, std::memory_order_release);

        while (finished.load(std::memory_order_acquire) != producer_count) {
            std::this_thread::yield();
        }

        if (failures.load(std::memory_order_acquire) != 0) {
            state.SkipWithError("async logger dropped records during benchmark");
            failed = true;
            break;
        }

        if (!logger->flush(std::chrono::seconds(10))) {
            state.SkipWithError("async logger flush timed out during benchmark");
            failed = true;
            break;
        }

        const std::uint64_t written = counting_backend->record_count() - before;
        if (written != total_records) {
            state.SkipWithError("async logger backend record count mismatch");
            failed = true;
            break;
        }
    }

    stop.store(true, std::memory_order_release);
    start_epoch.store(epoch + 1, std::memory_order_release);
    for (std::thread &producer : producers) {
        producer.join();
    }
    consumer.shutdown(std::chrono::seconds(10));
    logger.reset();
    runtime.stop();

    if (!failed) {
        state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(total_records));
        state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(total_records) *
                                static_cast<std::int64_t>(message.size()));
    }
}

void BM_AsyncLoggerOrderedExternalProducers(benchmark::State &state) {
    run_async_logger_external_producer_benchmark(state, af::log_ordering::ordered);
}

void BM_AsyncLoggerRelaxedExternalProducers(benchmark::State &state) {
    run_async_logger_external_producer_benchmark(state, af::log_ordering::relaxed);
}

void BM_AsyncLogRecordPoolAcquireRelease(benchmark::State &state) {
    constexpr std::string_view message = "asyncflow benchmark log message";
    const auto local_cache_capacity = static_cast<std::size_t>(state.range(0));
    af::detail::AsyncLogRecordPool pool(4096, local_cache_capacity);

    for (auto _ : state) {
        af::detail::LogRecord *record = pool.try_acquire(message);
        if (record == nullptr) [[unlikely]] {
            state.SkipWithError("async log record pool acquire failed");
            break;
        }
        benchmark::DoNotOptimize(record->message().data());
        af::detail::release_async_log_record(record);
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(message.size()));
}

void BM_AsyncLogRecordPoolBatchAcquireRelease(benchmark::State &state) {
    constexpr std::string_view message = "asyncflow benchmark log message";
    const auto batch_size = static_cast<std::size_t>(state.range(0));
    const auto local_cache_capacity = static_cast<std::size_t>(state.range(1));
    af::detail::AsyncLogRecordPool pool(batch_size * 2U + 1U, local_cache_capacity);
    std::vector<af::detail::LogRecord *> records(batch_size);

    bool failed = false;
    for (auto _ : state) {
        std::size_t acquired = 0;
        for (std::size_t i = 0; i < records.size(); ++i) {
            records[i] = pool.try_acquire(message);
            if (records[i] == nullptr) [[unlikely]] {
                state.SkipWithError("async log record pool batch acquire failed");
                failed = true;
                break;
            }
            ++acquired;
            benchmark::DoNotOptimize(records[i]->message().data());
        }
        if (failed) {
            af::detail::release_async_log_records(
                af::span<af::detail::LogRecord *const>(records.data(), acquired));
            break;
        }
        af::detail::release_async_log_records(
            af::span<af::detail::LogRecord *const>(records.data(), records.size()));
    }

    if (!failed) {
        state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(batch_size));
        state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(batch_size) *
                                static_cast<std::int64_t>(message.size()));
    }
}

void BM_AsyncLogRecordPoolCrossThreadReleaseBatch(benchmark::State &state) {
    constexpr std::string_view message = "asyncflow benchmark log message";
    const auto batch_size = static_cast<std::size_t>(state.range(0));
    const auto local_cache_capacity = static_cast<std::size_t>(state.range(1));
    af::detail::AsyncLogRecordPool pool(batch_size * 2U + 1U, local_cache_capacity);
    std::vector<af::detail::LogRecord *> records(batch_size);
    std::atomic<std::uint64_t> published{0};
    std::atomic<std::uint64_t> consumed{0};
    std::atomic<bool> stop{false};

    std::thread releaser([&] {
        std::uint64_t seen = 0;
        for (;;) {
            std::uint64_t target = published.load(std::memory_order_acquire);
            while (target == seen) {
                if (stop.load(std::memory_order_acquire)) {
                    return;
                }
                std::this_thread::yield();
                target = published.load(std::memory_order_acquire);
            }
            if (stop.load(std::memory_order_acquire)) {
                return;
            }

            af::detail::release_async_log_records(
                af::span<af::detail::LogRecord *const>(records.data(), records.size()));
            seen = target;
            consumed.store(target, std::memory_order_release);
        }
    });

    bool failed = false;
    std::uint64_t iteration = 0;
    for (auto _ : state) {
        std::size_t acquired = 0;
        for (std::size_t i = 0; i < records.size(); ++i) {
            records[i] = pool.try_acquire(message);
            if (records[i] == nullptr) [[unlikely]] {
                state.SkipWithError("async log record pool cross-thread acquire failed");
                failed = true;
                break;
            }
            ++acquired;
            benchmark::DoNotOptimize(records[i]->message().data());
        }
        if (failed) {
            af::detail::release_async_log_records(
                af::span<af::detail::LogRecord *const>(records.data(), acquired));
            break;
        }

        published.store(++iteration, std::memory_order_release);
        while (consumed.load(std::memory_order_acquire) != iteration) {
            std::this_thread::yield();
        }
    }

    stop.store(true, std::memory_order_release);
    releaser.join();

    if (!failed) {
        state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(batch_size));
        state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(batch_size) *
                                static_cast<std::int64_t>(message.size()));
    }
}

BENCHMARK(BM_AsyncLoggerOrderedExternalProducers)
    ->Args({1, 8192})
    ->Args({4, 8192})
    ->Args({8, 8192})
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_AsyncLoggerRelaxedExternalProducers)
    ->Args({1, 8192})
    ->Args({4, 8192})
    ->Args({8, 8192})
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_AsyncLogRecordPoolAcquireRelease)->Arg(0)->Arg(256)->Arg(1024);

BENCHMARK(BM_AsyncLogRecordPoolBatchAcquireRelease)
    ->Args({64, 0})
    ->Args({64, 256})
    ->Args({256, 256})
    ->Args({1024, 1024});

BENCHMARK(BM_AsyncLogRecordPoolCrossThreadReleaseBatch)
    ->Args({64, 0})
    ->Args({64, 256})
    ->Args({256, 256})
    ->Args({1024, 1024})
    ->UseRealTime();

} // namespace
