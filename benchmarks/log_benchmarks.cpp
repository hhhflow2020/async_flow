#include "runtime_benchmark_support.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include "af/span.hpp"
#include <string_view>
#include <thread>
#include <vector>

#include <benchmark/benchmark.h>

namespace {

class CountingLogBackend final : public af::LogBackend {
public:
    void write_batch(af::Span<af::detail::LogRecord *const> records) noexcept override {
        records_.fetch_add(records.size(), std::memory_order_relaxed);
    }

    [[nodiscard]] std::uint64_t record_count() const noexcept {
        return records_.load(std::memory_order_acquire);
    }

private:
    std::atomic<std::uint64_t> records_{0};
};

void run_async_logger_external_producer_benchmark(benchmark::State &state,
                                                  af::LogOrdering ordering) {
    using Runtime = af_bench::runtime::Runtime;

    const int producer_count = static_cast<int>(state.range(0));
    const int records_per_producer = static_cast<int>(state.range(1));
    const auto total_records = static_cast<std::uint64_t>(producer_count) *
                               static_cast<std::uint64_t>(records_per_producer);
    constexpr std::string_view message = "asyncflow benchmark log message";

    Runtime::init();

    auto backend = std::make_unique<CountingLogBackend>();
    auto *counting_backend = backend.get();

    const auto producer_shard_count = static_cast<std::size_t>(producer_count);
    af::AsyncLogConfig config = ordering == af::LogOrdering::Ordered
                                    ? af::AsyncLogConfig::ordered(producer_shard_count)
                                    : af::AsyncLogConfig::relaxed(0U, producer_shard_count);
    config.queue_capacity = static_cast<std::size_t>(total_records + 1024U);
    config.max_batch_size = 256;
    config.max_consumer_batches_per_run = 1024;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.backends.push_back(std::move(backend));

    auto logger = std::make_shared<af::AsyncLogger>(std::move(config));
    af::detail::RuntimeAsyncLogConsumerController<Runtime> consumer(
        logger, af_bench::runtime::BenchThreads::IO_0, 1024);
    if (!consumer.start()) {
        Runtime::shutdown();
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
    consumer.shutdown();
    logger.reset();
    Runtime::shutdown();

    if (!failed) {
        state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(total_records));
        state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(total_records) *
                                static_cast<std::int64_t>(message.size()));
    }
}

void BM_AsyncLoggerOrderedExternalProducers(benchmark::State &state) {
    run_async_logger_external_producer_benchmark(state, af::LogOrdering::Ordered);
}

void BM_AsyncLoggerRelaxedExternalProducers(benchmark::State &state) {
    run_async_logger_external_producer_benchmark(state, af::LogOrdering::Relaxed);
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

} // namespace
