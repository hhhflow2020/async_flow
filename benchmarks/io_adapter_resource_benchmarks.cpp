#include "io_benchmark_support.hpp"

namespace {

void BM_IoFileAdapterZeroByteRead(benchmark::State &state) {
    FakeTask task;
    af::IoFile<BenchIoThread> file(BenchIoThreads::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.read_some(task, nullptr, 0, op));
    }
}

void BM_IoTimerAdapterNullExpiration(benchmark::State &state) {
    FakeTask task;
    af::IoTimer<BenchIoThread> timer(BenchIoThreads::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(timer.wait(task, nullptr, op));
    }
}

void BM_IoEventAdapterNullValue(benchmark::State &state) {
    FakeTask task;
    af::IoEvent<BenchIoThread> event(BenchIoThreads::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(event.wait(task, nullptr, op));
    }
}

void BM_IoTimeoutInvalidDelay(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(
            af::io_wait_timeout(task, BenchIoThreads::IO_0, std::chrono::nanoseconds{0}, op));
    }
}

void BM_IoOpenAtNullPath(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    int opened = -1;
    for (auto _ : state) {
        benchmark::DoNotOptimize(
            af::io_openat(task, BenchIoThreads::IO_0, -1, nullptr, 0, 0, &opened, op));
    }
}

void BM_IoSocketNullOutput(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_socket(task, BenchIoThreads::IO_0, 0, 0, 0, nullptr, op));
    }
}

#if !defined(_WIN32)
void BM_IoSocketNameNullOutput(benchmark::State &state) {
    FakeTask task;
    socklen_t size = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_getsockname(task, BenchIoThreads::IO_0, 0, nullptr, &size));
    }
}
#endif

void BM_IoOpenAtDirectInvalidIndex(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    af::IoFixedFile<BenchIoThread> file;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_openat_direct(task, BenchIoThreads::IO_0, -1,
                                                      "/tmp/asyncflow-openat-direct-bench", 0, 0,
                                                      -1, &file, op));
    }
}

BENCHMARK(BM_IoFileAdapterZeroByteRead);
BENCHMARK(BM_IoTimerAdapterNullExpiration);
BENCHMARK(BM_IoEventAdapterNullValue);
BENCHMARK(BM_IoTimeoutInvalidDelay);
BENCHMARK(BM_IoOpenAtNullPath);
BENCHMARK(BM_IoSocketNullOutput);
#if !defined(_WIN32)
BENCHMARK(BM_IoSocketNameNullOutput);
#endif
BENCHMARK(BM_IoOpenAtDirectInvalidIndex);

} // namespace
