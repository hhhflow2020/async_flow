#include "io_benchmark_support.hpp"

namespace {

void BM_IoOpenAt2NullHow(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    int opened = -1;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_openat2(
            task, BenchIoThreads::IO_0, -1, "/tmp/asyncflow-openat2-bench", nullptr, &opened, op));
    }
}

void BM_IoStatxNullPath(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(
            af::io_statx(task, BenchIoThreads::IO_0, -1, nullptr, 0, 0, nullptr, op));
    }
}

void BM_IoMkdirAtNullPath(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(
            af::io_mkdirat(task, BenchIoThreads::IO_0, -1, nullptr, 0700U, op));
    }
}

void BM_IoSymlinkAtNullTarget(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_symlinkat(task, BenchIoThreads::IO_0, nullptr, -1,
                                                  "/tmp/asyncflow-symlinkat-bench", op));
    }
}

void BM_IoLinkAtNullOldPath(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_linkat(task, BenchIoThreads::IO_0, -1, nullptr, -1,
                                               "/tmp/asyncflow-linkat-bench", 0, op));
    }
}

void BM_IoFtruncateInvalidFd(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_ftruncate(task, BenchIoThreads::IO_0, -1, 0, op));
    }
}

void BM_IoCloseInvalidFd(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    for (auto _ : state) {
        af::UniqueFd fd;
        benchmark::DoNotOptimize(af::io_close(task, BenchIoThreads::IO_0, fd, op));
    }
}

BENCHMARK(BM_IoOpenAt2NullHow);
BENCHMARK(BM_IoStatxNullPath);
BENCHMARK(BM_IoMkdirAtNullPath);
BENCHMARK(BM_IoSymlinkAtNullTarget);
BENCHMARK(BM_IoLinkAtNullOldPath);
BENCHMARK(BM_IoFtruncateInvalidFd);
BENCHMARK(BM_IoCloseInvalidFd);

} // namespace
