#include "io_benchmark_support.hpp"

namespace {

void BM_IoSendfileZeroCount(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    af::IoOffset offset = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(
            af::io_sendfile_some(task, BenchIoThreads::IO_0, -1, -1, &offset, 0, op));
    }
}

void BM_IoSpliceZeroCount(benchmark::State &state) {
    FakeTask task;
    af::IoOpState op;
    af::IoOffset input_offset = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_splice_some(task, BenchIoThreads::IO_0, -1, &input_offset,
                                                    -1, nullptr, 0, 0, op));
    }
}
BENCHMARK(BM_IoSendfileZeroCount);
BENCHMARK(BM_IoSpliceZeroCount);

} // namespace
