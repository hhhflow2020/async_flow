#include "io_benchmark_support.hpp"

namespace {

#if !defined(_WIN32)
void BM_IoFileAdapterZeroIovReadvAt(benchmark::State& state) {
    FakeTask task;
    af::IoFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.readv_at(task, nullptr, 0, 0, op));
    }
}

void BM_IoFileAdapterZeroByteReadFixedAt(benchmark::State& state) {
    FakeTask task;
    af::IoFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.read_fixed_at(task, nullptr, 0, 0, 0, op));
    }
}

void BM_IoFileAdapterZeroByteWriteFixedAt(benchmark::State& state) {
    FakeTask task;
    af::IoFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.write_fixed_at(task, nullptr, 0, 0, 0, op));
    }
}

void BM_IoFixedFileAdapterZeroByteReadAt(benchmark::State& state) {
    FakeTask task;
    af::IoFixedFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.read_at(task, nullptr, 0, 0, op));
    }
}

void BM_IoFixedFileAdapterZeroByteWriteAt(benchmark::State& state) {
    FakeTask task;
    af::IoFixedFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.write_at(task, nullptr, 0, 0, op));
    }
}

void BM_IoFixedFileAdapterZeroIovReadvAt(benchmark::State& state) {
    FakeTask task;
    af::IoFixedFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.readv_at(task, nullptr, 0, 0, op));
    }
}

void BM_IoFixedFileAdapterZeroIovWritevAt(benchmark::State& state) {
    FakeTask task;
    af::IoFixedFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.writev_at(task, nullptr, 0, 0, op));
    }
}

void BM_IoFixedFileAdapterZeroByteRecv(benchmark::State& state) {
    FakeTask task;
    af::IoFixedFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.recv_some(task, nullptr, 0, op));
    }
}

void BM_IoFixedFileAdapterZeroByteSend(benchmark::State& state) {
    FakeTask task;
    af::IoFixedFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.send_some(task, nullptr, 0, op));
    }
}

void BM_IoFixedFileAdapterZeroIovRecvv(benchmark::State& state) {
    FakeTask task;
    af::IoFixedFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.recvv_some(task, nullptr, 0, op));
    }
}

void BM_IoFixedFileAdapterZeroIovSendv(benchmark::State& state) {
    FakeTask task;
    af::IoFixedFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.sendv_some(task, nullptr, 0, op));
    }
}

void BM_IoFixedFileAdapterZeroByteReadFixedAt(benchmark::State& state) {
    FakeTask task;
    af::IoFixedFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.read_fixed_at(
            task,
            af::IoFixedBuffer{nullptr, 0, 0},
            0,
            op));
    }
}

void BM_IoFixedFileAdapterZeroByteWriteFixedAt(benchmark::State& state) {
    FakeTask task;
    af::IoFixedFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.write_fixed_at(
            task,
            af::IoFixedBuffer{nullptr, 0, 0},
            0,
            op));
    }
}

BENCHMARK(BM_IoFileAdapterZeroIovReadvAt);
BENCHMARK(BM_IoFileAdapterZeroByteReadFixedAt);
BENCHMARK(BM_IoFileAdapterZeroByteWriteFixedAt);
BENCHMARK(BM_IoFixedFileAdapterZeroByteReadAt);
BENCHMARK(BM_IoFixedFileAdapterZeroByteWriteAt);
BENCHMARK(BM_IoFixedFileAdapterZeroIovReadvAt);
BENCHMARK(BM_IoFixedFileAdapterZeroIovWritevAt);
BENCHMARK(BM_IoFixedFileAdapterZeroByteRecv);
BENCHMARK(BM_IoFixedFileAdapterZeroByteSend);
BENCHMARK(BM_IoFixedFileAdapterZeroIovRecvv);
BENCHMARK(BM_IoFixedFileAdapterZeroIovSendv);
BENCHMARK(BM_IoFixedFileAdapterZeroByteReadFixedAt);
BENCHMARK(BM_IoFixedFileAdapterZeroByteWriteFixedAt);
#endif

} // namespace
