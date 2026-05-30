#include "io_benchmark_support.hpp"

namespace {

#if !defined(_WIN32)
void BM_IoStreamAdapterZeroIovSendv(benchmark::State& state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.sendv_some(task, nullptr, 0, op));
    }
}

void BM_IoStreamAdapterZeroIovSendvZc(benchmark::State& state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.sendv_zc_some(task, nullptr, 0, op));
    }
}

void BM_IoDatagramAdapterZeroIovRecvvFrom(benchmark::State& state) {
    FakeTask task;
    af::UdpSocket<BenchIoThread> socket(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(socket.recvv_from_some(task, nullptr, 0, nullptr, nullptr, op));
    }
}

void BM_IoDatagramAdapterZeroIovSendvTo(benchmark::State& state) {
    FakeTask task;
    af::UdpSocket<BenchIoThread> socket(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(socket.sendv_to_some(task, nullptr, 0, nullptr, 0, op));
    }
}

void BM_IoDatagramAdapterZeroIovSendvZcTo(benchmark::State& state) {
    FakeTask task;
    af::UdpSocket<BenchIoThread> socket(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(socket.sendv_zc_to_some(task, nullptr, 0, nullptr, 0, op));
    }
}
BENCHMARK(BM_IoStreamAdapterZeroIovSendv);
BENCHMARK(BM_IoStreamAdapterZeroIovSendvZc);
BENCHMARK(BM_IoDatagramAdapterZeroIovRecvvFrom);
BENCHMARK(BM_IoDatagramAdapterZeroIovSendvTo);
BENCHMARK(BM_IoDatagramAdapterZeroIovSendvZcTo);
#endif

} // namespace
