#include "io_benchmark_support.hpp"

namespace {

void BM_IoDatagramAdapterZeroByteRecv(benchmark::State &state) {
    FakeTask task;
    af::UdpSocket<BenchIoThread> socket(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(socket.recv_from_some(task, nullptr, 0, nullptr, nullptr, op));
    }
}

#if defined(__linux__)
void BM_IoDatagramAdapterInvalidRecvMultishot(benchmark::State &state) {
    FakeTask task;
    af::UdpSocket<BenchIoThread> socket(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    std::uint16_t buffer_id = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(socket.recv_multishot(task, 0, &buffer_id, op));
    }
}

void BM_IoDatagramAdapterInvalidRecvFromMultishot(benchmark::State &state) {
    FakeTask task;
    af::UdpSocket<BenchIoThread> socket(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    std::uint16_t buffer_id = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(
            socket.recv_from_multishot(task, 0, sizeof(sockaddr_storage), 0, &buffer_id, op));
    }
}
#endif

BENCHMARK(BM_IoDatagramAdapterZeroByteRecv);
#if defined(__linux__)
BENCHMARK(BM_IoDatagramAdapterInvalidRecvMultishot);
BENCHMARK(BM_IoDatagramAdapterInvalidRecvFromMultishot);
#endif

} // namespace
