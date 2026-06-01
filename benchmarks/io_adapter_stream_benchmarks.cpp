#include "io_benchmark_support.hpp"

namespace {

void BM_IoStreamAdapterZeroByteSend(benchmark::State &state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.send_some(task, nullptr, 0, op));
    }
}

#if defined(__linux__)
void BM_IoStreamAdapterZeroByteSendZc(benchmark::State &state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.send_zc_some(task, nullptr, 0, op));
    }
}

void BM_IoStreamAdapterInvalidRecvMultishot(benchmark::State &state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    std::uint16_t buffer_id = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.recv_multishot(task, 0, &buffer_id, op));
    }
}
#endif

void BM_IoListenerAdapterInvalidAccept(benchmark::State &state) {
    FakeTask task;
    af::TcpListener<BenchIoThread> listener(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(listener.accept_some(task, nullptr, nullptr, nullptr, op));
    }
}

void BM_IoListenerAdapterInvalidAcceptDirect(benchmark::State &state) {
    FakeTask task;
    af::TcpListener<BenchIoThread> listener(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    af::IoFixedFile<BenchIoThread> accepted;
    for (auto _ : state) {
        benchmark::DoNotOptimize(listener.accept_direct(task, nullptr, nullptr, 0, &accepted, op));
    }
}

void BM_IoListenerAdapterInvalidAcceptMultishot(benchmark::State &state) {
    FakeTask task;
    af::TcpListener<BenchIoThread> listener(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    int accepted = -1;
    for (auto _ : state) {
        benchmark::DoNotOptimize(listener.accept_multishot(task, nullptr, nullptr, &accepted, op));
    }
}

void BM_IoStreamAdapterInvalidConnect(benchmark::State &state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.connect(task, nullptr, 0, op));
    }
}

void BM_IoStreamAdapterInvalidShutdown(benchmark::State &state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.shutdown(task, 1, op));
    }
}

BENCHMARK(BM_IoStreamAdapterZeroByteSend);
#if defined(__linux__)
BENCHMARK(BM_IoStreamAdapterZeroByteSendZc);
BENCHMARK(BM_IoStreamAdapterInvalidRecvMultishot);
#endif
BENCHMARK(BM_IoListenerAdapterInvalidAccept);
BENCHMARK(BM_IoListenerAdapterInvalidAcceptDirect);
BENCHMARK(BM_IoListenerAdapterInvalidAcceptMultishot);
BENCHMARK(BM_IoStreamAdapterInvalidConnect);
BENCHMARK(BM_IoStreamAdapterInvalidShutdown);

} // namespace
