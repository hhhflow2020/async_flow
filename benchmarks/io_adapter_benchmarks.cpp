#include "io_benchmark_support.hpp"

namespace {

void BM_IoFileAdapterZeroByteRead(benchmark::State& state) {
    FakeTask task;
    af::IoFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.read_some(task, nullptr, 0, op));
    }
}

void BM_IoStreamAdapterZeroByteSend(benchmark::State& state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.send_some(task, nullptr, 0, op));
    }
}

#if defined(__linux__)
void BM_IoStreamAdapterZeroByteSendZc(benchmark::State& state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.send_zc_some(task, nullptr, 0, op));
    }
}

void BM_IoStreamAdapterInvalidRecvMultishot(benchmark::State& state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    std::uint16_t buffer_id = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.recv_multishot(task, 0, &buffer_id, op));
    }
}
#endif

void BM_IoDatagramAdapterZeroByteRecv(benchmark::State& state) {
    FakeTask task;
    af::UdpSocket<BenchIoThread> socket(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(socket.recv_from_some(task, nullptr, 0, nullptr, nullptr, op));
    }
}

#if defined(__linux__)
void BM_IoDatagramAdapterInvalidRecvMultishot(benchmark::State& state) {
    FakeTask task;
    af::UdpSocket<BenchIoThread> socket(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    std::uint16_t buffer_id = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(socket.recv_multishot(task, 0, &buffer_id, op));
    }
}

void BM_IoDatagramAdapterInvalidRecvFromMultishot(benchmark::State& state) {
    FakeTask task;
    af::UdpSocket<BenchIoThread> socket(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    std::uint16_t buffer_id = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(socket.recv_from_multishot(
            task,
            0,
            sizeof(sockaddr_storage),
            0,
            &buffer_id,
            op));
    }
}
#endif

void BM_IoListenerAdapterInvalidAccept(benchmark::State& state) {
    FakeTask task;
    af::TcpListener<BenchIoThread> listener(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(listener.accept_some(task, nullptr, nullptr, nullptr, op));
    }
}

void BM_IoListenerAdapterInvalidAcceptDirect(benchmark::State& state) {
    FakeTask task;
    af::TcpListener<BenchIoThread> listener(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    af::IoFixedFile<BenchIoThread> accepted;
    for (auto _ : state) {
        benchmark::DoNotOptimize(listener.accept_direct(
            task,
            nullptr,
            nullptr,
            0,
            &accepted,
            op));
    }
}

void BM_IoListenerAdapterInvalidAcceptMultishot(benchmark::State& state) {
    FakeTask task;
    af::TcpListener<BenchIoThread> listener(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    int accepted = -1;
    for (auto _ : state) {
        benchmark::DoNotOptimize(listener.accept_multishot(
            task,
            nullptr,
            nullptr,
            &accepted,
            op));
    }
}

void BM_IoStreamAdapterInvalidConnect(benchmark::State& state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.connect(task, nullptr, 0, op));
    }
}

void BM_IoStreamAdapterInvalidShutdown(benchmark::State& state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.shutdown(task, 1, op));
    }
}

void BM_IoTimerAdapterNullExpiration(benchmark::State& state) {
    FakeTask task;
    af::IoTimer<BenchIoThread> timer(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(timer.wait(task, nullptr, op));
    }
}

void BM_IoEventAdapterNullValue(benchmark::State& state) {
    FakeTask task;
    af::IoEvent<BenchIoThread> event(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(event.wait(task, nullptr, op));
    }
}

void BM_IoTimeoutInvalidDelay(benchmark::State& state) {
    FakeTask task;
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_wait_timeout(
            task,
            BenchIoThread::IO_0,
            std::chrono::nanoseconds{0},
            op));
    }
}

void BM_IoOpenAtNullPath(benchmark::State& state) {
    FakeTask task;
    af::IoOpState op;
    int opened = -1;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_openat(
            task,
            BenchIoThread::IO_0,
            -1,
            nullptr,
            0,
            0,
            &opened,
            op));
    }
}

void BM_IoSocketNullOutput(benchmark::State& state) {
    FakeTask task;
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_socket(
            task,
            BenchIoThread::IO_0,
            0,
            0,
            0,
            nullptr,
            op));
    }
}

#if !defined(_WIN32)
void BM_IoSocketNameNullOutput(benchmark::State& state) {
    FakeTask task;
    socklen_t size = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_getsockname(
            task,
            BenchIoThread::IO_0,
            0,
            nullptr,
            &size));
    }
}
#endif

void BM_IoOpenAtDirectInvalidIndex(benchmark::State& state) {
    FakeTask task;
    af::IoOpState op;
    af::IoFixedFile<BenchIoThread> file;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_openat_direct(
            task,
            BenchIoThread::IO_0,
            -1,
            "/tmp/asyncflow-openat-direct-bench",
            0,
            0,
            -1,
            &file,
            op));
    }
}

BENCHMARK(BM_IoFileAdapterZeroByteRead);
BENCHMARK(BM_IoStreamAdapterZeroByteSend);
#if defined(__linux__)
BENCHMARK(BM_IoStreamAdapterZeroByteSendZc);
BENCHMARK(BM_IoStreamAdapterInvalidRecvMultishot);
BENCHMARK(BM_IoDatagramAdapterInvalidRecvMultishot);
BENCHMARK(BM_IoDatagramAdapterInvalidRecvFromMultishot);
#endif
BENCHMARK(BM_IoDatagramAdapterZeroByteRecv);
BENCHMARK(BM_IoListenerAdapterInvalidAccept);
BENCHMARK(BM_IoListenerAdapterInvalidAcceptDirect);
BENCHMARK(BM_IoListenerAdapterInvalidAcceptMultishot);
BENCHMARK(BM_IoStreamAdapterInvalidConnect);
BENCHMARK(BM_IoStreamAdapterInvalidShutdown);
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
