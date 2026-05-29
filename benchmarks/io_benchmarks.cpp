#include <cstddef>
#include <cstdint>

#include <benchmark/benchmark.h>

#include "af/io.hpp"

namespace {

enum class BenchIoThread : std::int16_t {
    IO_0,
};

struct FakeRuntime {
    static bool io_uring_backend_available(BenchIoThread) noexcept {
        return false;
    }

    static bool io_wait(
        BenchIoThread,
        int,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_recv(
        BenchIoThread,
        int,
        void*,
        std::size_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_send(
        BenchIoThread,
        int,
        const void*,
        std::size_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }
};

struct FakeTask {
    using Runtime = FakeRuntime;
    using Thread = BenchIoThread;
};

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

void BM_IoDatagramAdapterZeroByteRecv(benchmark::State& state) {
    FakeTask task;
    af::UdpSocket<BenchIoThread> socket(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(socket.recv_from_some(task, nullptr, 0, nullptr, nullptr, op));
    }
}

BENCHMARK(BM_IoFileAdapterZeroByteRead);
BENCHMARK(BM_IoStreamAdapterZeroByteSend);
BENCHMARK(BM_IoDatagramAdapterZeroByteRecv);

} // namespace
