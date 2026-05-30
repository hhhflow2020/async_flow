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
        af::IoResult*,
        bool = false) noexcept {
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

#if !defined(_WIN32)
    static bool io_submit_recvmsg(
        BenchIoThread,
        int,
        void*,
        std::size_t,
        sockaddr*,
        socklen_t*,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_sendmsg(
        BenchIoThread,
        int,
        const void*,
        std::size_t,
        const sockaddr*,
        socklen_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_recvmsg_iov(
        BenchIoThread,
        int,
        const iovec*,
        int,
        sockaddr*,
        socklen_t*,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_sendmsg_iov(
        BenchIoThread,
        int,
        const iovec*,
        int,
        const sockaddr*,
        socklen_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_readv_at(
        BenchIoThread,
        int,
        const iovec*,
        int,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_writev_at(
        BenchIoThread,
        int,
        const iovec*,
        int,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_accept(
        BenchIoThread,
        int,
        sockaddr*,
        socklen_t*,
        int,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_connect(
        BenchIoThread,
        int,
        const sockaddr*,
        socklen_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }
#endif

    static bool io_submit_openat(
        BenchIoThread,
        int,
        const char*,
        int,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_close(
        BenchIoThread,
        int,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_statx(
        BenchIoThread,
        int,
        const char*,
        int,
        std::uint32_t,
        struct statx*,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_fallocate(
        BenchIoThread,
        int,
        int,
        std::uint64_t,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_renameat(
        BenchIoThread,
        int,
        const char*,
        int,
        const char*,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_unlinkat(
        BenchIoThread,
        int,
        const char*,
        int,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_splice(
        BenchIoThread,
        int,
        std::int64_t,
        int,
        std::int64_t,
        std::size_t,
        unsigned int,
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

void BM_IoListenerAdapterInvalidAccept(benchmark::State& state) {
    FakeTask task;
    af::TcpListener<BenchIoThread> listener(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(listener.accept_some(task, nullptr, nullptr, nullptr, op));
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

void BM_IoStatxNullPath(benchmark::State& state) {
    FakeTask task;
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_statx(
            task,
            BenchIoThread::IO_0,
            -1,
            nullptr,
            0,
            0,
            nullptr,
            op));
    }
}

void BM_IoCloseInvalidFd(benchmark::State& state) {
    FakeTask task;
    af::IoOpState op;
    for (auto _ : state) {
        af::UniqueFd fd;
        benchmark::DoNotOptimize(af::io_close(task, BenchIoThread::IO_0, fd, op));
    }
}

void BM_IoSendfileZeroCount(benchmark::State& state) {
    FakeTask task;
    af::IoOpState op;
    af::IoOffset offset = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_sendfile_some(
            task,
            BenchIoThread::IO_0,
            -1,
            -1,
            &offset,
            0,
            op));
    }
}

void BM_IoSpliceZeroCount(benchmark::State& state) {
    FakeTask task;
    af::IoOpState op;
    af::IoOffset input_offset = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(af::io_splice_some(
            task,
            BenchIoThread::IO_0,
            -1,
            &input_offset,
            -1,
            nullptr,
            0,
            0,
            op));
    }
}

#if !defined(_WIN32)
void BM_IoFileAdapterZeroIovReadvAt(benchmark::State& state) {
    FakeTask task;
    af::IoFile<BenchIoThread> file(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(file.readv_at(task, nullptr, 0, 0, op));
    }
}

void BM_IoStreamAdapterZeroIovSendv(benchmark::State& state) {
    FakeTask task;
    af::TcpStream<BenchIoThread> stream(BenchIoThread::IO_0, -1);
    af::IoOpState op;
    for (auto _ : state) {
        benchmark::DoNotOptimize(stream.sendv_some(task, nullptr, 0, op));
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
#endif

BENCHMARK(BM_IoFileAdapterZeroByteRead);
BENCHMARK(BM_IoStreamAdapterZeroByteSend);
BENCHMARK(BM_IoDatagramAdapterZeroByteRecv);
BENCHMARK(BM_IoListenerAdapterInvalidAccept);
BENCHMARK(BM_IoStreamAdapterInvalidConnect);
BENCHMARK(BM_IoTimerAdapterNullExpiration);
BENCHMARK(BM_IoEventAdapterNullValue);
BENCHMARK(BM_IoOpenAtNullPath);
BENCHMARK(BM_IoStatxNullPath);
BENCHMARK(BM_IoCloseInvalidFd);
BENCHMARK(BM_IoSendfileZeroCount);
BENCHMARK(BM_IoSpliceZeroCount);
#if !defined(_WIN32)
BENCHMARK(BM_IoFileAdapterZeroIovReadvAt);
BENCHMARK(BM_IoStreamAdapterZeroIovSendv);
BENCHMARK(BM_IoDatagramAdapterZeroIovRecvvFrom);
BENCHMARK(BM_IoDatagramAdapterZeroIovSendvTo);
#endif

} // namespace
