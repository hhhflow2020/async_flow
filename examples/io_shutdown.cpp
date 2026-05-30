#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <sys/socket.h>
#include <unistd.h>
#endif

#if defined(__linux__)
enum class ShutdownThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct ShutdownRuntimeTraits {
    using Thread = ShutdownThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(ShutdownThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(ShutdownThread thread) noexcept {
        return thread == ShutdownThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using shutdown_async = af::AsyncRuntime<ShutdownRuntimeTraits>;
using ShutdownTaskBase = shutdown_async::Task;

class ShutdownWriteTask final : public ShutdownTaskBase {
public:
    explicit ShutdownWriteTask(ShutdownTaskBase::FactoryToken token) : ShutdownTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<int>* error) {
        stream_.reset(ShutdownThread::IO_0, fd);
        completed_ = completed;
        error_ = error;
        return schedule(ShutdownThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return shutdown_write();
    }

    af::TaskResult shutdown_write() {
        const af::IoStatus status = stream_.shutdown(*this, SHUT_WR, shutdown_);
        if (status.pending()) {
            return pending();
        }
        error_->store(status.ready() ? 0 : status.error, std::memory_order_release);
        completed_->store(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<ShutdownThread> stream_{};
    af::IoOpState shutdown_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

void close_pair(int (&fds)[2]) {
    if (fds[0] >= 0) {
        ::close(fds[0]);
        fds[0] = -1;
    }
    if (fds[1] >= 0) {
        ::close(fds[1]);
        fds[1] = -1;
    }
}
#endif

int main() {
#if defined(__linux__)
    shutdown_async::init();
    if (!shutdown_async::io_backend_available(ShutdownThread::IO_0)) {
        std::cout << "io shutdown backend unavailable\n";
        shutdown_async::shutdown();
        return 0;
    }

    int fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        std::cerr << "socketpair failed\n";
        shutdown_async::shutdown();
        return 1;
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    if (!shutdown_async::start_task<ShutdownWriteTask>(fds[0], &completed, &error)) {
        std::cerr << "shutdown task start failed\n";
        close_pair(fds);
        shutdown_async::shutdown();
        return 1;
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (completed.load(std::memory_order_acquire) == 0) {
        if (std::chrono::steady_clock::now() > deadline) {
            std::cerr << "shutdown task timed out\n";
            close_pair(fds);
            shutdown_async::shutdown();
            return 1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (error.load(std::memory_order_acquire) != 0) {
        std::cerr << "shutdown failed error=" << error.load(std::memory_order_acquire) << '\n';
        close_pair(fds);
        shutdown_async::shutdown();
        return 1;
    }

    char ignored = 0;
    const ssize_t read_result = ::read(fds[1], &ignored, sizeof(ignored));
    if (read_result != 0) {
        std::cerr << "peer did not observe EOF\n";
        close_pair(fds);
        shutdown_async::shutdown();
        return 1;
    }

    std::cout << "io shutdown backend="
              << (shutdown_async::io_uring_backend_available(ShutdownThread::IO_0) ? "io_uring" : "fallback")
              << " eof=1\n";
    close_pair(fds);
    shutdown_async::shutdown();
    return 0;
#else
    std::cout << "io shutdown example is Linux-only\n";
    return 0;
#endif
}
