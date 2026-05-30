#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

enum class AcceptThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct AcceptRuntimeTraits {
    using Thread = AcceptThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(AcceptThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(AcceptThread thread) noexcept {
        return thread == AcceptThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using accept_async = af::AsyncRuntime<AcceptRuntimeTraits>;
using AcceptTask = accept_async::Task;

bool wait_until_at_least(std::atomic<int>& value, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

#if defined(__linux__)
void close_fd(int& fd) {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

class MultishotAcceptTask final : public AcceptTask {
public:
    explicit MultishotAcceptTask(AcceptTask::FactoryToken token) : AcceptTask(token) {}

    bool do_it(
        int fd,
        int target_accepts,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* accepted_count,
        std::atomic<int>* error) {
        listener_.reset(AcceptThread::IO_0, fd);
        target_accepts_ = target_accepts;
        armed_ = armed;
        completed_ = completed;
        accepted_count_ = accepted_count;
        error_ = error;
        return schedule(AcceptThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Accept,
        Cancel,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Accept:
            return accept_one();
        case State::Cancel:
            return finish_cancel();
        }
        return complete(EIO);
    }

    af::TaskResult accept_one() {
        const af::IoStatus status = listener_.accept_multishot(
            *this,
            nullptr,
            nullptr,
            &accepted_fd_,
            accept_);
        if (status.pending()) {
            if (!armed_once_) {
                armed_once_ = true;
                armed_->fetch_add(1, std::memory_order_release);
            }
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }

        close_fd(accepted_fd_);
        const int accepted = accepted_count_->fetch_add(1, std::memory_order_acq_rel) + 1;
        if (accepted < target_accepts_) {
            return pending();
        }
        if (!accept_.waiting) {
            return complete(0);
        }
        if (!accept_async::cancel_io(AcceptThread::IO_0, accept_)) {
            return complete(accept_.wait.error == 0 ? EIO : accept_.wait.error);
        }
        state_ = State::Cancel;
        return pending();
    }

    af::TaskResult finish_cancel() {
        int ignored = -1;
        const af::IoStatus status =
            listener_.accept_multishot(*this, nullptr, nullptr, &ignored, accept_);
        if (status.pending()) {
            return pending();
        }
        return complete(status.failed() && status.error == ECANCELED ? 0 : EIO);
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Accept};
    af::TcpListener<AcceptThread> listener_{};
    int accepted_fd_{-1};
    int target_accepts_{0};
    bool armed_once_{false};
    af::IoOpState accept_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* accepted_count_{nullptr};
    std::atomic<int>* error_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    accept_async::init();
    if (!accept_async::io_uring_backend_available(AcceptThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        accept_async::shutdown();
        return 0;
    }

    int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listener < 0) {
        std::cout << "socket failed\n";
        accept_async::shutdown();
        return 1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    socklen_t address_size = sizeof(address);
    if (::bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
        ::listen(listener, 16) != 0 ||
        ::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_size) != 0) {
        std::cout << "listen failed\n";
        close_fd(listener);
        accept_async::shutdown();
        return 1;
    }

    constexpr int target_accepts = 2;
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> accepted_count{0};
    std::atomic<int> error{0};
    const bool started = accept_async::start_task<MultishotAcceptTask>(
        listener,
        target_accepts,
        &armed,
        &completed,
        &accepted_count,
        &error);
    AF_ASSERT(started);
    if (!started || !wait_until_at_least(armed, 1)) {
        if (completed.load(std::memory_order_acquire) != 0) {
            std::cout << "io_uring multishot accept unsupported error="
                      << error.load(std::memory_order_acquire) << '\n';
            close_fd(listener);
            accept_async::shutdown();
            return 0;
        }
        std::cout << "io_uring multishot accept arm timed out\n";
        close_fd(listener);
        accept_async::shutdown();
        return 1;
    }

    int clients[target_accepts]{-1, -1};
    for (int& client : clients) {
        client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (client < 0) {
            std::cout << "client socket failed\n";
            close_fd(listener);
            accept_async::shutdown();
            return 1;
        }
        const int rc = ::connect(client, reinterpret_cast<sockaddr*>(&address), address_size);
        if (rc != 0 && errno != EINPROGRESS) {
            std::cout << "connect failed\n";
            close_fd(listener);
            accept_async::shutdown();
            return 1;
        }
    }

    if (!wait_until_at_least(completed, 1) || error.load(std::memory_order_acquire) != 0) {
        std::cout << "io_uring multishot accept failed error="
                  << error.load(std::memory_order_acquire) << '\n';
        for (int& client : clients) {
            close_fd(client);
        }
        close_fd(listener);
        accept_async::shutdown();
        return 1;
    }

    std::cout << "io_uring multishot accepted="
              << accepted_count.load(std::memory_order_acquire) << '\n';
    for (int& client : clients) {
        close_fd(client);
    }
    close_fd(listener);
    accept_async::shutdown();
    return 0;
#else
    std::cout << "io_uring multishot accept example is Linux-only\n";
    return 0;
#endif
}
