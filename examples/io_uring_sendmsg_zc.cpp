#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <cerrno>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace {

enum class SendmsgZcThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct SendmsgZcRuntimeTraits {
    using Thread = SendmsgZcThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(SendmsgZcThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(SendmsgZcThread thread) noexcept {
        return thread == SendmsgZcThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using sendmsg_zc_async = af::AsyncRuntime<SendmsgZcRuntimeTraits>;
using SendmsgZcTaskBase = sendmsg_zc_async::Task;

#if defined(__linux__)
class SendmsgZcTask final : public SendmsgZcTaskBase {
public:
    explicit SendmsgZcTask(SendmsgZcTaskBase::FactoryToken token) : SendmsgZcTaskBase(token) {}

    bool do_it(
        int socket_fd,
        const char* first,
        std::size_t first_size,
        const char* second,
        std::size_t second_size,
        std::size_t* bytes_sent) {
        stream_.reset(SendmsgZcThread::IO_0, socket_fd);
        first_ = first;
        first_size_ = first_size;
        second_ = second;
        second_size_ = second_size;
        bytes_sent_ = bytes_sent;
        return schedule(SendmsgZcThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const std::size_t total_size = first_size_ + second_size_;
        if (sent_ >= total_size) {
            return done();
        }

        int iov_count = 0;
        if (sent_ < first_size_) {
            iov_[iov_count++] = iovec{
                const_cast<char*>(first_ + sent_),
                first_size_ - sent_};
            iov_[iov_count++] = iovec{const_cast<char*>(second_), second_size_};
        } else {
            const std::size_t second_offset = sent_ - first_size_;
            iov_[iov_count++] = iovec{
                const_cast<char*>(second_ + second_offset),
                second_size_ - second_offset};
        }

        const af::IoStatus status = stream_.sendv_zc_some(*this, iov_, iov_count, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > total_size - sent_) {
            return failed();
        }

        sent_ += status.bytes;
        *bytes_sent_ = sent_;
        return again();
    }

    af::TcpStream<SendmsgZcThread> stream_{};
    const char* first_{nullptr};
    const char* second_{nullptr};
    std::size_t first_size_{0};
    std::size_t second_size_{0};
    std::size_t sent_{0};
    iovec iov_[2]{};
    af::IoOpState send_{};
    std::size_t* bytes_sent_{nullptr};
};

bool read_exact_until(int fd, char* output, std::size_t size) {
    std::size_t offset = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (offset < size) {
        const ssize_t n = ::read(fd, output + offset, size - offset);
        if (n > 0) {
            offset += static_cast<std::size_t>(n);
            continue;
        }
        if (n == 0) {
            return false;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return false;
        }
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}
#endif

} // namespace

int main() {
#if defined(__linux__)
    int fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        std::cerr << "socketpair failed\n";
        return 1;
    }
    af::UniqueFd sender(fds[0]);
    af::UniqueFd receiver(fds[1]);

    const char first[] = "asyncflow sendmsg_zc ";
    const char second[] = "uses vectored zero-copy when the kernel supports it\n";
    constexpr std::size_t first_size = sizeof(first) - 1U;
    constexpr std::size_t second_size = sizeof(second) - 1U;
    constexpr std::size_t payload_size = first_size + second_size;

    sendmsg_zc_async::init();
    const bool has_uring = sendmsg_zc_async::io_uring_backend_available(SendmsgZcThread::IO_0);
    std::size_t bytes_sent{0};
    const bool started = sendmsg_zc_async::start_task<SendmsgZcTask>(
        sender.get(),
        first,
        first_size,
        second,
        second_size,
        &bytes_sent);
    if (!started) {
        std::cerr << "sendmsg_zc task did not start\n";
        sendmsg_zc_async::shutdown();
        return 1;
    }
    sendmsg_zc_async::shutdown();

    char received[payload_size]{};
    if (!read_exact_until(receiver.get(), received, payload_size) ||
        std::memcmp(received, first, first_size) != 0 ||
        std::memcmp(received + first_size, second, second_size) != 0) {
        std::cerr << "payload mismatch\n";
        return 1;
    }

    std::cout << "sendmsg_zc bytes=" << bytes_sent
              << " io_uring=" << (has_uring ? "available" : "fallback") << '\n';
    return 0;
#else
    std::cout << "sendmsg_zc example is Linux-only\n";
    return 0;
#endif
}
