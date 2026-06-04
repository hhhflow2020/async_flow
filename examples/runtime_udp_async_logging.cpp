#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

struct RuntimeUdpLogicThreadTag;
struct RuntimeUdpIoThreadTag;

inline constexpr std::uint32_t runtime_udp_records_per_task = 32;

struct RuntimeUdpTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<RuntimeUdpLogicThreadTag, 2, af::thread_kind::cpu>("udp-log-cpu"),
        af::thread_group<RuntimeUdpIoThreadTag, 1, af::thread_kind::io>("udp-log-io"));
    static constexpr af::shutdown_policy shutdown_policy = af::shutdown_policy::wait_for_tasks;
};

using runtime_udp_async = af::AsyncRuntime<RuntimeUdpTraits>;
using RuntimeUdpTaskBase = runtime_udp_async::Task;

struct RuntimeUdpThreads {
    static constexpr auto logic = runtime_udp_async::thread_group<RuntimeUdpLogicThreadTag>();
    static constexpr auto IO_0 =
        runtime_udp_async::thread_group<RuntimeUdpIoThreadTag>().template at<0>();
};

class RuntimeUdpLogTask final : public RuntimeUdpTaskBase {
public:
    explicit RuntimeUdpLogTask(RuntimeUdpTaskBase::FactoryToken token)
        : RuntimeUdpTaskBase(token) {}

    bool do_it(std::uint32_t task_id, std::atomic<int> *completed) {
        task_id_ = task_id;
        completed_ = completed;
        return schedule_to(RuntimeUdpThreads::logic.shard(task_id));
    }

private:
    af::task_result run() override {
        for (std::uint32_t i = 0; i < runtime_udp_records_per_task; ++i) {
            LOG(INFO) << "runtime udp log task=" << task_id_ << " seq=" << i
                      << " thread=" << runtime_udp_async::current_thread_index();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::uint32_t task_id_{0};
    std::atomic<int> *completed_{nullptr};
};

bool wait_for_completion(std::atomic<int> &completed, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (completed.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

void close_fd(int &fd) noexcept {
    if (fd >= 0) {
        static_cast<void>(::close(fd));
        fd = -1;
    }
}

void set_fd_nonblocking(int fd) noexcept {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        static_cast<void>(::fcntl(fd, F_SETFL, flags | O_NONBLOCK));
    }
}

int make_loopback_udp_receiver(std::uint16_t &port) noexcept {
    int socket_fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd < 0) {
        return -1;
    }

    const int receive_buffer_size = 1 << 20;
    static_cast<void>(::setsockopt(socket_fd, SOL_SOCKET, SO_RCVBUF, &receive_buffer_size,
                                   sizeof(receive_buffer_size)));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(socket_fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        close_fd(socket_fd);
        return -1;
    }

    socklen_t address_size = sizeof(address);
    if (::getsockname(socket_fd, reinterpret_cast<sockaddr *>(&address), &address_size) != 0) {
        close_fd(socket_fd);
        return -1;
    }
    port = ntohs(address.sin_port);
    set_fd_nonblocking(socket_fd);
    return socket_fd;
}

void receive_datagrams(int fd, std::atomic<int> &received, int expected) {
    std::array<char, 2048> buffer{};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (received.load(std::memory_order_acquire) < expected &&
           std::chrono::steady_clock::now() < deadline) {
        const auto n = ::recv(fd, buffer.data(), buffer.size(), 0);
        if (n > 0) {
            received.fetch_add(1, std::memory_order_release);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

} // namespace

int main() {
    using namespace std::chrono_literals;

    std::uint16_t udp_port = 0;
    int receiver = make_loopback_udp_receiver(udp_port);
    if (receiver < 0) {
        std::cerr << "failed to create udp receiver: " << std::strerror(errno) << '\n';
        return 1;
    }

    constexpr int task_count = 4;
    constexpr int expected_datagrams =
        task_count * static_cast<int>(runtime_udp_records_per_task) + 1;
    std::atomic<int> received{0};
    std::thread receiver_thread([&] { receive_datagrams(receiver, received, expected_datagrams); });

    runtime_udp_async::init();

    af::AsyncLogConfig config = af::AsyncLogConfig::ordered(runtime_udp_async::thread_count);
    config.queue_capacity = 1U << 15U;
    config.max_batch_size = 512;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.flush_poll_interval = 1ms;
    config.backends.push_back(af::make_runtime_udp_log_backend<runtime_udp_async>({
        .thread = RuntimeUdpThreads::IO_0,
        .host = "127.0.0.1",
        .port = udp_port,
        .batch_queue_capacity = 128,
        .max_batch_records = 64,
        .max_datagram_size = 1400,
        .max_batches_per_run = 64,
    }));

    auto logging = af::start_async_logging_for_runtime<runtime_udp_async>(std::move(config),
                                                                          RuntimeUdpThreads::IO_0);

    std::atomic<int> completed{0};
    bool started = true;
    for (std::uint32_t i = 0; i < static_cast<std::uint32_t>(task_count); ++i) {
        started = runtime_udp_async::start_task<RuntimeUdpLogTask>(i, &completed) && started;
    }

    const bool completed_all = wait_for_completion(completed, task_count);
    LOG(INFO) << "external udp log after runtime tasks";

    const bool flushed = logging->flush(5s);
    const af::AsyncLogStats stats = logging->stats();
    logging->stop();
    runtime_udp_async::shutdown();

    receiver_thread.join();
    close_fd(receiver);

    std::cout << "started=" << started << " completed=" << completed.load(std::memory_order_acquire)
              << " accepted=" << stats.accepted << " dropped=" << stats.dropped
              << " flushed=" << flushed
              << " udp_received=" << received.load(std::memory_order_acquire) << '\n';
    return started && completed_all && flushed && stats.dropped == 0U &&
                   received.load(std::memory_order_acquire) == expected_datagrams
               ? 0
               : 1;
}
