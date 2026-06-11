#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

#include "af/log.hpp"
#include "af/runtime.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

inline constexpr std::uint32_t runtime_udp_records_per_task = 32;

class runtime_udp_log_task final : public af::runtime_task {
public:
    runtime_udp_log_task(af::runtime_task::factory_token token, af::runtime &owner) noexcept
        : af::runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_group_ref logic_threads, std::uint32_t task_id,
                             std::atomic<int> &completed) noexcept {
        task_id_ = task_id;
        completed_ = &completed;
        return schedule_to(logic_threads.shard(task_id_));
    }

private:
    af::task_result run_task() noexcept override {
        for (std::uint32_t i = 0; i < runtime_udp_records_per_task; ++i) {
            AF_LOG(INFO) << "runtime udp log task=" << task_id_ << " seq=" << i
                         << " thread=" << af::runtime::current_thread_index();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::uint32_t task_id_{0};
    std::atomic<int> *completed_{nullptr};
};

[[nodiscard]] bool wait_for_completion(std::atomic<int> &completed, int expected) {
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

[[nodiscard]] int make_loopback_udp_receiver(std::uint16_t &port) noexcept {
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
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

[[nodiscard]] af::runtime_config make_runtime_config(std::uint16_t udp_port) {
    using namespace std::chrono_literals;

    af::runtime_config config;
    config.threads = {
        af::cpu_threads("udp-log-cpu", 2),
        af::io_threads("udp-log-io", 1),
    };
    config.logger = af::log_config::ordered();
    config.logger.consumer_thread = af::thread_selector::io(0);
    config.logger.queue_capacity = 1U << 15U;
    config.logger.max_batch_records = 512;
    config.logger.max_batch_delay = 1000us;
    config.logger.overflow = af::log_overflow_policy::drop_newest;
    config.logger.backends = {
        af::udp_log_backend_config{"127.0.0.1", udp_port, 1400, af::thread_selector::io(0)},
    };
    config.shutdown.log_flush_timeout = 5s;
    return config;
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

    af::runtime runtime(make_runtime_config(udp_port));
    if (!runtime.start()) {
        std::cerr << "failed to start runtime\n";
        close_fd(receiver);
        receiver_thread.join();
        return 1;
    }

    const af::thread_group_ref logic_threads = runtime.thread_group("udp-log-cpu");
    if (logic_threads.empty()) {
        std::cerr << "runtime has no udp-log-cpu threads\n";
        runtime.stop();
        close_fd(receiver);
        receiver_thread.join();
        return 1;
    }

    std::atomic<int> completed{0};
    bool started = true;
    for (std::uint32_t i = 0; i < static_cast<std::uint32_t>(task_count); ++i) {
        auto task = af::make_task<runtime_udp_log_task>(runtime);
        const bool task_started = task->do_it(logic_threads, i, completed);
        started = task_started && started;
    }

    const bool completed_all = wait_for_completion(completed, task_count);
    AF_LOG(INFO) << "external udp log after runtime tasks";

    const bool flushed = runtime.flush_logger(5s);
    runtime.stop();

    receiver_thread.join();
    close_fd(receiver);

    std::cout << "started=" << started << " completed=" << completed.load(std::memory_order_acquire)
              << " flushed=" << flushed
              << " udp_received=" << received.load(std::memory_order_acquire) << '\n';
    return started && completed_all && flushed &&
                   received.load(std::memory_order_acquire) == expected_datagrams
               ? 0
               : 1;
}
