#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
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

inline constexpr std::uint32_t runtime_tcp_records_per_task = 32;

class RuntimeTcpLogTask final : public af::runtime_task {
public:
    RuntimeTcpLogTask(af::runtime_task::factory_token token, af::runtime &owner,
                      std::uint32_t task_id, std::atomic<int> &completed)
        : af::runtime_task(token, owner), task_id_(task_id), completed_(completed) {}

    [[nodiscard]] bool do_it(af::thread_group_ref logic_threads) noexcept {
        return schedule_to(logic_threads.shard(task_id_));
    }

private:
    af::task_result run_task() noexcept override {
        for (std::uint32_t i = 0; i < runtime_tcp_records_per_task; ++i) {
            LOG(INFO) << "runtime tcp log task=" << task_id_ << " seq=" << i
                      << " thread=" << af::runtime::current_thread_index();
        }
        completed_.fetch_add(1, std::memory_order_release);
        return done();
    }

    std::uint32_t task_id_{0};
    std::atomic<int> &completed_;
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

[[nodiscard]] bool wait_until_true(std::atomic<bool> &value) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!value.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

[[nodiscard]] bool wait_for_count(std::atomic<int> &value, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
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

[[nodiscard]] bool transient_socket_error() noexcept {
    return errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK;
}

[[nodiscard]] int make_loopback_tcp_listener(std::uint16_t &port) noexcept {
    int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) {
        return -1;
    }

    const int reuse = 1;
    static_cast<void>(::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 ||
        ::listen(listener, 16) != 0) {
        close_fd(listener);
        return -1;
    }

    socklen_t address_size = sizeof(address);
    if (::getsockname(listener, reinterpret_cast<sockaddr *>(&address), &address_size) != 0) {
        close_fd(listener);
        return -1;
    }
    port = ntohs(address.sin_port);
    set_fd_nonblocking(listener);
    return listener;
}

[[nodiscard]] int accept_until(int listener,
                               std::chrono::steady_clock::time_point deadline) noexcept {
    while (std::chrono::steady_clock::now() < deadline) {
        int accepted = ::accept(listener, nullptr, nullptr);
        if (accepted >= 0) {
            set_fd_nonblocking(accepted);
            return accepted;
        }
        if (!transient_socket_error()) {
            return -1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return -1;
}

void receive_stream_lines(int listener, std::atomic<int> &received, int expected,
                          std::atomic<bool> &accepted_connection) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    int accepted = accept_until(listener, deadline);
    if (accepted < 0) {
        return;
    }
    accepted_connection.store(true, std::memory_order_release);

    std::array<char, 4096> buffer{};
    while (received.load(std::memory_order_acquire) < expected &&
           std::chrono::steady_clock::now() < deadline) {
        const auto n = ::recv(accepted, buffer.data(), buffer.size(), 0);
        if (n > 0) {
            for (std::ptrdiff_t i = 0; i < n; ++i) {
                if (buffer[static_cast<std::size_t>(i)] == '\n') {
                    received.fetch_add(1, std::memory_order_release);
                }
            }
            continue;
        }
        if (n == 0) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    close_fd(accepted);
}

[[nodiscard]] af::runtime_config make_runtime_config(std::uint16_t tcp_port) {
    using namespace std::chrono_literals;

    af::runtime_config config;
    config.threads = {
        af::cpu_threads("tcp-log-cpu", 2),
        af::io_threads("tcp-log-io", 1),
    };
    config.logger = af::log_config::ordered();
    config.logger.consumer_thread = af::thread_selector::io(0);
    config.logger.queue_capacity = 1U << 15U;
    config.logger.max_batch_records = 512;
    config.logger.max_batch_delay = 1000us;
    config.logger.overflow = af::log_overflow_policy::drop_newest;
    config.logger.backends = {
        af::tcp_log_backend_config{"127.0.0.1", tcp_port, 1ms, af::thread_selector::io(0)},
    };
    config.shutdown.log_flush_timeout = 5s;
    return config;
}

} // namespace

int main() {
    using namespace std::chrono_literals;

    std::uint16_t tcp_port = 0;
    int listener = make_loopback_tcp_listener(tcp_port);
    if (listener < 0) {
        std::cerr << "failed to create tcp listener: " << std::strerror(errno) << '\n';
        return 1;
    }

    constexpr int task_count = 4;
    constexpr int expected_business_records =
        task_count * static_cast<int>(runtime_tcp_records_per_task) + 1;
    std::atomic<int> received{0};
    std::atomic<bool> accepted_connection{false};
    std::thread receiver_thread([&] {
        receive_stream_lines(listener, received, expected_business_records + 2,
                             accepted_connection);
    });

    af::runtime runtime(make_runtime_config(tcp_port));
    if (!runtime.start()) {
        std::cerr << "failed to start runtime\n";
        close_fd(listener);
        receiver_thread.join();
        return 1;
    }

    LOG(INFO) << "tcp log backend warmup";
    const bool warmup_flushed = runtime.flush_logger(5s);
    const bool connected = wait_until_true(accepted_connection);
    const int received_after_connect = received.load(std::memory_order_acquire);
    LOG(INFO) << "tcp log backend ready";
    const bool ready_flushed = runtime.flush_logger(5s);
    const bool ready_received = wait_for_count(received, received_after_connect + 1);
    const int received_before_tasks = received.load(std::memory_order_acquire);

    const af::thread_group_ref logic_threads = runtime.thread_group("tcp-log-cpu");
    if (logic_threads.empty()) {
        std::cerr << "runtime has no tcp-log-cpu threads\n";
        runtime.stop();
        close_fd(listener);
        receiver_thread.join();
        return 1;
    }

    std::atomic<int> completed{0};
    bool started = true;
    for (std::uint32_t i = 0; i < static_cast<std::uint32_t>(task_count); ++i) {
        auto task = af::make_task<RuntimeTcpLogTask>(runtime, i, completed);
        const bool task_started = task->do_it(logic_threads);
        started = task_started && started;
    }

    const bool completed_all = wait_for_completion(completed, task_count);
    LOG(INFO) << "external tcp log after runtime tasks";

    const bool flushed = runtime.flush_logger(5s);
    runtime.stop();

    receiver_thread.join();
    close_fd(listener);

    std::cout << "started=" << started << " completed=" << completed.load(std::memory_order_acquire)
              << " warmup_flushed=" << warmup_flushed << " connected=" << connected
              << " ready_flushed=" << ready_flushed << " ready_received=" << ready_received
              << " flushed=" << flushed
              << " tcp_received=" << received.load(std::memory_order_acquire) << '\n';
    return started && completed_all && warmup_flushed && connected && ready_flushed &&
                   ready_received && flushed &&
                   received.load(std::memory_order_acquire) >=
                       received_before_tasks + expected_business_records
               ? 0
               : 1;
}
