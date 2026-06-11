#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <thread>
#include <utility>
#include <vector>

#include <benchmark/benchmark.h>

#include "af/net.hpp"
#include "af/runtime.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

namespace {

class unique_fd {
public:
    explicit unique_fd(int fd = -1) noexcept : fd_(fd) {}

    unique_fd(const unique_fd &) = delete;
    unique_fd &operator=(const unique_fd &) = delete;

    unique_fd(unique_fd &&other) noexcept : fd_(other.release()) {}

    unique_fd &operator=(unique_fd &&other) noexcept {
        if (this != &other) {
            reset(other.release());
        }
        return *this;
    }

    ~unique_fd() {
        reset();
    }

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

    [[nodiscard]] int release() noexcept {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }

    void reset(int fd = -1) noexcept {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = fd;
    }

private:
    int fd_{-1};
};

template <typename Predicate>
[[nodiscard]] bool wait_until(Predicate predicate,
                              std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

[[nodiscard]] unique_fd connect_loopback(std::uint16_t port) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);

    for (int attempt = 0; attempt < 50; ++attempt) {
        unique_fd fd(::socket(AF_INET, SOCK_STREAM, 0));
        if (fd.get() < 0) {
            return unique_fd{};
        }
        if (::connect(fd.get(), reinterpret_cast<const sockaddr *>(&address), sizeof(address)) ==
            0) {
            return fd;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return unique_fd{};
}

[[nodiscard]] bool set_io_timeout(int fd) noexcept {
    timeval timeout{};
    timeout.tv_sec = 2;
    return ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0 &&
           ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0;
}

[[nodiscard]] bool send_all(int fd, const std::byte *data, std::size_t size) noexcept {
    std::size_t sent = 0;
    while (sent < size) {
        const ssize_t n = ::send(fd, data + sent, size - sent, 0);
        if (n > 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool recv_exact(int fd, std::byte *data, std::size_t size) noexcept {
    std::size_t received = 0;
    while (received < size) {
        const ssize_t n = ::recv(fd, data + received, size - received, 0);
        if (n > 0) {
            received += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool echo_roundtrip(int fd, const std::vector<std::byte> &payload,
                                  std::vector<std::byte> &received, bool verify_payload) noexcept {
    if (!send_all(fd, payload.data(), payload.size()) ||
        !recv_exact(fd, received.data(), received.size())) {
        return false;
    }
    if (!verify_payload) {
        benchmark::DoNotOptimize(received.data());
        return true;
    }
    return std::memcmp(payload.data(), received.data(), payload.size()) == 0;
}

void on_tcp_echo_read(void *owner, af::net::tcp_connection_ref conn,
                      af::buffer_view bytes) noexcept {
    static_cast<void>(owner);
    static_cast<void>(conn.send(bytes));
}

void run_tcp_echo_roundtrip_benchmark(benchmark::State &state, std::size_t connection_count,
                                      std::size_t payload_size) {
    af::runtime_config runtime_config;
    runtime_config.threads = {af::io_threads("bench-tcp-io", 1)};
    runtime_config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(runtime_config);
    if (!runtime.start()) {
        state.SkipWithError("failed to start runtime");
        return;
    }
    if (!wait_until([&] { return runtime.active_thread_count() == 1; })) {
        runtime.stop();
        state.SkipWithError("runtime thread did not start");
        return;
    }

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server_config server_config;
    server_config.connection.read_buffer_size = 64U * 1024U;
    server_config.connection.read_budget_bytes = 2U * 1024U * 1024U;
    server_config.connection.write_budget_bytes = 2U * 1024U * 1024U;

    af::net::tcp_server server(runtime, server_config);
    std::atomic<bool> started{false};
    std::atomic<bool> start_ok{false};
    std::atomic<bool> stopped{false};
    std::atomic<bool> stop_ok{false};
    std::atomic<std::uint16_t> port{0};
    auto stop_runtime = [&] {
        bool ok = true;
        if (started.load(std::memory_order_acquire)) {
            stopped.store(false, std::memory_order_release);
            stop_ok.store(false, std::memory_order_release);
            ok = runtime.post(io_thread,
                              [&] {
                                  const bool stopped_server = !server.running() || server.stop();
                                  stop_ok.store(stopped_server, std::memory_order_release);
                                  stopped.store(true, std::memory_order_release);
                              }) &&
                 wait_until([&] { return stopped.load(std::memory_order_acquire); }) &&
                 stop_ok.load(std::memory_order_acquire);
        }
        runtime.stop();
        return ok;
    };

    if (!runtime.post(io_thread, [&] {
            af::net::tcp_connection_callbacks callbacks;
            callbacks.on_read = &on_tcp_echo_read;

            af::net::tcp_listener_config listener_config;
            listener_config.name = "benchmark-tcp-echo";
            listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
            listener_config.threads = {io_thread};
            listener_config.options.reuse_port = false;
            listener_config.options.read_buffer_size = server_config.connection.read_buffer_size;
            listener_config.options.read_budget_bytes = server_config.connection.read_budget_bytes;
            listener_config.options.write_budget_bytes =
                server_config.connection.write_budget_bytes;

            const af::net::listener_result listener =
                server.add_listener(std::move(listener_config), callbacks);
            bool ok = listener.ok() && server.start();
            const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
            ok = ok && endpoint != nullptr;
            if (ok) {
                port.store(endpoint->port, std::memory_order_release);
            }
            start_ok.store(ok, std::memory_order_release);
            started.store(true, std::memory_order_release);
        })) {
        stop_runtime();
        state.SkipWithError("failed to post tcp server start");
        return;
    }
    if (!wait_until([&] { return started.load(std::memory_order_acquire); }) ||
        !start_ok.load(std::memory_order_acquire)) {
        stop_runtime();
        state.SkipWithError("tcp server did not start");
        return;
    }

    std::vector<unique_fd> clients;
    clients.reserve(connection_count);
    for (std::size_t i = 0; i < connection_count; ++i) {
        unique_fd client = connect_loopback(port.load(std::memory_order_acquire));
        if (client.get() < 0 || !set_io_timeout(client.get())) {
            clients.clear();
            stop_runtime();
            state.SkipWithError("failed to connect benchmark clients");
            return;
        }
        clients.push_back(std::move(client));
    }

    if (clients.empty()) {
        stop_runtime();
        state.SkipWithError("tcp echo benchmark requires at least one connection");
        return;
    }

    std::vector<std::byte> payload(payload_size);
    std::vector<std::byte> received(payload_size);
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<std::byte>(i & 0xffU);
    }
    for (unique_fd &client : clients) {
        if (!echo_roundtrip(client.get(), payload, received, true)) {
            clients.clear();
            stop_runtime();
            state.SkipWithError("tcp echo warmup failed");
            return;
        }
    }

    bool failed = false;
    for (auto _ : state) {
        for (unique_fd &client : clients) {
            if (!send_all(client.get(), payload.data(), payload.size())) {
                state.SkipWithError("tcp echo send failed");
                failed = true;
                break;
            }
        }
        if (failed) {
            break;
        }
        for (unique_fd &client : clients) {
            if (!recv_exact(client.get(), received.data(), received.size())) {
                state.SkipWithError("tcp echo receive failed");
                failed = true;
                break;
            }
            benchmark::DoNotOptimize(received.data());
        }
        if (failed) {
            break;
        }
    }

    clients.clear();
    if (!stop_runtime()) {
        state.SkipWithError("tcp server stop failed");
    }

    if (!failed) {
        const auto item_count = static_cast<std::int64_t>(connection_count);
        state.SetItemsProcessed(state.iterations() * item_count);
        state.SetBytesProcessed(state.iterations() * item_count *
                                static_cast<std::int64_t>(payload_size) * 2);
        state.counters["connections"] = benchmark::Counter(static_cast<double>(connection_count));
        state.counters["payload_bytes"] = benchmark::Counter(static_cast<double>(payload_size));
    }
}

void BM_TcpEchoRoundTrip(benchmark::State &state) {
    run_tcp_echo_roundtrip_benchmark(state, 1U, static_cast<std::size_t>(state.range(0)));
}

void BM_TcpEchoMultiConnectionRoundTrip(benchmark::State &state) {
    run_tcp_echo_roundtrip_benchmark(state, static_cast<std::size_t>(state.range(0)),
                                     static_cast<std::size_t>(state.range(1)));
}

BENCHMARK(BM_TcpEchoRoundTrip)
    ->Arg(64)
    ->Arg(1024)
    ->Arg(4096)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_TcpEchoMultiConnectionRoundTrip)
    ->Args({16, 64})
    ->Args({16, 1024})
    ->Args({64, 64})
    ->Args({64, 1024})
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond);

} // namespace
