#include <array>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "af/net.hpp"
#include "af/platform.hpp"
#include "af/runtime.hpp"

#include "gtest/gtest.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

namespace {

class UniqueFd {
public:
    explicit UniqueFd(int fd = -1) noexcept : fd_(fd) {}

    ~UniqueFd() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    UniqueFd(const UniqueFd &) = delete;
    UniqueFd &operator=(const UniqueFd &) = delete;

    UniqueFd(UniqueFd &&other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

    UniqueFd &operator=(UniqueFd &&other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) {
                ::close(fd_);
            }
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

private:
    int fd_{-1};
};

[[nodiscard]] std::uint16_t reserve_udp_loopback_port() {
    UniqueFd fd(::socket(AF_INET, SOCK_DGRAM, 0));
    EXPECT_GE(fd.get(), 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    EXPECT_EQ(::bind(fd.get(), reinterpret_cast<const sockaddr *>(&address), sizeof(address)), 0);
    socklen_t size = sizeof(address);
    EXPECT_EQ(::getsockname(fd.get(), reinterpret_cast<sockaddr *>(&address), &size), 0);
    return ntohs(address.sin_port);
}

[[nodiscard]] bool reserve_udp_loopback_v6_port(std::uint16_t &port) {
    UniqueFd fd(::socket(AF_INET6, SOCK_DGRAM, 0));
    if (fd.get() < 0) {
        return false;
    }
    sockaddr_in6 address{};
    address.sin6_family = AF_INET6;
    address.sin6_addr = in6addr_loopback;
    address.sin6_port = 0;
    if (::bind(fd.get(), reinterpret_cast<const sockaddr *>(&address), sizeof(address)) != 0) {
        return false;
    }
    socklen_t size = sizeof(address);
    if (::getsockname(fd.get(), reinterpret_cast<sockaddr *>(&address), &size) != 0) {
        return false;
    }
    port = ntohs(address.sin6_port);
    return true;
}

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

void set_recv_timeout(int fd, long seconds = 2, long microseconds = 0) {
    timeval timeout{};
    timeout.tv_sec = seconds;
    timeout.tv_usec = microseconds;
    static_cast<void>(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
}

struct RuntimeUdpState {
    std::atomic<bool> started{false};
    std::atomic<bool> start_ok{false};
    std::atomic<bool> stopped{false};
    std::atomic<bool> stop_ok{false};
    std::atomic<int> datagrams{0};
    std::atomic<int> errors{0};
    std::atomic<int> last_error{0};
    std::atomic<int> peer_family{0};
    std::atomic<int> owner_send_result{-1};
    std::atomic<std::uint16_t> port{0};
    std::atomic<std::uint16_t> peer_port{0};
    std::atomic<bool> endpoint_error{false};
    std::array<char, 64> data{};
    std::atomic<std::size_t> size{0};
    af::net::udp_socket_handle handle;
};

void runtime_udp_echo_datagram(void *owner, af::net::udp_socket_ref socket, af::buffer_view bytes,
                               const af::net::udp_peer &peer) noexcept {
    auto *state = static_cast<RuntimeUdpState *>(owner);
    if (state != nullptr) {
        state->datagrams.fetch_add(1, std::memory_order_release);
    }
    static_cast<void>(socket.send_to(bytes, peer));
}

void runtime_udp_peer_echo_datagram(void *owner, af::net::udp_socket_ref socket,
                                    af::buffer_view bytes, const af::net::udp_peer &peer) noexcept {
    auto *state = static_cast<RuntimeUdpState *>(owner);
    if (state != nullptr) {
        try {
            const af::net::udp_endpoint endpoint = peer.endpoint();
            state->peer_family.store(peer.native_family(), std::memory_order_release);
            state->peer_port.store(endpoint.port, std::memory_order_release);
        } catch (...) {
            state->endpoint_error.store(true, std::memory_order_release);
        }
        state->datagrams.fetch_add(1, std::memory_order_release);
    }
    static_cast<void>(socket.send_to(bytes, peer));
}

void runtime_udp_capture_datagram(void *owner, af::net::udp_socket_ref socket,
                                  af::buffer_view bytes, const af::net::udp_peer &peer) noexcept {
    static_cast<void>(peer);
    auto *state = static_cast<RuntimeUdpState *>(owner);
    if (state == nullptr) {
        return;
    }
    state->handle = socket.handle();
    const std::size_t size = std::min(bytes.size(), state->data.size());
    std::memcpy(state->data.data(), bytes.data(), size);
    state->size.store(size, std::memory_order_release);
    state->datagrams.fetch_add(1, std::memory_order_release);
}

void runtime_udp_owner_handle_echo(void *owner, af::net::udp_socket_ref socket,
                                   af::buffer_view bytes, const af::net::udp_peer &peer) noexcept {
    auto *state = static_cast<RuntimeUdpState *>(owner);
    const af::net::udp_send_result result = socket.handle().send_to(bytes, peer);
    if (state != nullptr) {
        state->owner_send_result.store(static_cast<int>(result), std::memory_order_release);
        state->datagrams.fetch_add(1, std::memory_order_release);
    }
}

void runtime_udp_noop_datagram(void *owner, af::net::udp_socket_ref socket, af::buffer_view bytes,
                               const af::net::udp_peer &peer) noexcept {
    static_cast<void>(socket);
    static_cast<void>(bytes);
    static_cast<void>(peer);
    auto *state = static_cast<RuntimeUdpState *>(owner);
    if (state != nullptr) {
        state->datagrams.fetch_add(1, std::memory_order_release);
    }
}

void runtime_udp_counting_echo_datagram(void *owner, af::net::udp_socket_ref socket,
                                        af::buffer_view bytes,
                                        const af::net::udp_peer &peer) noexcept {
    auto *state = static_cast<RuntimeUdpState *>(owner);
    if (state != nullptr) {
        state->datagrams.fetch_add(1, std::memory_order_release);
    }
    static_cast<void>(socket.send_to(bytes, peer));
}

void runtime_udp_error(void *owner, af::net::udp_socket_handle socket, int error) noexcept {
    static_cast<void>(socket);
    auto *state = static_cast<RuntimeUdpState *>(owner);
    if (state != nullptr) {
        state->last_error.store(error, std::memory_order_release);
        state->errors.fetch_add(1, std::memory_order_release);
    }
}

[[nodiscard]] af::runtime_config make_udp_runtime_config(std::size_t io_thread_count = 1) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", io_thread_count)};
    config.logger.consumer_thread = af::thread_selector::io(0);
    return config;
}

void reset_udp_start_state(RuntimeUdpState &state) noexcept {
    state.started.store(false, std::memory_order_release);
    state.start_ok.store(false, std::memory_order_release);
    state.port.store(0, std::memory_order_release);
    state.handle = af::net::udp_socket_handle{};
}

void reset_udp_stop_state(RuntimeUdpState &state) noexcept {
    state.stopped.store(false, std::memory_order_release);
    state.stop_ok.store(false, std::memory_order_release);
}

[[nodiscard]] bool try_start_udp_socket(af::runtime &runtime, af::thread_ref owner_thread,
                                        af::net::udp_socket &socket,
                                        af::net::udp_socket_config config, RuntimeUdpState &state,
                                        af::net::udp_datagram_callback on_datagram) {
    reset_udp_start_state(state);
    af::net::udp_socket_callbacks callbacks;
    callbacks.owner = &state;
    callbacks.on_datagram = on_datagram;
    callbacks.on_error = &runtime_udp_error;

    if (!runtime.post(owner_thread, [&socket, config = std::move(config), callbacks, &state,
                                     owner_thread]() mutable {
            const bool ok = socket.start(std::move(config), callbacks);
            if (ok) {
                state.handle = socket.handle_for_thread(owner_thread);
                const af::net::udp_endpoint *endpoint = socket.local_endpoint(owner_thread);
                if (endpoint != nullptr) {
                    state.port.store(endpoint->port, std::memory_order_release);
                }
            }
            state.start_ok.store(ok, std::memory_order_release);
            state.started.store(true, std::memory_order_release);
        })) {
        return false;
    }
    return wait_until([&] { return state.started.load(std::memory_order_acquire); });
}

[[nodiscard]] bool start_udp_socket(af::runtime &runtime, af::thread_ref owner_thread,
                                    af::net::udp_socket &socket, af::net::udp_socket_config config,
                                    RuntimeUdpState &state,
                                    af::net::udp_datagram_callback on_datagram,
                                    std::size_t expected_shards = 1) {
    if (!try_start_udp_socket(runtime, owner_thread, socket, std::move(config), state,
                              on_datagram)) {
        return false;
    }
    if (!state.start_ok.load(std::memory_order_acquire)) {
        return false;
    }
    return wait_until([&] { return socket.active_shard_count() == expected_shards; });
}

[[nodiscard]] bool stop_udp_socket(af::runtime &runtime, af::thread_ref owner_thread,
                                   af::net::udp_socket &socket, RuntimeUdpState &state) {
    reset_udp_stop_state(state);
    if (!runtime.post(owner_thread, [&socket, &state] {
            state.stop_ok.store(socket.stop(), std::memory_order_release);
            state.stopped.store(true, std::memory_order_release);
        })) {
        return false;
    }
    return wait_until([&] { return state.stopped.load(std::memory_order_acquire); }) &&
           state.stop_ok.load(std::memory_order_acquire);
}

} // namespace

TEST(NetUdpSocketTests, RuntimeUdpSocketEchoesDatagramToRawClient) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeUdpState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket socket(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::udp_socket_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_datagram = &runtime_udp_echo_datagram;
        callbacks.on_error = &runtime_udp_error;

        af::net::udp_socket_config socket_config;
        socket_config.name = "runtime-udp-echo";
        socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
        socket_config.threads = {io_thread};
        socket_config.options.reuse_port = false;

        bool ok = socket.start(std::move(socket_config), callbacks);
        const af::net::udp_endpoint *endpoint = socket.local_endpoint(io_thread);
        ok = ok && endpoint != nullptr;
        if (ok) {
            state.port.store(endpoint->port, std::memory_order_release);
        }
        state.start_ok.store(ok, std::memory_order_release);
        state.started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return state.started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(state.start_ok.load(std::memory_order_acquire));
    ASSERT_GT(state.port.load(std::memory_order_acquire), 0U);

    UniqueFd client(::socket(AF_INET, SOCK_DGRAM, 0));
    ASSERT_GE(client.get(), 0);
    set_recv_timeout(client.get());

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(state.port.load(std::memory_order_acquire));
    ASSERT_EQ(::sendto(client.get(), "hello", 5, 0, reinterpret_cast<const sockaddr *>(&address),
                       sizeof(address)),
              5);

    char buffer[32]{};
    const ssize_t n = ::recv(client.get(), buffer, sizeof(buffer), 0);
    ASSERT_EQ(n, 5);
    EXPECT_EQ(std::string(buffer, static_cast<std::size_t>(n)), "hello");
    EXPECT_EQ(state.errors.load(std::memory_order_acquire), 0);
    EXPECT_GE(state.datagrams.load(std::memory_order_acquire), 1);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        state.stop_ok.store(socket.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetUdpSocketTests, RuntimeUdpSocketOwnerHandleSendsWithoutQueueing) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeUdpState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket socket(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::udp_socket_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_datagram = &runtime_udp_owner_handle_echo;
        callbacks.on_error = &runtime_udp_error;

        af::net::udp_socket_config socket_config;
        socket_config.name = "runtime-udp-owner-handle";
        socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
        socket_config.threads = {io_thread};
        socket_config.options.reuse_port = false;

        bool ok = socket.start(std::move(socket_config), callbacks);
        const af::net::udp_endpoint *endpoint = socket.local_endpoint(io_thread);
        ok = ok && endpoint != nullptr;
        if (ok) {
            state.port.store(endpoint->port, std::memory_order_release);
        }
        state.start_ok.store(ok, std::memory_order_release);
        state.started.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(state.start_ok.load(std::memory_order_acquire));

    UniqueFd client(::socket(AF_INET, SOCK_DGRAM, 0));
    ASSERT_GE(client.get(), 0);
    set_recv_timeout(client.get());

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(state.port.load(std::memory_order_acquire));
    ASSERT_EQ(::sendto(client.get(), "owner", 5, 0, reinterpret_cast<const sockaddr *>(&address),
                       sizeof(address)),
              5);

    char buffer[32]{};
    const ssize_t n = ::recv(client.get(), buffer, sizeof(buffer), 0);
    ASSERT_EQ(n, 5);
    EXPECT_EQ(std::string(buffer, static_cast<std::size_t>(n)), "owner");
    ASSERT_TRUE(
        wait_until([&] { return state.owner_send_result.load(std::memory_order_acquire) != -1; }));
    EXPECT_EQ(state.owner_send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::udp_send_result::accepted));

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        state.stop_ok.store(socket.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetUdpSocketTests, RuntimeUdpConnectedClientSendsThroughHandle) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeUdpState server_state;
    RuntimeUdpState client_state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket server(runtime);
    af::net::udp_socket client(runtime);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::udp_socket_callbacks callbacks;
        callbacks.owner = &server_state;
        callbacks.on_datagram = &runtime_udp_echo_datagram;
        callbacks.on_error = &runtime_udp_error;

        af::net::udp_socket_config socket_config;
        socket_config.name = "runtime-udp-connected-server";
        socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
        socket_config.threads = {io_thread};
        socket_config.options.reuse_port = false;

        bool ok = server.start(std::move(socket_config), callbacks);
        const af::net::udp_endpoint *endpoint = server.local_endpoint(io_thread);
        ok = ok && endpoint != nullptr;
        if (ok) {
            server_state.port.store(endpoint->port, std::memory_order_release);
        }
        server_state.start_ok.store(ok, std::memory_order_release);
        server_state.started.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return server_state.started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(server_state.start_ok.load(std::memory_order_acquire));

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::udp_socket_callbacks callbacks;
        callbacks.owner = &client_state;
        callbacks.on_datagram = &runtime_udp_capture_datagram;
        callbacks.on_error = &runtime_udp_error;

        af::net::udp_socket_config socket_config;
        socket_config.name = "runtime-udp-connected-client";
        socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
        socket_config.remote_endpoint =
            af::net::udp_endpoint::loopback_v4(server_state.port.load(std::memory_order_acquire));
        socket_config.threads = {io_thread};
        socket_config.options.reuse_port = false;
        socket_config.connect_remote = true;

        const bool ok = client.start(std::move(socket_config), callbacks);
        if (ok) {
            client_state.handle = client.handle();
        }
        client_state.start_ok.store(ok, std::memory_order_release);
        client_state.started.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return client_state.started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(client_state.start_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(client_state.handle.valid());

    EXPECT_EQ(client_state.handle.send(af::buffer::copy("ping", 4)),
              af::net::udp_send_result::queued);
    ASSERT_TRUE(
        wait_until([&] { return client_state.size.load(std::memory_order_acquire) == 4U; }));
    EXPECT_EQ(
        std::string(client_state.data.data(), client_state.size.load(std::memory_order_acquire)),
        "ping");
    EXPECT_EQ(client_state.errors.load(std::memory_order_acquire), 0);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        client_state.stop_ok.store(client.stop(), std::memory_order_release);
        client_state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return client_state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(client_state.stop_ok.load(std::memory_order_acquire));
    EXPECT_EQ(client_state.handle.send(af::buffer::copy("after", 5)),
              af::net::udp_send_result::closed);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        server_state.stop_ok.store(server.stop(), std::memory_order_release);
        server_state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return server_state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(server_state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetUdpSocketTests, RuntimeUdpConnectedClientUsesMultipleIoShards) {
    const std::uint16_t port = reserve_udp_loopback_port();

    UniqueFd sink(::socket(AF_INET, SOCK_DGRAM, 0));
    ASSERT_GE(sink.get(), 0);
    set_recv_timeout(sink.get());
    sockaddr_in sink_address{};
    sink_address.sin_family = AF_INET;
    sink_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sink_address.sin_port = htons(port);
    ASSERT_EQ(
        ::bind(sink.get(), reinterpret_cast<const sockaddr *>(&sink_address), sizeof(sink_address)),
        0);

    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 2)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeUdpState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 2; }));

    const af::thread_ref io_thread0 = runtime.select_thread_ref(af::thread_selector::io(0));
    const af::thread_ref io_thread1 = runtime.select_thread_ref(af::thread_selector::io(1));
    af::net::udp_socket socket(runtime);

    ASSERT_TRUE(runtime.post(io_thread0, [&] {
        af::net::udp_socket_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_datagram = &runtime_udp_capture_datagram;
        callbacks.on_error = &runtime_udp_error;

        af::net::udp_socket_config socket_config;
        socket_config.name = "runtime-udp-two-shard-client";
        socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
        socket_config.remote_endpoint = af::net::udp_endpoint::loopback_v4(port);
        socket_config.threads = {io_thread0, io_thread1};
        socket_config.options.reuse_port = true;
        socket_config.connect_remote = true;

        const bool ok = socket.start(std::move(socket_config), callbacks);
        state.start_ok.store(ok, std::memory_order_release);
        state.started.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(state.start_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(wait_until([&] { return socket.active_shard_count() == 2U; }));

    const std::vector<af::net::udp_socket_handle> handles = socket.handles();
    ASSERT_EQ(handles.size(), 2U);
    EXPECT_TRUE(socket.handle_for_thread(io_thread0).valid());
    EXPECT_TRUE(socket.handle_for_thread(io_thread1).valid());

    EXPECT_EQ(handles[0].send(af::buffer::copy("one", 3)), af::net::udp_send_result::queued);
    EXPECT_EQ(handles[1].send(af::buffer::copy("two", 3)), af::net::udp_send_result::queued);

    std::array<char, 8> buffer{};
    std::vector<std::string> received;
    for (int i = 0; i < 2; ++i) {
        const ssize_t n = ::recv(sink.get(), buffer.data(), buffer.size(), 0);
        ASSERT_GT(n, 0);
        received.emplace_back(buffer.data(), static_cast<std::size_t>(n));
    }
    std::sort(received.begin(), received.end());
    EXPECT_EQ(received, (std::vector<std::string>{"one", "two"}));

    ASSERT_TRUE(runtime.post(io_thread0, [&] {
        state.stop_ok.store(socket.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(wait_until([&] { return socket.active_shard_count() == 0U; }));
    EXPECT_EQ(handles[0].send(af::buffer::copy("after", 5)), af::net::udp_send_result::closed);

    runtime.stop();
}

TEST(NetUdpSocketTests, EchoesDatagramToRawClient) {
    const std::uint16_t port = reserve_udp_loopback_port();

    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket server(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-echo";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(port);
    socket_config.threads = {io_thread};
    socket_config.options.reuse_port = false;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, server, std::move(socket_config), state,
                                 &runtime_udp_echo_datagram));

    UniqueFd client(::socket(AF_INET, SOCK_DGRAM, 0));
    ASSERT_GE(client.get(), 0);
    set_recv_timeout(client.get());

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    ASSERT_EQ(::sendto(client.get(), "hello", 5, 0, reinterpret_cast<const sockaddr *>(&address),
                       sizeof(address)),
              5);

    char buffer[32]{};
    const ssize_t n = ::recv(client.get(), buffer, sizeof(buffer), 0);
    ASSERT_EQ(n, 5);
    EXPECT_EQ(std::string(buffer, static_cast<std::size_t>(n)), "hello");

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, server, state));
    runtime.stop();
}

TEST(NetUdpSocketTests, EchoesDatagramUsingCachedPeerAddress) {
    const std::uint16_t port = reserve_udp_loopback_port();

    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket server(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-peer-echo";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(port);
    socket_config.threads = {io_thread};
    socket_config.options.reuse_port = false;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, server, std::move(socket_config), state,
                                 &runtime_udp_peer_echo_datagram));

    UniqueFd client(::socket(AF_INET, SOCK_DGRAM, 0));
    ASSERT_GE(client.get(), 0);
    set_recv_timeout(client.get());

    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    local.sin_port = 0;
    ASSERT_EQ(::bind(client.get(), reinterpret_cast<const sockaddr *>(&local), sizeof(local)), 0);

    socklen_t local_size = sizeof(local);
    ASSERT_EQ(::getsockname(client.get(), reinterpret_cast<sockaddr *>(&local), &local_size), 0);
    const std::uint16_t client_port = ntohs(local.sin_port);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    ASSERT_EQ(::sendto(client.get(), "peer", 4, 0, reinterpret_cast<const sockaddr *>(&address),
                       sizeof(address)),
              4);

    char buffer[32]{};
    const ssize_t n = ::recv(client.get(), buffer, sizeof(buffer), 0);
    ASSERT_EQ(n, 4);
    EXPECT_EQ(std::string(buffer, static_cast<std::size_t>(n)), "peer");
    EXPECT_FALSE(state.endpoint_error.load(std::memory_order_acquire));
    EXPECT_EQ(state.peer_family.load(std::memory_order_acquire), AF_INET);
    EXPECT_EQ(state.peer_port.load(std::memory_order_acquire), client_port);

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, server, state));
    runtime.stop();
}

TEST(NetUdpSocketTests, OwnerHandleSendsBufferViewWithoutQueueing) {
    const std::uint16_t port = reserve_udp_loopback_port();

    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket server(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-owner-handle-echo";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(port);
    socket_config.threads = {io_thread};
    socket_config.options.reuse_port = false;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, server, std::move(socket_config), state,
                                 &runtime_udp_owner_handle_echo));

    UniqueFd client(::socket(AF_INET, SOCK_DGRAM, 0));
    ASSERT_GE(client.get(), 0);
    set_recv_timeout(client.get());

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    ASSERT_EQ(::sendto(client.get(), "owner", 5, 0, reinterpret_cast<const sockaddr *>(&address),
                       sizeof(address)),
              5);

    char buffer[32]{};
    const ssize_t n = ::recv(client.get(), buffer, sizeof(buffer), 0);
    ASSERT_EQ(n, 5);
    EXPECT_EQ(std::string(buffer, static_cast<std::size_t>(n)), "owner");
    EXPECT_EQ(state.owner_send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::udp_send_result::accepted));

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, server, state));
    runtime.stop();
}

TEST(NetUdpSocketTests, EchoesIpv6DatagramToRawClient) {
    std::uint16_t port = 0;
    if (!reserve_udp_loopback_v6_port(port)) {
        GTEST_SKIP() << "IPv6 loopback is not available";
    }

    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket server(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-v6-echo";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v6(port);
    socket_config.threads = {io_thread};
    socket_config.options.reuse_port = false;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, server, std::move(socket_config), state,
                                 &runtime_udp_echo_datagram));

    UniqueFd client(::socket(AF_INET6, SOCK_DGRAM, 0));
    if (client.get() < 0) {
        static_cast<void>(stop_udp_socket(runtime, io_thread, server, state));
        runtime.stop();
        GTEST_SKIP() << "IPv6 datagram socket is not available";
    }
    set_recv_timeout(client.get());

    sockaddr_in6 address{};
    address.sin6_family = AF_INET6;
    address.sin6_addr = in6addr_loopback;
    address.sin6_port = htons(port);
    ASSERT_EQ(::sendto(client.get(), "hello6", 6, 0, reinterpret_cast<const sockaddr *>(&address),
                       sizeof(address)),
              6);

    char buffer[32]{};
    const ssize_t n = ::recv(client.get(), buffer, sizeof(buffer), 0);
    ASSERT_EQ(n, 6);
    EXPECT_EQ(std::string(buffer, static_cast<std::size_t>(n)), "hello6");

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, server, state));
    runtime.stop();
}

TEST(NetUdpSocketTests, ReceiveBufferGrowsToMaxDatagramSize) {
    const std::uint16_t port = reserve_udp_loopback_port();

    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket server(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-small-buffer-large-datagram";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(port);
    socket_config.threads = {io_thread};
    socket_config.options.receive_buffer_size = 4;
    socket_config.options.max_datagram_size = 8;
    socket_config.options.reuse_port = false;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, server, std::move(socket_config), state,
                                 &runtime_udp_echo_datagram));

    UniqueFd client(::socket(AF_INET, SOCK_DGRAM, 0));
    ASSERT_GE(client.get(), 0);
    set_recv_timeout(client.get());

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    ASSERT_EQ(::sendto(client.get(), "abcdef", 6, 0, reinterpret_cast<const sockaddr *>(&address),
                       sizeof(address)),
              6);

    char buffer[32]{};
    const ssize_t n = ::recv(client.get(), buffer, sizeof(buffer), 0);
    ASSERT_EQ(n, 6);
    EXPECT_EQ(std::string(buffer, static_cast<std::size_t>(n)), "abcdef");

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, server, state));
    runtime.stop();
}

TEST(NetUdpSocketTests, OversizedDatagramReportsMessageSizeAndDropsPacket) {
    const std::uint16_t port = reserve_udp_loopback_port();

    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket server(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-oversize-drop";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(port);
    socket_config.threads = {io_thread};
    socket_config.options.receive_buffer_size = 8;
    socket_config.options.max_datagram_size = 4;
    socket_config.options.reuse_port = false;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, server, std::move(socket_config), state,
                                 &runtime_udp_counting_echo_datagram));

    UniqueFd client(::socket(AF_INET, SOCK_DGRAM, 0));
    ASSERT_GE(client.get(), 0);
    set_recv_timeout(client.get(), 0, 100000);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    ASSERT_EQ(::sendto(client.get(), "abcdef", 6, 0, reinterpret_cast<const sockaddr *>(&address),
                       sizeof(address)),
              6);

    ASSERT_TRUE(
        wait_until([&] { return state.last_error.load(std::memory_order_acquire) == EMSGSIZE; }));
    EXPECT_EQ(state.datagrams.load(std::memory_order_acquire), 0);

    char buffer[32]{};
    EXPECT_LT(::recv(client.get(), buffer, sizeof(buffer), 0), 0);

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, server, state));
    runtime.stop();
}

TEST(NetUdpSocketTests, HandlesExposeBoundShardsAndDefaultHandleRoundRobins) {
    const std::uint16_t port = reserve_udp_loopback_port();

    UniqueFd sink(::socket(AF_INET, SOCK_DGRAM, 0));
    ASSERT_GE(sink.get(), 0);
    set_recv_timeout(sink.get());
    sockaddr_in sink_address{};
    sink_address.sin_family = AF_INET;
    sink_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sink_address.sin_port = htons(port);
    ASSERT_EQ(
        ::bind(sink.get(), reinterpret_cast<const sockaddr *>(&sink_address), sizeof(sink_address)),
        0);

    af::runtime runtime(make_udp_runtime_config(2));
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 2; }));

    const af::thread_ref io_thread0 = runtime.select_thread_ref(af::thread_selector::io(0));
    const af::thread_ref io_thread1 = runtime.select_thread_ref(af::thread_selector::io(1));
    af::net::udp_socket client(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-two-shard-client";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    socket_config.remote_endpoint = af::net::udp_endpoint::loopback_v4(port);
    socket_config.threads = {io_thread0, io_thread1};
    socket_config.options.reuse_port = true;
    socket_config.connect_remote = true;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread0, client, std::move(socket_config), state,
                                 &runtime_udp_noop_datagram, 2));

    const std::vector<af::net::udp_socket_handle> handles = client.handles();
    ASSERT_EQ(handles.size(), 2U);
    EXPECT_NE(handles[0].owner_thread(), handles[1].owner_thread());

    const af::net::udp_socket_handle first = client.handle();
    const af::net::udp_socket_handle second = client.handle();
    ASSERT_TRUE(first.valid());
    ASSERT_TRUE(second.valid());
    EXPECT_NE(first.owner_thread(), second.owner_thread());
    EXPECT_EQ(client.handle_for_thread(first.owner_thread()).owner_thread(), first.owner_thread());

    EXPECT_EQ(first.send(af::buffer::copy("one", 3)), af::net::udp_send_result::queued);
    EXPECT_EQ(second.send(af::buffer::copy("two", 3)), af::net::udp_send_result::queued);

    std::array<char, 8> buffer{};
    std::vector<std::string> received;
    for (int i = 0; i < 2; ++i) {
        const ssize_t n = ::recv(sink.get(), buffer.data(), buffer.size(), 0);
        ASSERT_GT(n, 0);
        received.emplace_back(buffer.data(), static_cast<std::size_t>(n));
    }
    std::sort(received.begin(), received.end());
    EXPECT_EQ(received, (std::vector<std::string>{"one", "two"}));

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread0, client, state));
    EXPECT_TRUE(client.handles().empty());
    EXPECT_EQ(client.handle().send(af::buffer::copy("stopped", 7)),
              af::net::udp_send_result::closed);
    EXPECT_EQ(client.handle_for_thread(first.owner_thread()).send(af::buffer::copy("stopped", 7)),
              af::net::udp_send_result::closed);
    runtime.stop();
}

TEST(NetUdpSocketTests, ConnectedClientSendsThroughRuntimeTask) {
    const std::uint16_t port = reserve_udp_loopback_port();
    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState server_state;
    RuntimeUdpState client_state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket server(runtime);
    af::net::udp_socket_config server_config;
    server_config.name = "udp-echo-server";
    server_config.local_endpoint = af::net::udp_endpoint::loopback_v4(port);
    server_config.threads = {io_thread};
    server_config.options.reuse_port = false;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, server, std::move(server_config), server_state,
                                 &runtime_udp_echo_datagram));

    af::net::udp_socket client(runtime);
    af::net::udp_socket_config client_config;
    client_config.name = "udp-connected-client";
    client_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    client_config.remote_endpoint = af::net::udp_endpoint::loopback_v4(port);
    client_config.threads = {io_thread};
    client_config.options.reuse_port = false;
    client_config.connect_remote = true;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, client, std::move(client_config), client_state,
                                 &runtime_udp_capture_datagram));

    EXPECT_EQ(client_state.handle.send(af::buffer::copy("ping", 4)),
              af::net::udp_send_result::queued);
    ASSERT_TRUE(
        wait_until([&] { return client_state.size.load(std::memory_order_acquire) == 4U; }));
    EXPECT_EQ(
        std::string(client_state.data.data(), client_state.size.load(std::memory_order_acquire)),
        "ping");

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, client, client_state));
    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, server, server_state));
    runtime.stop();
}

TEST(NetUdpSocketTests, ConnectedClientAcceptsInferredIpv4RemoteFamily) {
    const std::uint16_t port = reserve_udp_loopback_port();
    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState server_state;
    RuntimeUdpState client_state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket server(runtime);
    af::net::udp_socket_config server_config;
    server_config.name = "udp-inferred-family-echo-server";
    server_config.local_endpoint = af::net::udp_endpoint::loopback_v4(port);
    server_config.threads = {io_thread};
    server_config.options.reuse_port = false;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, server, std::move(server_config), server_state,
                                 &runtime_udp_echo_datagram));

    af::net::udp_socket client(runtime);
    af::net::udp_socket_config client_config;
    client_config.name = "udp-inferred-family-client";
    client_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    client_config.remote_endpoint = af::net::udp_endpoint::host("127.0.0.1", port);
    client_config.threads = {io_thread};
    client_config.options.reuse_port = false;
    client_config.connect_remote = true;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, client, std::move(client_config), client_state,
                                 &runtime_udp_capture_datagram));

    EXPECT_EQ(client_state.handle.send(af::buffer::copy("ping", 4)),
              af::net::udp_send_result::queued);
    ASSERT_TRUE(
        wait_until([&] { return client_state.size.load(std::memory_order_acquire) == 4U; }));
    EXPECT_EQ(
        std::string(client_state.data.data(), client_state.size.load(std::memory_order_acquire)),
        "ping");

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, client, client_state));
    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, server, server_state));
    runtime.stop();
}

TEST(NetUdpSocketTests, RejectsConnectedRemoteAddressFamilyMismatchSynchronously) {
    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket socket(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-remote-family-mismatch";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    socket_config.remote_endpoint =
        af::net::udp_endpoint::host("::1", 12345, af::net::address_family::unspecified);
    socket_config.threads = {io_thread};
    socket_config.connect_remote = true;
    ASSERT_TRUE(try_start_udp_socket(runtime, io_thread, socket, std::move(socket_config), state,
                                     &runtime_udp_noop_datagram));
    EXPECT_FALSE(state.start_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetUdpSocketTests, RejectsConnectedIpRemotePortZeroSynchronously) {
    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket socket(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-remote-zero-port";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    socket_config.remote_endpoint = af::net::udp_endpoint::loopback_v4(0);
    socket_config.threads = {io_thread};
    socket_config.connect_remote = true;
    ASSERT_TRUE(try_start_udp_socket(runtime, io_thread, socket, std::move(socket_config), state,
                                     &runtime_udp_noop_datagram));
    EXPECT_FALSE(state.start_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetUdpSocketTests, StoppedSocketHandlesReportClosedForSends) {
    const std::uint16_t port = reserve_udp_loopback_port();

    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket socket(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-stopped-handle";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    socket_config.remote_endpoint = af::net::udp_endpoint::loopback_v4(port);
    socket_config.threads = {io_thread};
    socket_config.options.reuse_port = false;
    socket_config.connect_remote = true;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, socket, std::move(socket_config), state,
                                 &runtime_udp_noop_datagram));

    const af::net::udp_socket_handle handle = state.handle;
    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, socket, state));

    const std::string_view payload = "x";
    EXPECT_EQ(handle.send(af::buffer::copy("x", 1)), af::net::udp_send_result::closed);
    EXPECT_EQ(handle.send(af::buffer_view(payload.data(), payload.size())),
              af::net::udp_send_result::closed);
    EXPECT_EQ(handle.send_to(af::buffer::copy("x", 1), af::net::udp_endpoint::loopback_v4(port)),
              af::net::udp_send_result::closed);
    EXPECT_EQ(handle.send_to(af::buffer_view(payload.data(), payload.size()),
                             af::net::udp_endpoint::loopback_v4(port)),
              af::net::udp_send_result::closed);

    runtime.stop();
}

TEST(NetUdpSocketTests, StopPublishesClosedStateBeforeOwnerStops) {
    const std::uint16_t port = reserve_udp_loopback_port();

    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket socket(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-stop-in-progress";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    socket_config.remote_endpoint = af::net::udp_endpoint::loopback_v4(port);
    socket_config.threads = {io_thread};
    socket_config.options.reuse_port = false;
    socket_config.connect_remote = true;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, socket, std::move(socket_config), state,
                                 &runtime_udp_noop_datagram));

    const af::net::udp_socket_handle handle = state.handle;
    EXPECT_EQ(handle.send(af::buffer::copy("pre", 3)), af::net::udp_send_result::queued);

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, socket, state));
    EXPECT_EQ(handle.send(af::buffer::copy("x", 1)), af::net::udp_send_result::closed);
    runtime.stop();
}

TEST(NetUdpSocketTests, OldHandleReportsClosedAfterSocketRestart) {
    const std::uint16_t port = reserve_udp_loopback_port();

    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket socket(runtime);
    af::net::udp_socket_config first_config;
    first_config.name = "udp-old-handle-before-restart";
    first_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    first_config.remote_endpoint = af::net::udp_endpoint::loopback_v4(port);
    first_config.threads = {io_thread};
    first_config.options.reuse_port = false;
    first_config.connect_remote = true;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, socket, std::move(first_config), state,
                                 &runtime_udp_noop_datagram));

    const af::net::udp_socket_handle old_handle = state.handle;
    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, socket, state));

    af::net::udp_socket_config second_config;
    second_config.name = "udp-old-handle-after-restart";
    second_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    second_config.remote_endpoint = af::net::udp_endpoint::loopback_v4(port);
    second_config.threads = {io_thread};
    second_config.options.reuse_port = false;
    second_config.connect_remote = true;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, socket, std::move(second_config), state,
                                 &runtime_udp_noop_datagram));
    const af::net::udp_socket_handle current_handle = state.handle;

    EXPECT_EQ(old_handle.send(af::buffer::copy("old", 3)), af::net::udp_send_result::closed);
    EXPECT_EQ(
        old_handle.send_to(af::buffer::copy("old", 3), af::net::udp_endpoint::loopback_v4(port)),
        af::net::udp_send_result::closed);
    EXPECT_EQ(current_handle.send(af::buffer::copy("new", 3)), af::net::udp_send_result::queued);

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, socket, state));
    runtime.stop();
}

TEST(NetUdpSocketTests, StoppedSocketHandleReportsClosedOnOwnerThread) {
    const std::uint16_t port = reserve_udp_loopback_port();
    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket socket(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-stopped-owner-handle";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    socket_config.remote_endpoint = af::net::udp_endpoint::loopback_v4(port);
    socket_config.threads = {io_thread};
    socket_config.options.reuse_port = false;
    socket_config.connect_remote = true;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, socket, std::move(socket_config), state,
                                 &runtime_udp_noop_datagram));

    const af::net::udp_socket_handle handle = state.handle;
    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, socket, state));

    std::atomic<int> send_result{-1};
    std::atomic<int> send_to_result{-1};
    std::atomic<bool> done{false};
    ASSERT_TRUE(runtime.post(io_thread, [handle, port, &send_result, &send_to_result, &done] {
        const af::net::udp_send_result send = handle.send(af::buffer::copy("x", 1));
        const af::net::udp_send_result send_to =
            handle.send_to(af::buffer::copy("x", 1), af::net::udp_endpoint::loopback_v4(port));
        send_result.store(static_cast<int>(send), std::memory_order_release);
        send_to_result.store(static_cast<int>(send_to), std::memory_order_release);
        done.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::udp_send_result::closed));
    EXPECT_EQ(send_to_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::udp_send_result::closed));

    runtime.stop();
}

TEST(NetUdpSocketTests, RejectsStartWhileAlreadyRunning) {
    const std::uint16_t port = reserve_udp_loopback_port();

    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket socket(runtime);
    af::net::udp_socket_config first_config;
    first_config.name = "udp-running-start";
    first_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    first_config.remote_endpoint = af::net::udp_endpoint::loopback_v4(port);
    first_config.threads = {io_thread};
    first_config.options.reuse_port = false;
    first_config.connect_remote = true;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, socket, std::move(first_config), state,
                                 &runtime_udp_noop_datagram));

    af::net::udp_socket_config second_config;
    second_config.name = "udp-running-start-again";
    second_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    second_config.remote_endpoint = af::net::udp_endpoint::loopback_v4(port);
    second_config.threads = {io_thread};
    second_config.options.reuse_port = false;
    second_config.connect_remote = true;
    ASSERT_TRUE(try_start_udp_socket(runtime, io_thread, socket, std::move(second_config), state,
                                     &runtime_udp_noop_datagram));
    EXPECT_FALSE(state.start_ok.load(std::memory_order_acquire));

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, socket, state));
    runtime.stop();
}

TEST(NetUdpSocketTests, RejectsUnixEndpointAcrossMultipleShards) {
    af::runtime runtime(make_udp_runtime_config(2));
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 2; }));

    const af::thread_ref io_thread0 = runtime.select_thread_ref(af::thread_selector::io(0));
    const af::thread_ref io_thread1 = runtime.select_thread_ref(af::thread_selector::io(1));
    af::net::udp_socket socket(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-unix-multi-shard-rejected";
    socket_config.local_endpoint =
        af::net::unix_endpoint::unix_path("/tmp/af-udp-multi-shard.sock");
    socket_config.threads = {io_thread0, io_thread1};
    socket_config.options.reuse_port = false;
    ASSERT_TRUE(try_start_udp_socket(runtime, io_thread0, socket, std::move(socket_config), state,
                                     &runtime_udp_noop_datagram));
    EXPECT_FALSE(state.start_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetUdpSocketTests, IpSocketRejectsUnixPeerEndpoint) {
    af::runtime runtime(make_udp_runtime_config());
    RuntimeUdpState state;
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::udp_socket socket(runtime);
    af::net::udp_socket_config socket_config;
    socket_config.name = "udp-ip-socket";
    socket_config.local_endpoint = af::net::udp_endpoint::loopback_v4(0);
    socket_config.threads = {io_thread};
    socket_config.options.reuse_port = false;
    ASSERT_TRUE(start_udp_socket(runtime, io_thread, socket, std::move(socket_config), state,
                                 &runtime_udp_noop_datagram));

    EXPECT_EQ(state.handle.send_to(af::buffer::copy("x", 1),
                                   af::net::unix_endpoint::unix_path("/tmp/af-udp-peer.sock")),
              af::net::udp_send_result::unsupported);

    EXPECT_TRUE(stop_udp_socket(runtime, io_thread, socket, state));
    runtime.stop();
}
