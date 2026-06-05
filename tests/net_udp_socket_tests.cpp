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
#include <utility>
#include <vector>

#include "af/async_runtime.hpp"
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

struct NetUdpIoTag;
struct NetUdpTwoIoTag;

struct NetUdpRuntimeTraits {
    static constexpr auto threads =
        af::thread_layout(af::thread_group<NetUdpIoTag, 1, af::thread_kind::io>("net-udp-io"));
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using NetUdpRuntime = af::AsyncRuntime<NetUdpRuntimeTraits>;

struct NetUdpTwoIoRuntimeTraits {
    static constexpr auto threads =
        af::thread_layout(af::thread_group<NetUdpTwoIoTag, 2, af::thread_kind::io>("net-udp-io"));
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using NetUdpTwoIoRuntime = af::AsyncRuntime<NetUdpTwoIoRuntimeTraits>;

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

struct UdpEchoHandler {
    void on_datagram(af::net::UdpSocketRef<NetUdpRuntime> socket, af::BufferView bytes,
                     const af::net::UdpEndpoint &peer) noexcept {
        static_cast<void>(socket.send_to(bytes, peer));
    }
};

struct UdpPeerEchoState {
    std::atomic<int> native_family{0};
    std::atomic<std::uint16_t> peer_port{0};
    std::atomic<bool> endpoint_error{false};
};

struct UdpPeerEchoHandler {
    UdpPeerEchoState *state{nullptr};

    void on_datagram(af::net::UdpSocketRef<NetUdpRuntime> socket, af::BufferView bytes,
                     const af::net::UdpPeer &peer) noexcept {
        if (state != nullptr) {
            try {
                const af::net::UdpEndpoint endpoint = peer.endpoint();
                state->native_family.store(peer.native_family(), std::memory_order_release);
                state->peer_port.store(endpoint.port, std::memory_order_release);
            } catch (...) {
                state->endpoint_error.store(true, std::memory_order_release);
            }
        }
        static_cast<void>(socket.send_to(bytes, peer));
    }
};

struct UdpOwnerHandleEchoState {
    std::atomic<int> send_result{-1};
};

struct UdpOwnerHandleEchoHandler {
    UdpOwnerHandleEchoState *state{nullptr};

    void on_datagram(af::net::UdpSocketRef<NetUdpRuntime> socket, af::BufferView bytes,
                     const af::net::UdpPeer &peer) noexcept {
        const af::net::UdpSendResult result = socket.handle().send_to(bytes, peer);
        if (state != nullptr) {
            state->send_result.store(static_cast<int>(result), std::memory_order_release);
        }
    }
};

struct UdpCaptureState {
    std::array<char, 64> data{};
    std::atomic<std::size_t> size{0};
};

struct UdpErrorState {
    std::atomic<int> error{0};
    std::atomic<std::size_t> datagrams{0};
};

struct UdpStoppedOwnerHandleState {
    std::atomic<int> send_result{-1};
    std::atomic<int> send_to_result{-1};
    std::atomic<bool> done{false};
};

struct UdpCaptureHandler {
    UdpCaptureState *state{nullptr};

    void on_datagram(af::net::UdpSocketRef<NetUdpRuntime>, af::BufferView bytes,
                     const af::net::UdpEndpoint &) noexcept {
        if (state == nullptr) {
            return;
        }
        const std::size_t size = std::min(bytes.size(), state->data.size());
        std::memcpy(state->data.data(), bytes.data(), size);
        state->size.store(size, std::memory_order_release);
    }
};

struct UdpNoopHandler {
    template <typename Runtime>
    void on_datagram(af::net::UdpSocketRef<Runtime>, af::BufferView,
                     const af::net::UdpEndpoint &) noexcept {}

    template <typename Runtime> void on_error(af::net::UdpSocketHandle<Runtime>, int) noexcept {}
};

struct UdpOversizeHandler {
    UdpErrorState *state{nullptr};

    void on_datagram(af::net::UdpSocketRef<NetUdpRuntime> socket, af::BufferView bytes,
                     const af::net::UdpEndpoint &peer) noexcept {
        if (state != nullptr) {
            state->datagrams.fetch_add(1U, std::memory_order_release);
        }
        static_cast<void>(socket.send_to(bytes, peer));
    }

    void on_error(af::net::UdpSocketHandle<NetUdpRuntime>, int error) noexcept {
        if (state != nullptr) {
            state->error.store(error, std::memory_order_release);
        }
    }
};

class UdpStoppedOwnerHandleTask final : public NetUdpRuntime::Task {
public:
    explicit UdpStoppedOwnerHandleTask(NetUdpRuntime::Task::FactoryToken token)
        : NetUdpRuntime::Task(token) {}

    bool do_it(af::net::UdpSocketHandle<NetUdpRuntime> handle,
               std::shared_ptr<UdpStoppedOwnerHandleState> state, std::uint16_t peer_port) {
        handle_ = std::move(handle);
        state_ = std::move(state);
        peer_port_ = peer_port;
        return schedule(NetUdpRuntime::thread_group<NetUdpIoTag>().template at<0>());
    }

private:
    af::TaskResult run() override {
        if (state_ != nullptr) {
            const af::net::UdpSendResult send_result = handle_.send(af::Buffer::copy("x", 1));
            const af::net::UdpSendResult send_to_result = handle_.send_to(
                af::Buffer::copy("x", 1), af::net::UdpEndpoint::loopback(peer_port_));
            state_->send_result.store(static_cast<int>(send_result), std::memory_order_release);
            state_->send_to_result.store(static_cast<int>(send_to_result),
                                         std::memory_order_release);
            state_->done.store(true, std::memory_order_release);
        }
        return done();
    }

    af::net::UdpSocketHandle<NetUdpRuntime> handle_;
    std::shared_ptr<UdpStoppedOwnerHandleState> state_;
    std::uint16_t peer_port_{0};
};

struct RuntimeUdpState {
    std::atomic<bool> started{false};
    std::atomic<bool> start_ok{false};
    std::atomic<bool> stopped{false};
    std::atomic<bool> stop_ok{false};
    std::atomic<int> datagrams{0};
    std::atomic<int> errors{0};
    std::atomic<int> last_error{0};
    std::atomic<int> owner_send_result{-1};
    std::atomic<std::uint16_t> port{0};
    std::array<char, 64> data{};
    std::atomic<std::size_t> size{0};
    af::net::udp_socket_handle handle;
};

void runtime_udp_echo_datagram(void *owner, af::net::udp_socket_ref socket, af::BufferView bytes,
                               const af::net::udp_peer &peer) noexcept {
    auto *state = static_cast<RuntimeUdpState *>(owner);
    if (state != nullptr) {
        state->datagrams.fetch_add(1, std::memory_order_release);
    }
    static_cast<void>(socket.send_to(bytes, peer));
}

void runtime_udp_capture_datagram(void *owner, af::net::udp_socket_ref socket, af::BufferView bytes,
                                  const af::net::udp_peer &peer) noexcept {
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
                                   af::BufferView bytes, const af::net::udp_peer &peer) noexcept {
    auto *state = static_cast<RuntimeUdpState *>(owner);
    const af::net::udp_send_result result = socket.handle().send_to(bytes, peer);
    if (state != nullptr) {
        state->owner_send_result.store(static_cast<int>(result), std::memory_order_release);
        state->datagrams.fetch_add(1, std::memory_order_release);
    }
}

void runtime_udp_error(void *owner, af::net::udp_socket_handle socket, int error) noexcept {
    static_cast<void>(socket);
    auto *state = static_cast<RuntimeUdpState *>(owner);
    if (state != nullptr) {
        state->last_error.store(error, std::memory_order_release);
        state->errors.fetch_add(1, std::memory_order_release);
    }
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
    ASSERT_TRUE(wait_until([&] {
        return state.owner_send_result.load(std::memory_order_acquire) != -1;
    }));
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

    EXPECT_EQ(client_state.handle.send(af::Buffer::copy("ping", 4)),
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
    EXPECT_EQ(client_state.handle.send(af::Buffer::copy("after", 5)),
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

    EXPECT_EQ(handles[0].send(af::Buffer::copy("one", 3)), af::net::udp_send_result::queued);
    EXPECT_EQ(handles[1].send(af::Buffer::copy("two", 3)), af::net::udp_send_result::queued);

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
    EXPECT_EQ(handles[0].send(af::Buffer::copy("after", 5)), af::net::udp_send_result::closed);

    runtime.stop();
}

TEST(NetUdpSocketTests, EchoesDatagramToRawClient) {
    const std::uint16_t port = reserve_udp_loopback_port();

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> server;
    server.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(server.start<UdpEchoHandler>({
        .name = "udp-echo",
        .local_endpoint = af::net::UdpEndpoint::loopback(port),
    }));
    NetUdpRuntime::wait_for_idle();

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

    EXPECT_TRUE(server.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, EchoesDatagramUsingCachedPeerAddress) {
    const std::uint16_t port = reserve_udp_loopback_port();
    UdpPeerEchoState state;

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> server;
    server.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(server.start<UdpPeerEchoHandler>(
        {
            .name = "udp-peer-echo",
            .local_endpoint = af::net::UdpEndpoint::loopback(port),
        },
        UdpPeerEchoHandler{&state}));
    NetUdpRuntime::wait_for_idle();

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
    EXPECT_EQ(state.native_family.load(std::memory_order_acquire), AF_INET);
    EXPECT_EQ(state.peer_port.load(std::memory_order_acquire), client_port);

    EXPECT_TRUE(server.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, OwnerHandleSendsBufferViewWithoutQueueing) {
    const std::uint16_t port = reserve_udp_loopback_port();
    UdpOwnerHandleEchoState state;

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> server;
    server.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(server.start<UdpOwnerHandleEchoHandler>(
        {
            .name = "udp-owner-handle-echo",
            .local_endpoint = af::net::UdpEndpoint::loopback(port),
        },
        UdpOwnerHandleEchoHandler{&state}));
    NetUdpRuntime::wait_for_idle();

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
    EXPECT_EQ(state.send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::UdpSendResult::Accepted));

    EXPECT_TRUE(server.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, EchoesIpv6DatagramToRawClient) {
    std::uint16_t port = 0;
    if (!reserve_udp_loopback_v6_port(port)) {
        GTEST_SKIP() << "IPv6 loopback is not available";
    }

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> server;
    server.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(server.start<UdpEchoHandler>({
        .name = "udp-v6-echo",
        .local_endpoint = af::net::UdpEndpoint::loopback_v6(port),
        .options = {.reuse_port = false},
    }));
    NetUdpRuntime::wait_for_idle();

    UniqueFd client(::socket(AF_INET6, SOCK_DGRAM, 0));
    if (client.get() < 0) {
        static_cast<void>(server.stop());
        NetUdpRuntime::wait_for_idle();
        NetUdpRuntime::shutdown();
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

    EXPECT_TRUE(server.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, ReceiveBufferGrowsToMaxDatagramSize) {
    const std::uint16_t port = reserve_udp_loopback_port();

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> server;
    server.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(server.start<UdpEchoHandler>({
        .name = "udp-small-buffer-large-datagram",
        .local_endpoint = af::net::UdpEndpoint::loopback(port),
        .options = {.receive_buffer_size = 4, .max_datagram_size = 8},
    }));
    NetUdpRuntime::wait_for_idle();

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

    EXPECT_TRUE(server.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, OversizedDatagramReportsMessageSizeAndDropsPacket) {
    const std::uint16_t port = reserve_udp_loopback_port();
    UdpErrorState state;

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> server;
    server.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(server.start<UdpOversizeHandler>(
        {
            .name = "udp-oversize-drop",
            .local_endpoint = af::net::UdpEndpoint::loopback(port),
            .options = {.receive_buffer_size = 8, .max_datagram_size = 4},
        },
        UdpOversizeHandler{&state}));
    NetUdpRuntime::wait_for_idle();

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
        wait_until([&] { return state.error.load(std::memory_order_acquire) == EMSGSIZE; }));
    EXPECT_EQ(state.datagrams.load(std::memory_order_acquire), 0U);

    char buffer[32]{};
    EXPECT_LT(::recv(client.get(), buffer, sizeof(buffer), 0), 0);

    EXPECT_TRUE(server.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
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

    NetUdpTwoIoRuntime::init();
    af::net::UdpSocket<NetUdpTwoIoRuntime> client;
    client.bind_threads(NetUdpTwoIoRuntime::thread_group<NetUdpTwoIoTag>());
    ASSERT_TRUE(client.start<UdpNoopHandler>({
        .name = "udp-two-shard-client",
        .local_endpoint = af::net::UdpEndpoint::loopback(0),
        .remote_endpoint = af::net::UdpEndpoint::loopback(port),
        .connect_remote = true,
    }));
    NetUdpTwoIoRuntime::wait_for_idle();

    const std::vector<af::net::UdpSocketHandle<NetUdpTwoIoRuntime>> handles = client.handles();
    ASSERT_EQ(handles.size(), 2U);
    EXPECT_NE(handles[0].shard_index(), handles[1].shard_index());

    const af::net::UdpSocketHandle<NetUdpTwoIoRuntime> first = client.handle();
    const af::net::UdpSocketHandle<NetUdpTwoIoRuntime> second = client.handle();
    EXPECT_NE(first.shard_index(), second.shard_index());
    EXPECT_EQ(client.handle_for_shard(first.shard_index()).shard_index(), first.shard_index());

    EXPECT_EQ(first.send(af::Buffer::copy("one", 3)), af::net::UdpSendResult::Queued);
    EXPECT_EQ(second.send(af::Buffer::copy("two", 3)), af::net::UdpSendResult::Queued);

    std::array<char, 8> buffer{};
    std::vector<std::string> received;
    for (int i = 0; i < 2; ++i) {
        const ssize_t n = ::recv(sink.get(), buffer.data(), buffer.size(), 0);
        ASSERT_GT(n, 0);
        received.emplace_back(buffer.data(), static_cast<std::size_t>(n));
    }
    std::sort(received.begin(), received.end());
    EXPECT_EQ(received, (std::vector<std::string>{"one", "two"}));

    EXPECT_TRUE(client.stop());
    EXPECT_TRUE(client.handles().empty());
    EXPECT_EQ(client.handle().send(af::Buffer::copy("stopped", 7)), af::net::UdpSendResult::Closed);
    EXPECT_EQ(client.handle_for_shard(first.shard_index()).send(af::Buffer::copy("stopped", 7)),
              af::net::UdpSendResult::Closed);
    NetUdpTwoIoRuntime::wait_for_idle();
    NetUdpTwoIoRuntime::shutdown();
}

TEST(NetUdpSocketTests, ConnectedClientSendsThroughRuntimeTask) {
    const std::uint16_t port = reserve_udp_loopback_port();
    UdpCaptureState state;

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> server;
    server.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(server.start<UdpEchoHandler>({
        .name = "udp-echo-server",
        .local_endpoint = af::net::UdpEndpoint::loopback(port),
    }));
    NetUdpRuntime::wait_for_idle();

    af::net::UdpSocket<NetUdpRuntime> client;
    client.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(client.start<UdpCaptureHandler>(
        {
            .name = "udp-connected-client",
            .local_endpoint = af::net::UdpEndpoint::loopback(0),
            .remote_endpoint = af::net::UdpEndpoint::loopback(port),
            .connect_remote = true,
        },
        UdpCaptureHandler{&state}));
    NetUdpRuntime::wait_for_idle();

    EXPECT_EQ(client.handle().send(af::Buffer::copy("ping", 4)), af::net::UdpSendResult::Queued);
    ASSERT_TRUE(wait_until([&] { return state.size.load(std::memory_order_acquire) == 4U; }));
    EXPECT_EQ(std::string(state.data.data(), state.size.load(std::memory_order_acquire)), "ping");

    EXPECT_TRUE(client.stop());
    EXPECT_TRUE(server.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, ConnectedClientAcceptsInferredIpv4RemoteFamily) {
    const std::uint16_t port = reserve_udp_loopback_port();
    UdpCaptureState state;

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> server;
    server.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(server.start<UdpEchoHandler>({
        .name = "udp-inferred-family-echo-server",
        .local_endpoint = af::net::UdpEndpoint::loopback(port),
    }));
    NetUdpRuntime::wait_for_idle();

    af::net::UdpSocket<NetUdpRuntime> client;
    client.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(client.start<UdpCaptureHandler>(
        {
            .name = "udp-inferred-family-client",
            .local_endpoint = af::net::UdpEndpoint::loopback(0),
            .remote_endpoint = af::net::UdpEndpoint::host("127.0.0.1", port),
            .connect_remote = true,
        },
        UdpCaptureHandler{&state}));
    NetUdpRuntime::wait_for_idle();

    EXPECT_EQ(client.handle().send(af::Buffer::copy("ping", 4)), af::net::UdpSendResult::Queued);
    ASSERT_TRUE(wait_until([&] { return state.size.load(std::memory_order_acquire) == 4U; }));
    EXPECT_EQ(std::string(state.data.data(), state.size.load(std::memory_order_acquire)), "ping");

    EXPECT_TRUE(client.stop());
    EXPECT_TRUE(server.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, RejectsConnectedRemoteAddressFamilyMismatchSynchronously) {
    NetUdpRuntime::init();

    af::net::UdpSocket<NetUdpRuntime> socket;
    socket.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    EXPECT_FALSE(socket.start<UdpNoopHandler>({
        .name = "udp-remote-family-mismatch",
        .local_endpoint = af::net::UdpEndpoint::loopback(0),
        .remote_endpoint =
            af::net::UdpEndpoint::host("::1", 12345, af::net::AddressFamily::Unspecified),
        .connect_remote = true,
    }));

    EXPECT_TRUE(socket.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, RejectsConnectedIpRemotePortZeroSynchronously) {
    NetUdpRuntime::init();

    af::net::UdpSocket<NetUdpRuntime> socket;
    socket.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    EXPECT_FALSE(socket.start<UdpNoopHandler>({
        .name = "udp-remote-zero-port",
        .local_endpoint = af::net::UdpEndpoint::loopback(0),
        .remote_endpoint = af::net::UdpEndpoint::loopback(0),
        .connect_remote = true,
    }));

    EXPECT_TRUE(socket.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, StoppedSocketHandlesReportClosedForSends) {
    const std::uint16_t port = reserve_udp_loopback_port();

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> socket;
    socket.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(socket.start<UdpNoopHandler>({
        .name = "udp-stopped-handle",
        .local_endpoint = af::net::UdpEndpoint::loopback(0),
        .remote_endpoint = af::net::UdpEndpoint::loopback(port),
        .connect_remote = true,
    }));
    NetUdpRuntime::wait_for_idle();

    const af::net::UdpSocketHandle<NetUdpRuntime> handle = socket.handle();
    EXPECT_TRUE(socket.stop());

    const std::string_view payload = "x";
    EXPECT_EQ(handle.send(af::Buffer::copy("x", 1)), af::net::UdpSendResult::Closed);
    EXPECT_EQ(handle.send(af::BufferView(payload.data(), payload.size())),
              af::net::UdpSendResult::Closed);
    EXPECT_EQ(handle.send_to(af::Buffer::copy("x", 1), af::net::UdpEndpoint::loopback(port)),
              af::net::UdpSendResult::Closed);
    EXPECT_EQ(handle.send_to(af::BufferView(payload.data(), payload.size()),
                             af::net::UdpEndpoint::loopback(port)),
              af::net::UdpSendResult::Closed);

    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, StopPublishesClosedStateBeforeOwnerStops) {
    const std::uint16_t port = reserve_udp_loopback_port();

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> socket;
    socket.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(socket.start<UdpNoopHandler>({
        .name = "udp-stop-in-progress",
        .local_endpoint = af::net::UdpEndpoint::loopback(0),
        .remote_endpoint = af::net::UdpEndpoint::loopback(port),
        .connect_remote = true,
    }));
    NetUdpRuntime::wait_for_idle();

    const af::net::UdpSocketHandle<NetUdpRuntime> handle = socket.handle();
    EXPECT_EQ(handle.send(af::Buffer::copy("pre", 3)), af::net::UdpSendResult::Queued);

    EXPECT_TRUE(socket.stop());
    EXPECT_EQ(handle.send(af::Buffer::copy("x", 1)), af::net::UdpSendResult::Closed);
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, OldHandleReportsClosedAfterSocketRestart) {
    const std::uint16_t port = reserve_udp_loopback_port();

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> socket;
    socket.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(socket.start<UdpNoopHandler>({
        .name = "udp-old-handle-before-restart",
        .local_endpoint = af::net::UdpEndpoint::loopback(0),
        .remote_endpoint = af::net::UdpEndpoint::loopback(port),
        .connect_remote = true,
    }));
    NetUdpRuntime::wait_for_idle();

    const af::net::UdpSocketHandle<NetUdpRuntime> old_handle = socket.handle();
    EXPECT_TRUE(socket.stop());
    NetUdpRuntime::wait_for_idle();

    ASSERT_TRUE(socket.start<UdpNoopHandler>({
        .name = "udp-old-handle-after-restart",
        .local_endpoint = af::net::UdpEndpoint::loopback(0),
        .remote_endpoint = af::net::UdpEndpoint::loopback(port),
        .connect_remote = true,
    }));
    NetUdpRuntime::wait_for_idle();
    const af::net::UdpSocketHandle<NetUdpRuntime> current_handle = socket.handle();

    EXPECT_EQ(old_handle.send(af::Buffer::copy("old", 3)), af::net::UdpSendResult::Closed);
    EXPECT_EQ(old_handle.send_to(af::Buffer::copy("old", 3), af::net::UdpEndpoint::loopback(port)),
              af::net::UdpSendResult::Closed);
    EXPECT_EQ(current_handle.send(af::Buffer::copy("new", 3)), af::net::UdpSendResult::Queued);

    EXPECT_TRUE(socket.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, StoppedSocketHandleReportsClosedOnOwnerThread) {
    const std::uint16_t port = reserve_udp_loopback_port();
    auto state = std::make_shared<UdpStoppedOwnerHandleState>();

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> socket;
    socket.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(socket.start<UdpNoopHandler>({
        .name = "udp-stopped-owner-handle",
        .local_endpoint = af::net::UdpEndpoint::loopback(0),
        .remote_endpoint = af::net::UdpEndpoint::loopback(port),
        .connect_remote = true,
    }));
    NetUdpRuntime::wait_for_idle();

    const af::net::UdpSocketHandle<NetUdpRuntime> handle = socket.handle();
    EXPECT_TRUE(socket.stop());
    ASSERT_TRUE(NetUdpRuntime::start_task<UdpStoppedOwnerHandleTask>(handle, state, port));

    ASSERT_TRUE(wait_until([&] { return state->done.load(std::memory_order_acquire); }));
    EXPECT_EQ(state->send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::UdpSendResult::Closed));
    EXPECT_EQ(state->send_to_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::UdpSendResult::Closed));

    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, RejectsStartWhileAlreadyRunning) {
    const std::uint16_t port = reserve_udp_loopback_port();

    NetUdpRuntime::init();
    af::net::UdpSocket<NetUdpRuntime> socket;
    socket.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(socket.start<UdpNoopHandler>({
        .name = "udp-running-start",
        .local_endpoint = af::net::UdpEndpoint::loopback(0),
        .remote_endpoint = af::net::UdpEndpoint::loopback(port),
        .connect_remote = true,
    }));

    EXPECT_FALSE(socket.start<UdpNoopHandler>({
        .name = "udp-running-start-again",
        .local_endpoint = af::net::UdpEndpoint::loopback(0),
        .remote_endpoint = af::net::UdpEndpoint::loopback(port),
        .connect_remote = true,
    }));

    EXPECT_TRUE(socket.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}

TEST(NetUdpSocketTests, RejectsUnixEndpointAcrossMultipleShards) {
    NetUdpTwoIoRuntime::init();

    af::net::UdpSocket<NetUdpTwoIoRuntime> socket;
    socket.bind_threads(NetUdpTwoIoRuntime::thread_group<NetUdpTwoIoTag>());
    EXPECT_FALSE(socket.start<UdpNoopHandler>({
        .name = "udp-unix-multi-shard-rejected",
        .local_endpoint = af::net::UnixEndpoint::unix_path("/tmp/af-udp-multi-shard.sock"),
        .options = {.reuse_port = false},
    }));

    NetUdpTwoIoRuntime::wait_for_idle();
    NetUdpTwoIoRuntime::shutdown();
}

TEST(NetUdpSocketTests, IpSocketRejectsUnixPeerEndpoint) {
    NetUdpRuntime::init();

    af::net::UdpSocket<NetUdpRuntime> socket;
    socket.bind_threads(NetUdpRuntime::thread_group<NetUdpIoTag>());
    ASSERT_TRUE(socket.start<UdpNoopHandler>({
        .name = "udp-ip-socket",
        .local_endpoint = af::net::UdpEndpoint::loopback(0),
    }));
    NetUdpRuntime::wait_for_idle();

    EXPECT_EQ(socket.handle().send_to(af::Buffer::copy("x", 1),
                                      af::net::UnixEndpoint::unix_path("/tmp/af-udp-peer.sock")),
              af::net::UdpSendResult::Unsupported);

    EXPECT_TRUE(socket.stop());
    NetUdpRuntime::wait_for_idle();
    NetUdpRuntime::shutdown();
}
