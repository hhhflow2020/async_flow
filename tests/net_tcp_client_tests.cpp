#include <array>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>
#include <utility>

#include "af/net.hpp"
#include "af/platform.hpp"
#include "af/runtime.hpp"

#include "gtest/gtest.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
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

[[nodiscard]] std::uint16_t reserve_loopback_port() {
    UniqueFd fd(::socket(AF_INET, SOCK_STREAM, 0));
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

[[nodiscard]] bool reserve_loopback_v6_port(std::uint16_t &port) {
    UniqueFd fd(::socket(AF_INET6, SOCK_STREAM, 0));
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

struct RuntimeTcpClientServerState {
    std::atomic<bool> started{false};
    std::atomic<bool> start_ok{false};
    std::atomic<bool> stopped{false};
    std::atomic<bool> stop_ok{false};
    std::atomic<int> accepted{0};
    std::atomic<int> reads{0};
    std::atomic<int> closes{0};
    std::atomic<std::uint16_t> port{0};
};

struct RuntimeTcpClientState {
    bool send_on_connect{false};
    std::atomic<bool> connect_started{false};
    std::atomic<bool> connect_ok{false};
    std::atomic<bool> connected{false};
    std::atomic<bool> stopped{false};
    std::atomic<bool> stop_ok{false};
    std::atomic<int> errors{0};
    std::atomic<int> last_error{0};
    std::atomic<int> closes{0};
    std::atomic<int> connection_count_after_close{-1};
    std::atomic<bool> connection_count_checked{false};
    std::array<char, 64> data{};
    std::atomic<std::size_t> size{0};
    af::net::tcp_connection_handle handle;
};

void runtime_tcp_client_server_accept(void *owner, af::net::tcp_connection_ref conn) noexcept {
    auto *state = static_cast<RuntimeTcpClientServerState *>(owner);
    if (state != nullptr && conn.valid()) {
        state->accepted.fetch_add(1, std::memory_order_release);
    }
}

void runtime_tcp_client_server_read(void *owner, af::net::tcp_connection_ref conn,
                                    af::buffer_view bytes) noexcept {
    auto *state = static_cast<RuntimeTcpClientServerState *>(owner);
    if (state != nullptr) {
        state->reads.fetch_add(1, std::memory_order_release);
    }
    static_cast<void>(conn.send(bytes));
}

void runtime_tcp_client_server_close(void *owner, af::net::tcp_connection_ref conn,
                                     af::net::close_reason reason) noexcept {
    static_cast<void>(conn);
    static_cast<void>(reason);
    auto *state = static_cast<RuntimeTcpClientServerState *>(owner);
    if (state != nullptr) {
        state->closes.fetch_add(1, std::memory_order_release);
    }
}

void runtime_tcp_client_connect(void *owner, af::net::tcp_connection_ref conn) noexcept {
    auto *state = static_cast<RuntimeTcpClientState *>(owner);
    if (state == nullptr || !conn.valid()) {
        return;
    }
    state->handle = conn.handle();
    state->connected.store(true, std::memory_order_release);
    if (state->send_on_connect) {
        static_cast<void>(conn.send(af::buffer::copy("hello", 5)));
    }
}

void runtime_tcp_client_read(void *owner, af::net::tcp_connection_ref conn,
                             af::buffer_view bytes) noexcept {
    static_cast<void>(conn);
    auto *state = static_cast<RuntimeTcpClientState *>(owner);
    if (state == nullptr) {
        return;
    }
    const std::size_t size = std::min(bytes.size(), state->data.size());
    std::memcpy(state->data.data(), bytes.data(), size);
    state->size.store(size, std::memory_order_release);
}

void runtime_tcp_client_close(void *owner, af::net::tcp_connection_ref conn,
                              af::net::close_reason reason) noexcept {
    static_cast<void>(conn);
    static_cast<void>(reason);
    auto *state = static_cast<RuntimeTcpClientState *>(owner);
    if (state != nullptr) {
        state->closes.fetch_add(1, std::memory_order_release);
    }
}

void runtime_tcp_client_error(void *owner, int error) noexcept {
    auto *state = static_cast<RuntimeTcpClientState *>(owner);
    if (state != nullptr) {
        state->last_error.store(error, std::memory_order_release);
        state->errors.fetch_add(1, std::memory_order_release);
    }
}

[[nodiscard]] af::runtime_config make_tcp_client_runtime_config(std::string name = "net-rt-io") {
    af::runtime_config config;
    config.threads = {af::io_threads(std::move(name), 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);
    return config;
}

void reset_server_start_state(RuntimeTcpClientServerState &state) noexcept {
    state.started.store(false, std::memory_order_release);
    state.start_ok.store(false, std::memory_order_release);
    state.stopped.store(false, std::memory_order_release);
    state.stop_ok.store(false, std::memory_order_release);
    state.accepted.store(0, std::memory_order_release);
    state.reads.store(0, std::memory_order_release);
    state.closes.store(0, std::memory_order_release);
    state.port.store(0, std::memory_order_release);
}

void reset_client_connect_state(RuntimeTcpClientState &state) noexcept {
    state.connect_started.store(false, std::memory_order_release);
    state.connect_ok.store(false, std::memory_order_release);
    state.connected.store(false, std::memory_order_release);
    state.stopped.store(false, std::memory_order_release);
    state.stop_ok.store(false, std::memory_order_release);
    state.errors.store(0, std::memory_order_release);
    state.last_error.store(0, std::memory_order_release);
    state.closes.store(0, std::memory_order_release);
    state.connection_count_after_close.store(-1, std::memory_order_release);
    state.connection_count_checked.store(false, std::memory_order_release);
    state.size.store(0, std::memory_order_release);
    state.handle = {};
}

[[nodiscard]] bool start_runtime_tcp_echo_server(af::runtime &runtime, af::thread_ref io_thread,
                                                 af::net::tcp_server &server,
                                                 RuntimeTcpClientServerState &state,
                                                 af::net::tcp_endpoint endpoint, std::string name) {
    reset_server_start_state(state);
    if (!runtime.post(io_thread, [&server, &state, endpoint = std::move(endpoint),
                                  name = std::move(name), io_thread]() mutable {
            af::net::tcp_connection_callbacks callbacks;
            callbacks.owner = &state;
            callbacks.on_accept = &runtime_tcp_client_server_accept;
            callbacks.on_read = &runtime_tcp_client_server_read;
            callbacks.on_close = &runtime_tcp_client_server_close;

            af::net::tcp_listener_config listener_config;
            listener_config.name = std::move(name);
            listener_config.endpoint = std::move(endpoint);
            listener_config.threads = {io_thread};
            listener_config.options.reuse_port = false;

            const af::net::listener_result listener =
                server.add_listener(std::move(listener_config), callbacks);
            bool ok = listener.ok();
            if (ok) {
                ok = server.start();
            }
            if (ok) {
                const af::net::tcp_endpoint *local_endpoint =
                    server.local_endpoint(listener.listener);
                ok = local_endpoint != nullptr;
                if (ok) {
                    state.port.store(local_endpoint->port, std::memory_order_release);
                }
            }
            state.start_ok.store(ok, std::memory_order_release);
            state.started.store(true, std::memory_order_release);
        })) {
        return false;
    }
    return wait_until([&] { return state.started.load(std::memory_order_acquire); }) &&
           state.start_ok.load(std::memory_order_acquire);
}

[[nodiscard]] bool connect_runtime_tcp_client(
    af::runtime &runtime, af::thread_ref io_thread, af::net::tcp_client &client,
    RuntimeTcpClientState &state, af::net::tcp_endpoint remote_endpoint, std::string name,
    af::net::tcp_endpoint local_endpoint = af::net::tcp_endpoint::any(0), bool bind_local = false,
    std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
    reset_client_connect_state(state);
    if (!runtime.post(io_thread, [&client, &state, remote_endpoint = std::move(remote_endpoint),
                                  local_endpoint = std::move(local_endpoint),
                                  name = std::move(name), io_thread, bind_local,
                                  timeout]() mutable {
            af::net::tcp_client_callbacks callbacks;
            callbacks.owner = &state;
            callbacks.on_connect = &runtime_tcp_client_connect;
            callbacks.on_read = &runtime_tcp_client_read;
            callbacks.on_close = &runtime_tcp_client_close;
            callbacks.on_error = &runtime_tcp_client_error;

            af::net::tcp_client_connect_config connect_config;
            connect_config.name = std::move(name);
            connect_config.remote_endpoint = std::move(remote_endpoint);
            connect_config.local_endpoint = std::move(local_endpoint);
            connect_config.bind_local = bind_local;
            connect_config.owner_thread = io_thread;
            connect_config.connect_timeout = timeout;

            state.connect_ok.store(client.connect(std::move(connect_config), callbacks),
                                   std::memory_order_release);
            state.connect_started.store(true, std::memory_order_release);
        })) {
        return false;
    }
    return wait_until([&] { return state.connect_started.load(std::memory_order_acquire); }) &&
           state.connect_ok.load(std::memory_order_acquire);
}

[[nodiscard]] bool stop_runtime_tcp_client(af::runtime &runtime, af::thread_ref io_thread,
                                           af::net::tcp_client &client,
                                           RuntimeTcpClientState &state) {
    state.stopped.store(false, std::memory_order_release);
    state.stop_ok.store(false, std::memory_order_release);
    if (!runtime.post(io_thread, [&client, &state] {
            state.stop_ok.store(client.stop(), std::memory_order_release);
            state.stopped.store(true, std::memory_order_release);
        })) {
        return false;
    }
    return wait_until([&] { return state.stopped.load(std::memory_order_acquire); }) &&
           state.stop_ok.load(std::memory_order_acquire);
}

[[nodiscard]] bool stop_runtime_tcp_server(af::runtime &runtime, af::thread_ref io_thread,
                                           af::net::tcp_server &server,
                                           RuntimeTcpClientServerState &state) {
    state.stopped.store(false, std::memory_order_release);
    state.stop_ok.store(false, std::memory_order_release);
    if (!runtime.post(io_thread, [&server, &state] {
            state.stop_ok.store(server.stop(), std::memory_order_release);
            state.stopped.store(true, std::memory_order_release);
        })) {
        return false;
    }
    return wait_until([&] { return state.stopped.load(std::memory_order_acquire); }) &&
           state.stop_ok.load(std::memory_order_acquire);
}

[[nodiscard]] bool query_pending_connects(af::runtime &runtime, af::thread_ref io_thread,
                                          af::net::tcp_client &client, std::size_t &count) {
    std::atomic<bool> done{false};
    if (!runtime.post(io_thread, [&client, &count, &done] {
            count = client.pending_connect_count();
            done.store(true, std::memory_order_release);
        })) {
        return false;
    }
    return wait_until([&] { return done.load(std::memory_order_acquire); });
}

} // namespace

TEST(NetTcpClientTests, RuntimeTcpClientConnectsAndReceivesEcho) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpClientServerState server_state;
    RuntimeTcpClientState client_state;
    client_state.send_on_connect = true;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    af::net::tcp_client client(runtime);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_connection_callbacks callbacks;
        callbacks.owner = &server_state;
        callbacks.on_accept = &runtime_tcp_client_server_accept;
        callbacks.on_read = &runtime_tcp_client_server_read;
        callbacks.on_close = &runtime_tcp_client_server_close;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-client-echo-server";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_thread};
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        bool ok = listener.ok() && server.start();
        const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
        ok = ok && endpoint != nullptr;
        if (ok) {
            server_state.port.store(endpoint->port, std::memory_order_release);
        }
        server_state.start_ok.store(ok, std::memory_order_release);
        server_state.started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return server_state.started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(server_state.start_ok.load(std::memory_order_acquire));
    ASSERT_GT(server_state.port.load(std::memory_order_acquire), 0U);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_client_callbacks callbacks;
        callbacks.owner = &client_state;
        callbacks.on_connect = &runtime_tcp_client_connect;
        callbacks.on_read = &runtime_tcp_client_read;
        callbacks.on_close = &runtime_tcp_client_close;
        callbacks.on_error = &runtime_tcp_client_error;

        af::net::tcp_client_connect_config connect_config;
        connect_config.name = "runtime-client";
        connect_config.remote_endpoint =
            af::net::tcp_endpoint::loopback_v4(server_state.port.load(std::memory_order_acquire));
        connect_config.owner_thread = io_thread;
        connect_config.connect_timeout = std::chrono::seconds(2);

        const bool ok = client.connect(std::move(connect_config), callbacks);
        client_state.connect_ok.store(ok, std::memory_order_release);
        client_state.connect_started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(
        wait_until([&] { return client_state.connect_started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(client_state.connect_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(
        wait_until([&] { return client_state.size.load(std::memory_order_acquire) == 5U; }));
    EXPECT_EQ(
        std::string(client_state.data.data(), client_state.size.load(std::memory_order_acquire)),
        "hello");
    EXPECT_EQ(client_state.errors.load(std::memory_order_acquire), 0);
    EXPECT_GE(server_state.accepted.load(std::memory_order_acquire), 1);
    EXPECT_GE(server_state.reads.load(std::memory_order_acquire), 1);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        client_state.stop_ok.store(client.stop(), std::memory_order_release);
        client_state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return client_state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(client_state.stop_ok.load(std::memory_order_acquire));

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        server_state.stop_ok.store(server.stop(), std::memory_order_release);
        server_state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return server_state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(server_state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpClientTests, RuntimeTcpClientHandleSendsFromExternalThread) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpClientServerState server_state;
    RuntimeTcpClientState client_state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    af::net::tcp_client client(runtime);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_connection_callbacks callbacks;
        callbacks.owner = &server_state;
        callbacks.on_accept = &runtime_tcp_client_server_accept;
        callbacks.on_read = &runtime_tcp_client_server_read;
        callbacks.on_close = &runtime_tcp_client_server_close;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-client-handle-server";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_thread};
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        bool ok = listener.ok() && server.start();
        const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
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
        af::net::tcp_client_callbacks callbacks;
        callbacks.owner = &client_state;
        callbacks.on_connect = &runtime_tcp_client_connect;
        callbacks.on_read = &runtime_tcp_client_read;
        callbacks.on_close = &runtime_tcp_client_close;
        callbacks.on_error = &runtime_tcp_client_error;

        af::net::tcp_client_connect_config connect_config;
        connect_config.name = "runtime-client-external-handle";
        connect_config.remote_endpoint =
            af::net::tcp_endpoint::loopback_v4(server_state.port.load(std::memory_order_acquire));
        connect_config.owner_thread = io_thread;
        connect_config.connect_timeout = std::chrono::seconds(2);

        const bool ok = client.connect(std::move(connect_config), callbacks);
        client_state.connect_ok.store(ok, std::memory_order_release);
        client_state.connect_started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(
        wait_until([&] { return client_state.connect_started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(client_state.connect_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(wait_until([&] { return client_state.connected.load(std::memory_order_acquire); }));

    EXPECT_EQ(client_state.handle.send(af::buffer::copy("queued", 6)),
              af::net::send_result::queued);
    ASSERT_TRUE(
        wait_until([&] { return client_state.size.load(std::memory_order_acquire) == 6U; }));
    EXPECT_EQ(
        std::string(client_state.data.data(), client_state.size.load(std::memory_order_acquire)),
        "queued");

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        client_state.stop_ok.store(client.stop(), std::memory_order_release);
        client_state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return client_state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(client_state.stop_ok.load(std::memory_order_acquire));

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        server_state.stop_ok.store(server.stop(), std::memory_order_release);
        server_state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return server_state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(server_state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpClientTests, RuntimeTcpClientHandleCloseReclaimsSlot) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpClientServerState server_state;
    RuntimeTcpClientState client_state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    af::net::tcp_client client(runtime);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_connection_callbacks callbacks;
        callbacks.owner = &server_state;
        callbacks.on_accept = &runtime_tcp_client_server_accept;
        callbacks.on_read = &runtime_tcp_client_server_read;
        callbacks.on_close = &runtime_tcp_client_server_close;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-client-handle-close-server";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_thread};
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        bool ok = listener.ok() && server.start();
        const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
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
        af::net::tcp_client_callbacks callbacks;
        callbacks.owner = &client_state;
        callbacks.on_connect = &runtime_tcp_client_connect;
        callbacks.on_read = &runtime_tcp_client_read;
        callbacks.on_close = &runtime_tcp_client_close;
        callbacks.on_error = &runtime_tcp_client_error;

        af::net::tcp_client_connect_config connect_config;
        connect_config.name = "runtime-client-handle-close";
        connect_config.remote_endpoint =
            af::net::tcp_endpoint::loopback_v4(server_state.port.load(std::memory_order_acquire));
        connect_config.owner_thread = io_thread;
        connect_config.connect_timeout = std::chrono::seconds(2);

        const bool ok = client.connect(std::move(connect_config), callbacks);
        client_state.connect_ok.store(ok, std::memory_order_release);
        client_state.connect_started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(
        wait_until([&] { return client_state.connect_started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(client_state.connect_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(wait_until([&] { return client_state.connected.load(std::memory_order_acquire); }));

    EXPECT_TRUE(client_state.handle.close());
    ASSERT_TRUE(
        wait_until([&] { return client_state.closes.load(std::memory_order_acquire) >= 1; }));

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        client_state.connection_count_after_close.store(static_cast<int>(client.connection_count()),
                                                        std::memory_order_release);
        client_state.connection_count_checked.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until(
        [&] { return client_state.connection_count_checked.load(std::memory_order_acquire); }));
    EXPECT_EQ(client_state.connection_count_after_close.load(std::memory_order_acquire), 0);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        server_state.stop_ok.store(server.stop(), std::memory_order_release);
        server_state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return server_state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(server_state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpClientTests, RuntimeTcpClientReportsConnectError) {
    const std::uint16_t port = reserve_loopback_port();

    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpClientState client_state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_client client(runtime);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_client_callbacks callbacks;
        callbacks.owner = &client_state;
        callbacks.on_connect = &runtime_tcp_client_connect;
        callbacks.on_read = &runtime_tcp_client_read;
        callbacks.on_close = &runtime_tcp_client_close;
        callbacks.on_error = &runtime_tcp_client_error;

        af::net::tcp_client_connect_config connect_config;
        connect_config.name = "runtime-client-error";
        connect_config.remote_endpoint = af::net::tcp_endpoint::loopback_v4(port);
        connect_config.owner_thread = io_thread;
        connect_config.connect_timeout = std::chrono::seconds(2);

        const bool ok = client.connect(std::move(connect_config), callbacks);
        client_state.connect_ok.store(ok, std::memory_order_release);
        client_state.connect_started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(
        wait_until([&] { return client_state.connect_started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(client_state.connect_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(
        wait_until([&] { return client_state.errors.load(std::memory_order_acquire) == 1; }));
    EXPECT_FALSE(client_state.connected.load(std::memory_order_acquire));
    EXPECT_NE(client_state.last_error.load(std::memory_order_acquire), 0);

    runtime.stop();
}

TEST(NetTcpClientTests, ConnectsAndReceivesEcho) {
    af::runtime runtime(make_tcp_client_runtime_config());
    RuntimeTcpClientServerState server_state;
    RuntimeTcpClientState client_state;
    client_state.send_on_connect = true;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    af::net::tcp_client client(runtime);
    ASSERT_TRUE(start_runtime_tcp_echo_server(runtime, io_thread, server, server_state,
                                              af::net::tcp_endpoint::loopback_v4(0),
                                              "tcp-client-test-server"));
    ASSERT_TRUE(connect_runtime_tcp_client(
        runtime, io_thread, client, client_state,
        af::net::tcp_endpoint::loopback_v4(server_state.port.load(std::memory_order_acquire)),
        "tcp-client"));

    ASSERT_TRUE(
        wait_until([&] { return client_state.size.load(std::memory_order_acquire) == 5U; }));
    EXPECT_EQ(
        std::string(client_state.data.data(), client_state.size.load(std::memory_order_acquire)),
        "hello");
    EXPECT_EQ(client_state.errors.load(std::memory_order_acquire), 0);

    EXPECT_TRUE(stop_runtime_tcp_client(runtime, io_thread, client, client_state));
    EXPECT_TRUE(stop_runtime_tcp_server(runtime, io_thread, server, server_state));
    runtime.stop();
}

TEST(NetTcpClientTests, ConnectsOverIpv6Loopback) {
    std::uint16_t port = 0;
    if (!reserve_loopback_v6_port(port)) {
        GTEST_SKIP() << "IPv6 loopback is not available";
    }

    af::runtime runtime(make_tcp_client_runtime_config("net-client-v6-io"));
    RuntimeTcpClientServerState server_state;
    RuntimeTcpClientState client_state;
    client_state.send_on_connect = true;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    af::net::tcp_client client(runtime);
    ASSERT_TRUE(start_runtime_tcp_echo_server(runtime, io_thread, server, server_state,
                                              af::net::tcp_endpoint::loopback_v6(port),
                                              "tcp-client-v6-server"));
    ASSERT_TRUE(connect_runtime_tcp_client(runtime, io_thread, client, client_state,
                                           af::net::tcp_endpoint::loopback_v6(port),
                                           "tcp-client-v6"));

    ASSERT_TRUE(
        wait_until([&] { return client_state.size.load(std::memory_order_acquire) == 5U; }));
    EXPECT_EQ(
        std::string(client_state.data.data(), client_state.size.load(std::memory_order_acquire)),
        "hello");
    EXPECT_EQ(client_state.errors.load(std::memory_order_acquire), 0);

    EXPECT_TRUE(stop_runtime_tcp_client(runtime, io_thread, client, client_state));
    EXPECT_TRUE(stop_runtime_tcp_server(runtime, io_thread, server, server_state));
    runtime.stop();
}

TEST(NetTcpClientTests, ExternalHandleSendIsQueuedToOwnerIoThread) {
    af::runtime runtime(make_tcp_client_runtime_config("net-client-external-io"));
    RuntimeTcpClientServerState server_state;
    RuntimeTcpClientState client_state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    af::net::tcp_client client(runtime);
    ASSERT_TRUE(start_runtime_tcp_echo_server(runtime, io_thread, server, server_state,
                                              af::net::tcp_endpoint::loopback_v4(0),
                                              "tcp-client-external-send-server"));
    ASSERT_TRUE(connect_runtime_tcp_client(
        runtime, io_thread, client, client_state,
        af::net::tcp_endpoint::loopback_v4(server_state.port.load(std::memory_order_acquire)),
        "tcp-client-external-send"));

    ASSERT_TRUE(wait_until([&] { return client_state.connected.load(std::memory_order_acquire); }));
    EXPECT_EQ(client_state.handle.send(af::buffer::copy("queued", 6)),
              af::net::send_result::queued);
    ASSERT_TRUE(
        wait_until([&] { return client_state.size.load(std::memory_order_acquire) == 6U; }));
    EXPECT_EQ(
        std::string(client_state.data.data(), client_state.size.load(std::memory_order_acquire)),
        "queued");

    EXPECT_TRUE(stop_runtime_tcp_client(runtime, io_thread, client, client_state));
    EXPECT_TRUE(stop_runtime_tcp_server(runtime, io_thread, server, server_state));
    runtime.stop();
}

TEST(NetTcpClientTests, StopRejectsWritesFromOldHandles) {
    af::runtime runtime(make_tcp_client_runtime_config("net-client-old-handle-io"));
    RuntimeTcpClientServerState server_state;
    RuntimeTcpClientState client_state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    af::net::tcp_client client(runtime);
    ASSERT_TRUE(start_runtime_tcp_echo_server(runtime, io_thread, server, server_state,
                                              af::net::tcp_endpoint::loopback_v4(0),
                                              "tcp-client-stop-old-handle-server"));
    ASSERT_TRUE(connect_runtime_tcp_client(
        runtime, io_thread, client, client_state,
        af::net::tcp_endpoint::loopback_v4(server_state.port.load(std::memory_order_acquire)),
        "tcp-client-stop-old-handle"));

    ASSERT_TRUE(wait_until([&] { return client_state.connected.load(std::memory_order_acquire); }));
    EXPECT_TRUE(stop_runtime_tcp_client(runtime, io_thread, client, client_state));
    EXPECT_EQ(client_state.handle.send(af::buffer::copy("late", 4)), af::net::send_result::closed);

    EXPECT_TRUE(stop_runtime_tcp_server(runtime, io_thread, server, server_state));
    runtime.stop();
}

TEST(NetTcpClientTests, BindLocalEndpointConnectsWhenFamilyMatches) {
    af::runtime runtime(make_tcp_client_runtime_config("net-client-bind-io"));
    RuntimeTcpClientServerState server_state;
    RuntimeTcpClientState client_state;
    client_state.send_on_connect = true;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    af::net::tcp_client client(runtime);
    ASSERT_TRUE(start_runtime_tcp_echo_server(runtime, io_thread, server, server_state,
                                              af::net::tcp_endpoint::loopback_v4(0),
                                              "tcp-client-bind-local-server"));
    ASSERT_TRUE(connect_runtime_tcp_client(
        runtime, io_thread, client, client_state,
        af::net::tcp_endpoint::loopback_v4(server_state.port.load(std::memory_order_acquire)),
        "tcp-client-bind-local", af::net::tcp_endpoint::loopback_v4(0), true));

    ASSERT_TRUE(
        wait_until([&] { return client_state.size.load(std::memory_order_acquire) == 5U; }));
    EXPECT_EQ(
        std::string(client_state.data.data(), client_state.size.load(std::memory_order_acquire)),
        "hello");

    EXPECT_TRUE(stop_runtime_tcp_client(runtime, io_thread, client, client_state));
    EXPECT_TRUE(stop_runtime_tcp_server(runtime, io_thread, server, server_state));
    runtime.stop();
}

TEST(NetTcpClientTests, RejectsBindLocalAddressFamilyMismatchSynchronously) {
    const std::uint16_t port = reserve_loopback_port();
    af::runtime runtime(make_tcp_client_runtime_config("net-client-mismatch-io"));
    RuntimeTcpClientState client_state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_client client(runtime);
    EXPECT_FALSE(connect_runtime_tcp_client(
        runtime, io_thread, client, client_state, af::net::tcp_endpoint::loopback_v4(port),
        "tcp-client-bind-local-family-mismatch", af::net::tcp_endpoint::loopback_v6(0), true));
    EXPECT_TRUE(client_state.connect_started.load(std::memory_order_acquire));
    EXPECT_FALSE(client_state.connect_ok.load(std::memory_order_acquire));
    EXPECT_EQ(client_state.errors.load(std::memory_order_acquire), 0);

    runtime.stop();
}

TEST(NetTcpClientTests, ReportsConnectionErrors) {
    const std::uint16_t port = reserve_loopback_port();
    af::runtime runtime(make_tcp_client_runtime_config("net-client-error-io"));
    RuntimeTcpClientState client_state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_client client(runtime);
    ASSERT_TRUE(connect_runtime_tcp_client(runtime, io_thread, client, client_state,
                                           af::net::tcp_endpoint::loopback_v4(port),
                                           "tcp-client-error"));

    ASSERT_TRUE(
        wait_until([&] { return client_state.errors.load(std::memory_order_acquire) == 1; }));
    EXPECT_FALSE(client_state.connected.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpClientTests, ConnectAfterStopRestartsClientControlPlane) {
    af::runtime runtime(make_tcp_client_runtime_config("net-client-restart-io"));
    RuntimeTcpClientServerState server_state;
    RuntimeTcpClientState first_state;
    RuntimeTcpClientState second_state;
    first_state.send_on_connect = true;
    second_state.send_on_connect = true;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    af::net::tcp_client client(runtime);
    ASSERT_TRUE(start_runtime_tcp_echo_server(runtime, io_thread, server, server_state,
                                              af::net::tcp_endpoint::loopback_v4(0),
                                              "tcp-client-restart-server"));

    const af::net::tcp_endpoint endpoint =
        af::net::tcp_endpoint::loopback_v4(server_state.port.load(std::memory_order_acquire));
    ASSERT_TRUE(connect_runtime_tcp_client(runtime, io_thread, client, first_state, endpoint,
                                           "tcp-client-restart-first"));
    ASSERT_TRUE(wait_until([&] { return first_state.size.load(std::memory_order_acquire) == 5U; }));
    EXPECT_TRUE(stop_runtime_tcp_client(runtime, io_thread, client, first_state));

    ASSERT_TRUE(connect_runtime_tcp_client(runtime, io_thread, client, second_state, endpoint,
                                           "tcp-client-restart-second"));
    ASSERT_TRUE(
        wait_until([&] { return second_state.size.load(std::memory_order_acquire) == 5U; }));
    EXPECT_EQ(
        std::string(second_state.data.data(), second_state.size.load(std::memory_order_acquire)),
        "hello");

    EXPECT_TRUE(stop_runtime_tcp_client(runtime, io_thread, client, second_state));
    EXPECT_TRUE(stop_runtime_tcp_server(runtime, io_thread, server, server_state));
    runtime.stop();
}

TEST(NetTcpClientTests, StopReturnsWithoutWaitingForConnectTimeout) {
    af::runtime runtime(make_tcp_client_runtime_config("net-client-pending-stop-io"));
    RuntimeTcpClientState client_state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_client client(runtime);
    ASSERT_TRUE(connect_runtime_tcp_client(
        runtime, io_thread, client, client_state,
        af::net::tcp_endpoint::host("203.0.113.1", 9, af::net::address_family::ipv4),
        "tcp-client-stop-pending-connect", af::net::tcp_endpoint::any(0), false,
        std::chrono::seconds(3)));

    std::size_t pending_count = 0;
    ASSERT_TRUE(query_pending_connects(runtime, io_thread, client, pending_count));
    if (pending_count == 0U) {
        static_cast<void>(stop_runtime_tcp_client(runtime, io_thread, client, client_state));
        runtime.stop();
        GTEST_SKIP() << "pending connect completed synchronously on this host";
    }

    const auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(stop_runtime_tcp_client(runtime, io_thread, client, client_state));
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, std::chrono::seconds(1));

    runtime.stop();
}

TEST(NetTcpClientTests, StalePendingConnectCompletionDoesNotStopNewConnect) {
    af::runtime runtime(make_tcp_client_runtime_config("net-client-stale-pending-io"));
    RuntimeTcpClientServerState server_state;
    RuntimeTcpClientState stale_state;
    RuntimeTcpClientState current_state;
    current_state.send_on_connect = true;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_client client(runtime);
    ASSERT_TRUE(connect_runtime_tcp_client(
        runtime, io_thread, client, stale_state,
        af::net::tcp_endpoint::host("203.0.113.1", 9, af::net::address_family::ipv4),
        "tcp-client-stale-pending", af::net::tcp_endpoint::any(0), false,
        std::chrono::milliseconds(50)));

    std::size_t pending_count = 0;
    ASSERT_TRUE(query_pending_connects(runtime, io_thread, client, pending_count));
    if (pending_count == 0U) {
        static_cast<void>(stop_runtime_tcp_client(runtime, io_thread, client, stale_state));
        runtime.stop();
        GTEST_SKIP() << "pending connect completed synchronously on this host";
    }
    ASSERT_TRUE(stop_runtime_tcp_client(runtime, io_thread, client, stale_state));

    af::net::tcp_server server(runtime);
    ASSERT_TRUE(start_runtime_tcp_echo_server(runtime, io_thread, server, server_state,
                                              af::net::tcp_endpoint::loopback_v4(0),
                                              "tcp-client-stale-current-server"));
    ASSERT_TRUE(connect_runtime_tcp_client(
        runtime, io_thread, client, current_state,
        af::net::tcp_endpoint::loopback_v4(server_state.port.load(std::memory_order_acquire)),
        "tcp-client-stale-current"));
    ASSERT_TRUE(
        wait_until([&] { return current_state.size.load(std::memory_order_acquire) == 5U; }));
    EXPECT_EQ(
        std::string(current_state.data.data(), current_state.size.load(std::memory_order_acquire)),
        "hello");

    std::this_thread::sleep_for(std::chrono::milliseconds(75));
    EXPECT_EQ(current_state.errors.load(std::memory_order_acquire), 0);
    EXPECT_TRUE(current_state.connected.load(std::memory_order_acquire));

    EXPECT_TRUE(stop_runtime_tcp_client(runtime, io_thread, client, current_state));
    EXPECT_TRUE(stop_runtime_tcp_server(runtime, io_thread, server, server_state));
    runtime.stop();
}
