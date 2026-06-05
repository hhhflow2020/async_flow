#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "af/net.hpp"
#include "af/platform.hpp"
#include "af/runtime.hpp"

#include "gtest/gtest.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace {

[[nodiscard]] std::string unique_unix_socket_path() {
    static std::atomic<std::uint32_t> counter{0};
    return "/tmp/asyncflow-unix-" + std::to_string(::getpid()) + "-" +
           std::to_string(counter.fetch_add(1, std::memory_order_relaxed)) + ".sock";
}

class UnixPathGuard {
public:
    explicit UnixPathGuard(std::string path) : path_(std::move(path)) {
        static_cast<void>(::unlink(path_.c_str()));
    }

    ~UnixPathGuard() {
        static_cast<void>(::unlink(path_.c_str()));
    }

    [[nodiscard]] const std::string &path() const noexcept {
        return path_;
    }

private:
    std::string path_;
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

struct RuntimeUnixStreamState {
    std::atomic<bool> server_started{false};
    std::atomic<bool> server_start_ok{false};
    std::atomic<bool> client_started{false};
    std::atomic<bool> client_start_ok{false};
    std::atomic<bool> stopped{false};
    std::atomic<int> accepted{0};
    std::atomic<int> connected{0};
    std::atomic<int> errors{0};
    std::array<char, 64> data{};
    std::atomic<std::size_t> size{0};
    af::net::unix_connection_handle handle;
    std::string_view connect_payload{"runtime-unix"};
};

void runtime_unix_stream_accept(void *owner, af::net::unix_connection_ref conn) noexcept {
    static_cast<void>(conn);
    auto *state = static_cast<RuntimeUnixStreamState *>(owner);
    if (state != nullptr) {
        state->accepted.fetch_add(1, std::memory_order_release);
    }
}

void runtime_unix_stream_echo(void *owner, af::net::unix_connection_ref conn,
                              af::BufferView bytes) noexcept {
    static_cast<void>(owner);
    static_cast<void>(conn.send(bytes));
}

void runtime_unix_stream_connect(void *owner, af::net::unix_connection_ref conn) noexcept {
    auto *state = static_cast<RuntimeUnixStreamState *>(owner);
    std::string_view payload{"runtime-unix"};
    if (state != nullptr) {
        state->handle = conn.handle();
        state->connected.fetch_add(1, std::memory_order_release);
        payload = state->connect_payload;
    }
    if (!payload.empty()) {
        static_cast<void>(conn.send(af::Buffer::copy(payload.data(), payload.size())));
    }
}

void runtime_unix_stream_passive_connect(void *owner, af::net::unix_connection_ref conn) noexcept {
    auto *state = static_cast<RuntimeUnixStreamState *>(owner);
    if (state != nullptr) {
        state->handle = conn.handle();
        state->connected.fetch_add(1, std::memory_order_release);
    }
}

void runtime_unix_stream_capture(void *owner, af::net::unix_connection_ref conn,
                                 af::BufferView bytes) noexcept {
    auto *state = static_cast<RuntimeUnixStreamState *>(owner);
    if (state == nullptr) {
        return;
    }
    const std::size_t size = std::min(bytes.size(), state->data.size());
    std::memcpy(state->data.data(), bytes.data(), size);
    state->size.store(size, std::memory_order_release);
    conn.close();
}

void runtime_unix_stream_error(void *owner, int error) noexcept {
    static_cast<void>(error);
    auto *state = static_cast<RuntimeUnixStreamState *>(owner);
    if (state != nullptr) {
        state->errors.fetch_add(1, std::memory_order_release);
    }
}

struct RuntimeUnixDatagramState {
    std::atomic<bool> server_started{false};
    std::atomic<bool> server_start_ok{false};
    std::atomic<bool> client_started{false};
    std::atomic<bool> client_start_ok{false};
    std::atomic<bool> stopped{false};
    std::atomic<int> datagrams{0};
    std::atomic<int> errors{0};
    std::array<char, 64> data{};
    std::atomic<std::size_t> size{0};
    af::net::unix_datagram_socket_handle handle;
};

void runtime_unix_datagram_echo(void *owner, af::net::unix_datagram_socket_ref socket,
                                af::BufferView bytes,
                                const af::net::unix_datagram_peer &peer) noexcept {
    auto *state = static_cast<RuntimeUnixDatagramState *>(owner);
    if (state != nullptr) {
        state->datagrams.fetch_add(1, std::memory_order_release);
    }
    static_cast<void>(socket.send_to(bytes, peer));
}

void runtime_unix_datagram_capture(void *owner, af::net::unix_datagram_socket_ref socket,
                                   af::BufferView bytes,
                                   const af::net::unix_datagram_peer &peer) noexcept {
    static_cast<void>(peer);
    auto *state = static_cast<RuntimeUnixDatagramState *>(owner);
    if (state == nullptr) {
        return;
    }
    state->handle = socket.handle();
    const std::size_t size = std::min(bytes.size(), state->data.size());
    std::memcpy(state->data.data(), bytes.data(), size);
    state->size.store(size, std::memory_order_release);
    state->datagrams.fetch_add(1, std::memory_order_release);
}

void runtime_unix_datagram_error(void *owner, af::net::unix_datagram_socket_handle socket,
                                 int error) noexcept {
    static_cast<void>(socket);
    static_cast<void>(error);
    auto *state = static_cast<RuntimeUnixDatagramState *>(owner);
    if (state != nullptr) {
        state->errors.fetch_add(1, std::memory_order_release);
    }
}

void runtime_unix_datagram_noop(void *owner, af::net::unix_datagram_socket_ref socket,
                                af::BufferView bytes,
                                const af::net::unix_datagram_peer &peer) noexcept {
    static_cast<void>(socket);
    static_cast<void>(bytes);
    static_cast<void>(peer);
    auto *state = static_cast<RuntimeUnixDatagramState *>(owner);
    if (state != nullptr) {
        state->datagrams.fetch_add(1, std::memory_order_release);
    }
}

[[nodiscard]] af::runtime_config make_unix_runtime_config(std::string name,
                                                          std::size_t io_thread_count = 1) {
    af::runtime_config config;
    config.threads = {af::io_threads(std::move(name), io_thread_count)};
    config.logger.consumer_thread = af::thread_selector::io(0);
    return config;
}

[[nodiscard]] bool start_unix_stream_server(af::runtime &runtime, af::thread_ref owner_thread,
                                            af::net::unix_stream_server &server, std::string path,
                                            RuntimeUnixStreamState &state,
                                            std::vector<af::thread_ref> listener_threads = {},
                                            af::net::tcp_listener_options options = {}) {
    state.server_started.store(false, std::memory_order_release);
    state.server_start_ok.store(false, std::memory_order_release);
    if (listener_threads.empty()) {
        listener_threads.push_back(owner_thread);
    }

    if (!runtime.post(owner_thread, [&server, path = std::move(path), &state,
                                     threads = std::move(listener_threads), options]() mutable {
            af::net::unix_stream_callbacks callbacks;
            callbacks.owner = &state;
            callbacks.on_accept = &runtime_unix_stream_accept;
            callbacks.on_read = &runtime_unix_stream_echo;

            af::net::unix_stream_listener_config listener_config;
            listener_config.name = "unix-stream-listener";
            listener_config.endpoint = af::net::unix_endpoint::unix_path(std::move(path));
            listener_config.threads = std::move(threads);
            listener_config.options = options;

            const af::net::listener_result listener =
                server.add_listener(std::move(listener_config), callbacks);
            const bool ok = listener.ok() && server.start();
            state.server_start_ok.store(ok, std::memory_order_release);
            state.server_started.store(true, std::memory_order_release);
        })) {
        return false;
    }
    return wait_until([&] { return state.server_started.load(std::memory_order_acquire); }) &&
           state.server_start_ok.load(std::memory_order_acquire);
}

[[nodiscard]] bool connect_unix_stream_client(
    af::runtime &runtime, af::thread_ref owner_thread, af::net::unix_stream_client &client,
    std::string path, RuntimeUnixStreamState &state,
    af::net::tcp_client_connect_callback on_connect = &runtime_unix_stream_connect) {
    state.client_started.store(false, std::memory_order_release);
    state.client_start_ok.store(false, std::memory_order_release);

    if (!runtime.post(
            owner_thread, [&client, path = std::move(path), &state, on_connect]() mutable {
                af::net::unix_stream_client_callbacks callbacks;
                callbacks.owner = &state;
                callbacks.on_connect = on_connect;
                callbacks.on_read = &runtime_unix_stream_capture;
                callbacks.on_error = &runtime_unix_stream_error;

                af::net::unix_stream_connect_config connect_config;
                connect_config.name = "unix-stream-client";
                connect_config.endpoint = af::net::unix_endpoint::unix_path(std::move(path));
                connect_config.owner_thread = af::thread_ref(af::runtime::current_thread_index());
                connect_config.connect_timeout = std::chrono::seconds(2);

                state.client_start_ok.store(client.connect(std::move(connect_config), callbacks),
                                            std::memory_order_release);
                state.client_started.store(true, std::memory_order_release);
            })) {
        return false;
    }
    return wait_until([&] { return state.client_started.load(std::memory_order_acquire); }) &&
           state.client_start_ok.load(std::memory_order_acquire);
}

[[nodiscard]] bool stop_unix_stream_pair(af::runtime &runtime, af::thread_ref owner_thread,
                                         af::net::unix_stream_server *server,
                                         af::net::unix_stream_client *client,
                                         RuntimeUnixStreamState &state) {
    state.stopped.store(false, std::memory_order_release);
    if (!runtime.post(owner_thread, [server, client, &state] {
            if (client != nullptr) {
                static_cast<void>(client->stop());
            }
            const bool server_ok = server == nullptr || server->stop();
            state.stopped.store(server_ok, std::memory_order_release);
        })) {
        return false;
    }
    return wait_until([&] { return state.stopped.load(std::memory_order_acquire); });
}

[[nodiscard]] bool bind_unix_datagram_socket(af::runtime &runtime, af::thread_ref owner_thread,
                                             af::net::unix_datagram_socket &socket,
                                             std::string path, RuntimeUnixDatagramState &state,
                                             af::net::udp_datagram_callback on_datagram) {
    state.server_started.store(false, std::memory_order_release);
    state.server_start_ok.store(false, std::memory_order_release);
    if (!runtime.post(
            owner_thread, [&socket, path = std::move(path), &state, on_datagram]() mutable {
                af::net::unix_datagram_callbacks callbacks;
                callbacks.owner = &state;
                callbacks.on_datagram = on_datagram;
                callbacks.on_error = &runtime_unix_datagram_error;

                af::net::unix_datagram_bind_config bind_config;
                bind_config.name = "unix-datagram-bind";
                bind_config.local_endpoint = af::net::unix_endpoint::unix_path(std::move(path));
                bind_config.threads = {af::thread_ref(af::runtime::current_thread_index())};

                const bool ok = socket.bind(std::move(bind_config), callbacks);
                if (ok) {
                    state.handle = socket.handle();
                }
                state.server_start_ok.store(ok, std::memory_order_release);
                state.server_started.store(true, std::memory_order_release);
            })) {
        return false;
    }
    return wait_until([&] { return state.server_started.load(std::memory_order_acquire); }) &&
           state.server_start_ok.load(std::memory_order_acquire);
}

[[nodiscard]] bool connect_unix_datagram_socket(af::runtime &runtime, af::thread_ref owner_thread,
                                                af::net::unix_datagram_socket &socket,
                                                std::string local_path, std::string remote_path,
                                                RuntimeUnixDatagramState &state,
                                                af::net::udp_datagram_callback on_datagram) {
    state.client_started.store(false, std::memory_order_release);
    state.client_start_ok.store(false, std::memory_order_release);
    if (!runtime.post(owner_thread, [&socket, local_path = std::move(local_path),
                                     remote_path = std::move(remote_path), &state,
                                     on_datagram]() mutable {
            af::net::unix_datagram_callbacks callbacks;
            callbacks.owner = &state;
            callbacks.on_datagram = on_datagram;
            callbacks.on_error = &runtime_unix_datagram_error;

            af::net::unix_datagram_connect_config connect_config;
            connect_config.name = "unix-datagram-connect";
            connect_config.local_endpoint =
                af::net::unix_endpoint::unix_path(std::move(local_path));
            connect_config.remote_endpoint =
                af::net::unix_endpoint::unix_path(std::move(remote_path));
            connect_config.threads = {af::thread_ref(af::runtime::current_thread_index())};

            const bool ok = socket.connect(std::move(connect_config), callbacks);
            if (ok) {
                state.handle = socket.handle();
            }
            state.client_start_ok.store(ok && state.handle.valid(), std::memory_order_release);
            state.client_started.store(true, std::memory_order_release);
        })) {
        return false;
    }
    return wait_until([&] { return state.client_started.load(std::memory_order_acquire); }) &&
           state.client_start_ok.load(std::memory_order_acquire);
}

[[nodiscard]] bool stop_unix_datagram_pair(af::runtime &runtime, af::thread_ref owner_thread,
                                           af::net::unix_datagram_socket *server,
                                           af::net::unix_datagram_socket *client,
                                           RuntimeUnixDatagramState &state) {
    state.stopped.store(false, std::memory_order_release);
    if (!runtime.post(owner_thread, [server, client, &state] {
            const bool client_ok = client == nullptr || client->stop();
            const bool server_ok = server == nullptr || server->stop();
            state.stopped.store(client_ok && server_ok, std::memory_order_release);
        })) {
        return false;
    }
    return wait_until([&] { return state.stopped.load(std::memory_order_acquire); });
}

} // namespace

TEST(NetUnixSocketTests, ServerAndClientEchoOverUnixStream) {
    UnixPathGuard path(unique_unix_socket_path());
    RuntimeUnixStreamState state;
    state.connect_payload = "unix";

    af::runtime runtime(make_unix_runtime_config("net-unix-io"));
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::unix_stream_server server(runtime);
    af::net::unix_stream_client client(runtime);
    ASSERT_TRUE(start_unix_stream_server(runtime, io_thread, server, path.path(), state));
    ASSERT_TRUE(connect_unix_stream_client(runtime, io_thread, client, path.path(), state));

    ASSERT_TRUE(wait_until([&] { return state.size.load(std::memory_order_acquire) == 4U; }));
    EXPECT_EQ(std::string(state.data.data(), state.size.load(std::memory_order_acquire)), "unix");
    EXPECT_EQ(state.errors.load(std::memory_order_acquire), 0);

    EXPECT_TRUE(stop_unix_stream_pair(runtime, io_thread, &server, &client, state));
    runtime.stop();
    EXPECT_NE(::access(path.path().c_str(), F_OK), 0);
}

TEST(NetUnixSocketTests, ExternalHandleSendIsQueuedToOwnerIoThread) {
    UnixPathGuard path(unique_unix_socket_path());
    RuntimeUnixStreamState state;

    af::runtime runtime(make_unix_runtime_config("net-unix-io"));
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::unix_stream_server server(runtime);
    af::net::unix_stream_client client(runtime);
    ASSERT_TRUE(start_unix_stream_server(runtime, io_thread, server, path.path(), state));
    ASSERT_TRUE(connect_unix_stream_client(runtime, io_thread, client, path.path(), state,
                                           &runtime_unix_stream_passive_connect));

    ASSERT_TRUE(wait_until([&] { return state.connected.load(std::memory_order_acquire) == 1; }));
    EXPECT_EQ(state.handle.send(af::Buffer::copy("queued", 6)), af::net::send_result::queued);
    ASSERT_TRUE(wait_until([&] { return state.size.load(std::memory_order_acquire) == 6U; }));
    EXPECT_EQ(std::string(state.data.data(), state.size.load(std::memory_order_acquire)), "queued");

    EXPECT_TRUE(stop_unix_stream_pair(runtime, io_thread, &server, &client, state));
    runtime.stop();
}

TEST(NetUnixSocketTests, ReportsConnectionErrors) {
    UnixPathGuard path(unique_unix_socket_path());
    RuntimeUnixStreamState state;

    af::runtime runtime(make_unix_runtime_config("net-unix-io"));
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::unix_stream_client client(runtime);
    ASSERT_TRUE(connect_unix_stream_client(runtime, io_thread, client, path.path(), state));

    ASSERT_TRUE(wait_until([&] { return state.errors.load(std::memory_order_acquire) == 1; }));
    EXPECT_EQ(state.connected.load(std::memory_order_acquire), 0);

    EXPECT_TRUE(stop_unix_stream_pair(runtime, io_thread, nullptr, &client, state));
    runtime.stop();
}

TEST(NetUnixSocketTests, ListenerUnlinksStalePathBeforeBind) {
    UnixPathGuard path(unique_unix_socket_path());
    {
        std::ofstream stale(path.path());
        ASSERT_TRUE(stale.good());
        stale << "stale";
    }

    RuntimeUnixStreamState state;
    af::runtime runtime(make_unix_runtime_config("net-unix-io"));
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::unix_stream_server server(runtime);
    ASSERT_TRUE(start_unix_stream_server(runtime, io_thread, server, path.path(), state));

    EXPECT_TRUE(stop_unix_stream_pair(runtime, io_thread, &server, nullptr, state));
    runtime.stop();
    EXPECT_NE(::access(path.path().c_str(), F_OK), 0);
}

TEST(NetUnixSocketTests, StreamWrapperNormalizesReusePortForMultiThreadUnixListener) {
    UnixPathGuard path(unique_unix_socket_path());
    af::net::tcp_listener_options options;
    options.reuse_port = true;

    RuntimeUnixStreamState state;
    af::runtime runtime(make_unix_runtime_config("net-unix-mio", 2));
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 2; }));

    const af::thread_ref io_thread0 = runtime.select_thread_ref(af::thread_selector::io(0));
    const af::thread_ref io_thread1 = runtime.select_thread_ref(af::thread_selector::io(1));
    af::net::unix_stream_server server(runtime);
    ASSERT_TRUE(start_unix_stream_server(runtime, io_thread0, server, path.path(), state,
                                         {io_thread0, io_thread1}, options));
    EXPECT_TRUE(stop_unix_stream_pair(runtime, io_thread0, &server, nullptr, state));
    runtime.stop();
    EXPECT_NE(::access(path.path().c_str(), F_OK), 0);
}

TEST(NetUnixSocketTests, DatagramServerAndClientEchoOverUnixSocket) {
    UnixPathGuard server_path(unique_unix_socket_path());
    UnixPathGuard client_path(unique_unix_socket_path());
    RuntimeUnixDatagramState state;

    af::runtime runtime(make_unix_runtime_config("net-unix-dgram-io"));
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::unix_datagram_socket server(runtime);
    af::net::unix_datagram_socket client(runtime);
    ASSERT_TRUE(bind_unix_datagram_socket(runtime, io_thread, server, server_path.path(), state,
                                          &runtime_unix_datagram_echo));
    ASSERT_TRUE(connect_unix_datagram_socket(runtime, io_thread, client, client_path.path(),
                                             server_path.path(), state,
                                             &runtime_unix_datagram_capture));

    EXPECT_EQ(state.handle.send(af::Buffer::copy("unix-dgram", 10)),
              af::net::udp_send_result::queued);
    ASSERT_TRUE(wait_until([&] { return state.size.load(std::memory_order_acquire) == 10U; }));
    EXPECT_EQ(std::string(state.data.data(), state.size.load(std::memory_order_acquire)),
              "unix-dgram");
    EXPECT_EQ(state.errors.load(std::memory_order_acquire), 0);

    EXPECT_TRUE(stop_unix_datagram_pair(runtime, io_thread, &server, &client, state));
    runtime.stop();
    EXPECT_NE(::access(server_path.path().c_str(), F_OK), 0);
    EXPECT_NE(::access(client_path.path().c_str(), F_OK), 0);
}

TEST(NetUnixSocketTests, DatagramUnlinksStalePathBeforeBind) {
    UnixPathGuard path(unique_unix_socket_path());
    {
        std::ofstream stale(path.path());
        ASSERT_TRUE(stale.good());
        stale << "stale";
    }

    RuntimeUnixDatagramState state;
    af::runtime runtime(make_unix_runtime_config("net-unix-dgram-io"));
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::unix_datagram_socket socket(runtime);
    ASSERT_TRUE(bind_unix_datagram_socket(runtime, io_thread, socket, path.path(), state,
                                          &runtime_unix_datagram_noop));

    EXPECT_TRUE(stop_unix_datagram_pair(runtime, io_thread, &socket, nullptr, state));
    runtime.stop();
    EXPECT_NE(::access(path.path().c_str(), F_OK), 0);
}

TEST(NetUnixSocketTests, RuntimeUnixStreamServerAndClientEcho) {
    UnixPathGuard path(unique_unix_socket_path());
    RuntimeUnixStreamState state;

    af::runtime_config config;
    config.threads = {af::io_threads("net-unix-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::unix_stream_server server(runtime);
    af::net::unix_stream_client client(runtime);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::unix_stream_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_unix_stream_accept;
        callbacks.on_read = &runtime_unix_stream_echo;

        af::net::unix_stream_listener_config listener_config;
        listener_config.name = "runtime-unix-stream";
        listener_config.endpoint = af::net::unix_endpoint::unix_path(path.path());
        listener_config.threads = {io_thread};

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        const bool ok = listener.ok() && server.start();
        state.server_start_ok.store(ok, std::memory_order_release);
        state.server_started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return state.server_started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(state.server_start_ok.load(std::memory_order_acquire));

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::unix_stream_client_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_connect = &runtime_unix_stream_connect;
        callbacks.on_read = &runtime_unix_stream_capture;
        callbacks.on_error = &runtime_unix_stream_error;

        af::net::unix_stream_connect_config connect_config;
        connect_config.name = "runtime-unix-stream-client";
        connect_config.endpoint = af::net::unix_endpoint::unix_path(path.path());
        connect_config.owner_thread = io_thread;
        connect_config.connect_timeout = std::chrono::seconds(2);

        const bool ok = client.connect(std::move(connect_config), callbacks);
        state.client_start_ok.store(ok, std::memory_order_release);
        state.client_started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return state.client_started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(state.client_start_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(wait_until([&] { return state.size.load(std::memory_order_acquire) == 12U; }));
    EXPECT_EQ(std::string(state.data.data(), state.size.load(std::memory_order_acquire)),
              "runtime-unix");
    EXPECT_EQ(state.accepted.load(std::memory_order_acquire), 1);
    EXPECT_EQ(state.connected.load(std::memory_order_acquire), 1);
    EXPECT_EQ(state.errors.load(std::memory_order_acquire), 0);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        static_cast<void>(client.stop());
        const bool server_stopped = server.stop();
        state.stopped.store(server_stopped, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));

    runtime.stop();
    EXPECT_NE(::access(path.path().c_str(), F_OK), 0);
}

TEST(NetUnixSocketTests, RuntimeUnixDatagramServerAndClientEcho) {
    UnixPathGuard server_path(unique_unix_socket_path());
    UnixPathGuard client_path(unique_unix_socket_path());
    RuntimeUnixDatagramState state;

    af::runtime_config config;
    config.threads = {af::io_threads("net-unix-dgram-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::unix_datagram_socket server(runtime);
    af::net::unix_datagram_socket client(runtime);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::unix_datagram_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_datagram = &runtime_unix_datagram_echo;
        callbacks.on_error = &runtime_unix_datagram_error;

        af::net::unix_datagram_bind_config bind_config;
        bind_config.name = "runtime-unix-dgram-server";
        bind_config.local_endpoint = af::net::unix_endpoint::unix_path(server_path.path());
        bind_config.threads = {io_thread};

        const bool ok = server.bind(std::move(bind_config), callbacks);
        state.server_start_ok.store(ok, std::memory_order_release);
        state.server_started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return state.server_started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(state.server_start_ok.load(std::memory_order_acquire));

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::unix_datagram_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_datagram = &runtime_unix_datagram_capture;
        callbacks.on_error = &runtime_unix_datagram_error;

        af::net::unix_datagram_connect_config connect_config;
        connect_config.name = "runtime-unix-dgram-client";
        connect_config.local_endpoint = af::net::unix_endpoint::unix_path(client_path.path());
        connect_config.remote_endpoint = af::net::unix_endpoint::unix_path(server_path.path());
        connect_config.threads = {io_thread};

        const bool ok = client.connect(std::move(connect_config), callbacks);
        state.handle = client.handle();
        state.client_start_ok.store(ok && state.handle.valid(), std::memory_order_release);
        state.client_started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return state.client_started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(state.client_start_ok.load(std::memory_order_acquire));
    EXPECT_EQ(state.handle.send(af::Buffer::copy("runtime-dgram", 13)),
              af::net::udp_send_result::queued);

    ASSERT_TRUE(wait_until([&] { return state.size.load(std::memory_order_acquire) == 13U; }));
    EXPECT_EQ(std::string(state.data.data(), state.size.load(std::memory_order_acquire)),
              "runtime-dgram");
    EXPECT_EQ(state.errors.load(std::memory_order_acquire), 0);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        const bool client_stopped = client.stop();
        const bool server_stopped = server.stop();
        state.stopped.store(client_stopped && server_stopped, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));

    runtime.stop();
    EXPECT_NE(::access(server_path.path().c_str(), F_OK), 0);
    EXPECT_NE(::access(client_path.path().c_str(), F_OK), 0);
}
