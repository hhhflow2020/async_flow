#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "af/async_runtime.hpp"
#include "af/net.hpp"
#include "af/platform.hpp"

#include "gtest/gtest.h"

#include <unistd.h>

namespace {

struct NetUnixIoTag;
struct NetUnixMultiIoTag;

struct NetUnixRuntimeTraits {
    static constexpr auto threads =
        af::thread_layout(af::thread_group<NetUnixIoTag, 1, af::thread_kind::io>("net-unix-io"));
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using NetUnixRuntime = af::AsyncRuntime<NetUnixRuntimeTraits>;

struct NetUnixMultiRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<NetUnixMultiIoTag, 2, af::thread_kind::io>("net-unix-mio"));
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using NetUnixMultiRuntime = af::AsyncRuntime<NetUnixMultiRuntimeTraits>;

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

struct ReactorCallState {
    std::atomic<bool> done{false};
    std::atomic<bool> ok{false};
};

template <typename Runtime> class ReactorCallTask final : public Runtime::Task {
public:
    explicit ReactorCallTask(typename Runtime::Task::FactoryToken token) : Runtime::Task(token) {}

    bool do_it(typename Runtime::Thread thread, std::function<bool()> fn,
               std::shared_ptr<ReactorCallState> state) {
        fn_ = std::move(fn);
        state_ = std::move(state);
        return this->schedule(thread);
    }

private:
    af::TaskResult run() override {
        bool ok = false;
        try {
            ok = fn_ != nullptr && fn_();
        } catch (...) {
            ok = false;
        }
        if (state_ != nullptr) {
            state_->ok.store(ok, std::memory_order_release);
            state_->done.store(true, std::memory_order_release);
        }
        return this->done();
    }

    std::function<bool()> fn_;
    std::shared_ptr<ReactorCallState> state_;
};

template <typename Runtime> [[nodiscard]] typename Runtime::Thread first_reactor_thread() noexcept {
    for (std::uint16_t i = 0; i < Runtime::thread_count; ++i) {
        const auto thread = Runtime::thread_from_index(i);
        const af::thread_kind kind = Runtime::thread_kind(thread);
        if (kind == af::thread_kind::io) {
            return thread;
        }
    }
    return Runtime::thread_from_index(0);
}

template <typename Runtime, typename Fn>
[[nodiscard]] bool call_on_reactor(typename Runtime::Thread thread, Fn &&fn) {
    auto state = std::make_shared<ReactorCallState>();
    std::function<bool()> wrapped(std::forward<Fn>(fn));
    if (!Runtime::template start_task<ReactorCallTask<Runtime>>(thread, std::move(wrapped),
                                                                state)) {
        return false;
    }
    if (!wait_until([&] { return state->done.load(std::memory_order_acquire); })) {
        return false;
    }
    return state->ok.load(std::memory_order_acquire);
}

template <typename Runtime> class ReactorUnixStreamServer {
public:
    using Thread = typename Runtime::Thread;
    using ListenerConfig = typename af::net::UnixStreamServer<Runtime>::ListenerConfig;

    ReactorUnixStreamServer() : control_thread_(first_reactor_thread<Runtime>()) {}

    explicit ReactorUnixStreamServer(af::net::UnixStreamRuntimeConfig config)
        : server_(config), control_thread_(first_reactor_thread<Runtime>()) {}

    ReactorUnixStreamServer &bind_threads(std::vector<Thread> threads) {
        static_cast<void>(call_on_reactor<Runtime>(control_thread_,
                                                   [this, threads = std::move(threads)]() mutable {
                                                       server_.bind_threads(std::move(threads));
                                                       return true;
                                                   }));
        return *this;
    }

    template <typename Group> ReactorUnixStreamServer &bind_threads(Group) {
        static_cast<void>(call_on_reactor<Runtime>(control_thread_, [this] {
            server_.bind_threads(Group{});
            return true;
        }));
        return *this;
    }

    template <typename Handler>
    [[nodiscard]] af::net::ListenerResult add_listener(ListenerConfig config,
                                                       Handler handler = Handler{}) {
        af::net::ListenerResult result = af::net::ListenerResult::failure(EIO);
        const bool called = call_on_reactor<Runtime>(
            control_thread_,
            [this, config = std::move(config), handler = std::move(handler), &result]() mutable {
                result =
                    server_.template add_listener<Handler>(std::move(config), std::move(handler));
                return true;
            });
        return called ? result : af::net::ListenerResult::failure(EIO);
    }

    [[nodiscard]] bool start() {
        return call_on_reactor<Runtime>(control_thread_, [this] { return server_.start(); });
    }

    [[nodiscard]] bool stop() {
        return call_on_reactor<Runtime>(control_thread_, [this] { return server_.stop(); });
    }

private:
    af::net::UnixStreamServer<Runtime> server_;
    Thread control_thread_{};
};

template <typename Runtime> class ReactorUnixStreamClient {
public:
    using Thread = typename Runtime::Thread;
    using ConnectConfig = typename af::net::UnixStreamClient<Runtime>::ConnectConfig;

    ReactorUnixStreamClient() : control_thread_(first_reactor_thread<Runtime>()) {}

    explicit ReactorUnixStreamClient(af::net::UnixStreamRuntimeConfig config)
        : client_(config), control_thread_(first_reactor_thread<Runtime>()) {}

    ReactorUnixStreamClient &bind_threads(std::vector<Thread> threads) {
        static_cast<void>(call_on_reactor<Runtime>(control_thread_,
                                                   [this, threads = std::move(threads)]() mutable {
                                                       client_.bind_threads(std::move(threads));
                                                       return true;
                                                   }));
        return *this;
    }

    template <typename Group> ReactorUnixStreamClient &bind_threads(Group) {
        static_cast<void>(call_on_reactor<Runtime>(control_thread_, [this] {
            client_.bind_threads(Group{});
            return true;
        }));
        return *this;
    }

    template <typename Handler>
    [[nodiscard]] bool connect(ConnectConfig config, Handler handler = Handler{}) {
        return call_on_reactor<Runtime>(control_thread_, [this, config = std::move(config),
                                                          handler = std::move(handler)]() mutable {
            return client_.template connect<Handler>(std::move(config), std::move(handler));
        });
    }

    [[nodiscard]] bool stop() {
        return call_on_reactor<Runtime>(control_thread_, [this] { return client_.stop(); });
    }

private:
    af::net::UnixStreamClient<Runtime> client_;
    Thread control_thread_{};
};

struct UnixEchoServerHandler {
    void on_read(af::net::UnixConnectionRef<NetUnixRuntime> conn, af::BufferView bytes) noexcept {
        static_cast<void>(conn.send(bytes));
    }
};

struct UnixClientState {
    std::atomic<bool> connected{false};
    std::atomic<int> errors{0};
    std::array<char, 64> data{};
    std::atomic<std::size_t> size{0};
    af::net::UnixConnectionHandle<NetUnixRuntime> handle;
};

struct SendingUnixClientHandler {
    std::shared_ptr<UnixClientState> state;

    void on_connect(af::net::UnixConnectionRef<NetUnixRuntime> conn) noexcept {
        if (state != nullptr) {
            state->handle = conn.handle();
            state->connected.store(true, std::memory_order_release);
        }
        static_cast<void>(conn.send(af::Buffer::copy("unix", 4)));
    }

    void on_read(af::net::UnixConnectionRef<NetUnixRuntime> conn, af::BufferView bytes) noexcept {
        if (state == nullptr) {
            return;
        }
        const std::size_t size = std::min(bytes.size(), state->data.size());
        std::memcpy(state->data.data(), bytes.data(), size);
        state->size.store(size, std::memory_order_release);
        conn.close();
    }

    void on_connect_error(int) noexcept {
        if (state != nullptr) {
            state->errors.fetch_add(1, std::memory_order_release);
        }
    }
};

struct PassiveUnixClientHandler {
    std::shared_ptr<UnixClientState> state;

    void on_connect(af::net::UnixConnectionRef<NetUnixRuntime> conn) noexcept {
        if (state != nullptr) {
            state->handle = conn.handle();
            state->connected.store(true, std::memory_order_release);
        }
    }

    void on_read(af::net::UnixConnectionRef<NetUnixRuntime> conn, af::BufferView bytes) noexcept {
        if (state == nullptr) {
            return;
        }
        const std::size_t size = std::min(bytes.size(), state->data.size());
        std::memcpy(state->data.data(), bytes.data(), size);
        state->size.store(size, std::memory_order_release);
        conn.close();
    }
};

struct ErrorUnixClientHandler {
    std::shared_ptr<UnixClientState> state;

    void on_connect(af::net::UnixConnectionRef<NetUnixRuntime>) noexcept {
        if (state != nullptr) {
            state->connected.store(true, std::memory_order_release);
        }
    }

    void on_connect_error(int) noexcept {
        if (state != nullptr) {
            state->errors.fetch_add(1, std::memory_order_release);
        }
    }
};

struct UnixDatagramEchoHandler {
    void on_datagram(af::net::UnixDatagramSocketRef<NetUnixRuntime> socket, af::BufferView bytes,
                     const af::net::UnixEndpoint &peer) noexcept {
        static_cast<void>(socket.send_to(bytes, peer));
    }
};

struct UnixDatagramState {
    std::array<char, 64> data{};
    std::atomic<std::size_t> size{0};
    std::atomic<int> errors{0};
};

struct UnixDatagramCaptureHandler {
    UnixDatagramState *state{nullptr};

    void on_datagram(af::net::UnixDatagramSocketRef<NetUnixRuntime>, af::BufferView bytes,
                     const af::net::UnixEndpoint &) noexcept {
        if (state == nullptr) {
            return;
        }
        const std::size_t size = std::min(bytes.size(), state->data.size());
        std::memcpy(state->data.data(), bytes.data(), size);
        state->size.store(size, std::memory_order_release);
    }

    void on_error(af::net::UnixDatagramSocketHandle<NetUnixRuntime>, int) noexcept {
        if (state != nullptr) {
            state->errors.fetch_add(1, std::memory_order_release);
        }
    }
};

struct UnixDatagramNoopHandler {
    void on_datagram(af::net::UnixDatagramSocketRef<NetUnixRuntime>, af::BufferView,
                     const af::net::UnixEndpoint &) noexcept {}
};

struct UnixMultiNoopHandler {};

} // namespace

TEST(NetUnixSocketTests, ServerAndClientEchoOverUnixStream) {
    UnixPathGuard path(unique_unix_socket_path());
    auto state = std::make_shared<UnixClientState>();

    NetUnixRuntime::init();
    ReactorUnixStreamServer<NetUnixRuntime> server;
    server.bind_threads(NetUnixRuntime::thread_group<NetUnixIoTag>());
    const af::net::ListenerResult listener = server.add_listener<UnixEchoServerHandler>({
        .name = "unix-echo",
        .endpoint = af::net::UnixEndpoint::unix_path(path.path()),
    });
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    ReactorUnixStreamClient<NetUnixRuntime> client;
    client.bind_threads(NetUnixRuntime::thread_group<NetUnixIoTag>());
    ASSERT_TRUE(client.connect<SendingUnixClientHandler>(
        {
            .name = "unix-client",
            .endpoint = af::net::UnixEndpoint::unix_path(path.path()),
            .connect_timeout = std::chrono::seconds(2),
        },
        SendingUnixClientHandler{state}));

    ASSERT_TRUE(wait_until([&] { return state->size.load(std::memory_order_acquire) == 4U; }));
    EXPECT_EQ(std::string(state->data.data(), state->size.load(std::memory_order_acquire)), "unix");
    EXPECT_EQ(state->errors.load(std::memory_order_acquire), 0);

    EXPECT_TRUE(client.stop());
    EXPECT_TRUE(server.stop());
    NetUnixRuntime::wait_for_idle();
    NetUnixRuntime::shutdown();
    EXPECT_NE(::access(path.path().c_str(), F_OK), 0);
}

TEST(NetUnixSocketTests, ExternalHandleSendIsQueuedToOwnerIoThread) {
    UnixPathGuard path(unique_unix_socket_path());
    auto state = std::make_shared<UnixClientState>();

    NetUnixRuntime::init();
    ReactorUnixStreamServer<NetUnixRuntime> server;
    server.bind_threads(NetUnixRuntime::thread_group<NetUnixIoTag>());
    const af::net::ListenerResult listener = server.add_listener<UnixEchoServerHandler>({
        .name = "unix-external-send-server",
        .endpoint = af::net::UnixEndpoint::unix_path(path.path()),
    });
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    ReactorUnixStreamClient<NetUnixRuntime> client;
    client.bind_threads(NetUnixRuntime::thread_group<NetUnixIoTag>());
    ASSERT_TRUE(client.connect<PassiveUnixClientHandler>(
        {
            .name = "unix-external-send-client",
            .endpoint = af::net::UnixEndpoint::unix_path(path.path()),
            .connect_timeout = std::chrono::seconds(2),
        },
        PassiveUnixClientHandler{state}));

    ASSERT_TRUE(wait_until([&] { return state->connected.load(std::memory_order_acquire); }));
    EXPECT_EQ(state->handle.send(af::Buffer::copy("queued", 6)), af::net::SendResult::Queued);
    ASSERT_TRUE(wait_until([&] { return state->size.load(std::memory_order_acquire) == 6U; }));
    EXPECT_EQ(std::string(state->data.data(), state->size.load(std::memory_order_acquire)),
              "queued");

    EXPECT_TRUE(client.stop());
    EXPECT_TRUE(server.stop());
    NetUnixRuntime::wait_for_idle();
    NetUnixRuntime::shutdown();
}

TEST(NetUnixSocketTests, ReportsConnectionErrors) {
    UnixPathGuard path(unique_unix_socket_path());
    auto state = std::make_shared<UnixClientState>();

    NetUnixRuntime::init();
    ReactorUnixStreamClient<NetUnixRuntime> client;
    client.bind_threads(NetUnixRuntime::thread_group<NetUnixIoTag>());
    ASSERT_TRUE(client.connect<ErrorUnixClientHandler>(
        {
            .name = "unix-missing-server",
            .endpoint = af::net::UnixEndpoint::unix_path(path.path()),
            .connect_timeout = std::chrono::seconds(2),
        },
        ErrorUnixClientHandler{state}));

    ASSERT_TRUE(wait_until([&] { return state->errors.load(std::memory_order_acquire) == 1; }));
    EXPECT_FALSE(state->connected.load(std::memory_order_acquire));

    EXPECT_TRUE(client.stop());
    NetUnixRuntime::wait_for_idle();
    NetUnixRuntime::shutdown();
}

TEST(NetUnixSocketTests, ListenerUnlinksStalePathBeforeBind) {
    UnixPathGuard path(unique_unix_socket_path());
    {
        std::ofstream stale(path.path());
        ASSERT_TRUE(stale.good());
        stale << "stale";
    }

    NetUnixRuntime::init();
    ReactorUnixStreamServer<NetUnixRuntime> server;
    server.bind_threads(NetUnixRuntime::thread_group<NetUnixIoTag>());
    const af::net::ListenerResult listener = server.add_listener<UnixEchoServerHandler>({
        .name = "unix-stale-path",
        .endpoint = af::net::UnixEndpoint::unix_path(path.path()),
    });
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    EXPECT_TRUE(server.stop());
    NetUnixRuntime::wait_for_idle();
    NetUnixRuntime::shutdown();
    EXPECT_NE(::access(path.path().c_str(), F_OK), 0);
}

TEST(NetUnixSocketTests, StreamWrapperNormalizesReusePortForMultiThreadUnixListener) {
    UnixPathGuard path(unique_unix_socket_path());
    af::net::TcpListenerOptions options;
    options.reuse_port = true;

    NetUnixMultiRuntime::init();
    ReactorUnixStreamServer<NetUnixMultiRuntime> server(af::net::UnixStreamRuntimeConfig{});
    const af::net::ListenerResult listener = server.add_listener<UnixMultiNoopHandler>({
        .name = "unix-multi-listener",
        .endpoint = af::net::UnixEndpoint::unix_path(path.path()),
        .threads = {NetUnixMultiRuntime::thread_group<NetUnixMultiIoTag>().template at<0>(),
                    NetUnixMultiRuntime::thread_group<NetUnixMultiIoTag>().template at<1>()},
        .options = options,
    });
    ASSERT_TRUE(listener.ok()) << listener.error;
    EXPECT_TRUE(server.start());
    EXPECT_TRUE(server.stop());
    NetUnixMultiRuntime::wait_for_idle();
    NetUnixMultiRuntime::shutdown();
    EXPECT_NE(::access(path.path().c_str(), F_OK), 0);
}

TEST(NetUnixSocketTests, DatagramServerAndClientEchoOverUnixSocket) {
    UnixPathGuard server_path(unique_unix_socket_path());
    UnixPathGuard client_path(unique_unix_socket_path());
    UnixDatagramState state;

    NetUnixRuntime::init();
    af::net::UnixDatagramSocket<NetUnixRuntime> server;
    server.bind_threads(NetUnixRuntime::thread_group<NetUnixIoTag>());
    ASSERT_TRUE(server.bind<UnixDatagramEchoHandler>({
        .name = "unix-datagram-echo",
        .local_endpoint = af::net::UnixEndpoint::unix_path(server_path.path()),
    }));
    NetUnixRuntime::wait_for_idle();

    af::net::UnixDatagramSocket<NetUnixRuntime> client;
    client.bind_threads(NetUnixRuntime::thread_group<NetUnixIoTag>());
    ASSERT_TRUE(client.connect<UnixDatagramCaptureHandler>(
        {
            .name = "unix-datagram-client",
            .local_endpoint = af::net::UnixEndpoint::unix_path(client_path.path()),
            .remote_endpoint = af::net::UnixEndpoint::unix_path(server_path.path()),
        },
        UnixDatagramCaptureHandler{&state}));
    NetUnixRuntime::wait_for_idle();

    EXPECT_EQ(client.handle().send(af::Buffer::copy("unix-dgram", 10)),
              af::net::UdpSendResult::Queued);
    ASSERT_TRUE(wait_until([&] { return state.size.load(std::memory_order_acquire) == 10U; }));
    EXPECT_EQ(std::string(state.data.data(), state.size.load(std::memory_order_acquire)),
              "unix-dgram");
    EXPECT_EQ(state.errors.load(std::memory_order_acquire), 0);

    EXPECT_TRUE(client.stop());
    EXPECT_TRUE(server.stop());
    NetUnixRuntime::wait_for_idle();
    NetUnixRuntime::shutdown();
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

    NetUnixRuntime::init();
    af::net::UnixDatagramSocket<NetUnixRuntime> socket;
    socket.bind_threads(NetUnixRuntime::thread_group<NetUnixIoTag>());
    ASSERT_TRUE(socket.bind<UnixDatagramNoopHandler>({
        .name = "unix-datagram-stale-path",
        .local_endpoint = af::net::UnixEndpoint::unix_path(path.path()),
    }));

    EXPECT_TRUE(socket.stop());
    NetUnixRuntime::wait_for_idle();
    NetUnixRuntime::shutdown();
    EXPECT_NE(::access(path.path().c_str(), F_OK), 0);
}
