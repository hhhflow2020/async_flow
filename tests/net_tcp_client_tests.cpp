#include <array>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "af/async_runtime.hpp"
#include "af/net.hpp"
#include "af/platform.hpp"

#include "gtest/gtest.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

struct NetTcpClientIoTag;

struct NetTcpClientRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<NetTcpClientIoTag, 1, af::preferred_io_thread_kind>("net-client-io"));
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using NetTcpClientRuntime = af::AsyncRuntime<NetTcpClientRuntimeTraits>;

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

struct ReactorCallState {
    std::atomic<bool> done{false};
    std::atomic<bool> ok{false};
};

class ReactorCallTask final : public NetTcpClientRuntime::Task {
public:
    explicit ReactorCallTask(NetTcpClientRuntime::Task::FactoryToken token)
        : NetTcpClientRuntime::Task(token) {}

    bool do_it(std::function<bool()> fn, std::shared_ptr<ReactorCallState> state) {
        fn_ = std::move(fn);
        state_ = std::move(state);
        return schedule(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>().template at<0>());
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
        return done();
    }

    std::function<bool()> fn_;
    std::shared_ptr<ReactorCallState> state_;
};

template <typename Fn> [[nodiscard]] bool call_on_reactor(Fn &&fn) {
    auto state = std::make_shared<ReactorCallState>();
    std::function<bool()> wrapped(std::forward<Fn>(fn));
    if (!NetTcpClientRuntime::start_task<ReactorCallTask>(std::move(wrapped), state)) {
        return false;
    }
    if (!wait_until([&] { return state->done.load(std::memory_order_acquire); })) {
        return false;
    }
    return state->ok.load(std::memory_order_acquire);
}

class ReactorTcpServer {
public:
    using ListenerConfig = typename af::net::TcpServer<NetTcpClientRuntime>::ListenerConfig;

    ReactorTcpServer &bind_threads(std::vector<NetTcpClientRuntime::Thread> threads) {
        static_cast<void>(call_on_reactor([this, threads = std::move(threads)]() mutable {
            server_.bind_threads(std::move(threads));
            return true;
        }));
        return *this;
    }

    template <typename Group> ReactorTcpServer &bind_threads(Group) {
        static_cast<void>(call_on_reactor([this] {
            server_.bind_threads(Group{});
            return true;
        }));
        return *this;
    }

    template <typename Handler>
    [[nodiscard]] af::net::ListenerResult add_listener(ListenerConfig config,
                                                       Handler handler = Handler{}) {
        af::net::ListenerResult result = af::net::ListenerResult::failure(EIO);
        const bool called = call_on_reactor([this, config = std::move(config),
                                             handler = std::move(handler), &result]() mutable {
            result = server_.template add_listener<Handler>(std::move(config), std::move(handler));
            return true;
        });
        return called ? result : af::net::ListenerResult::failure(EIO);
    }

    [[nodiscard]] bool start() {
        return call_on_reactor([this] { return server_.start(); });
    }

    [[nodiscard]] bool stop() {
        return call_on_reactor([this] { return server_.stop(); });
    }

private:
    af::net::TcpServer<NetTcpClientRuntime> server_;
};

class ReactorTcpClient {
public:
    using ConnectConfig = typename af::net::TcpClient<NetTcpClientRuntime>::ConnectConfig;

    ReactorTcpClient &bind_threads(std::vector<NetTcpClientRuntime::Thread> threads) {
        static_cast<void>(call_on_reactor([this, threads = std::move(threads)]() mutable {
            client_.bind_threads(std::move(threads));
            return true;
        }));
        return *this;
    }

    template <typename Group> ReactorTcpClient &bind_threads(Group) {
        static_cast<void>(call_on_reactor([this] {
            client_.bind_threads(Group{});
            return true;
        }));
        return *this;
    }

    template <typename Handler>
    [[nodiscard]] bool connect(ConnectConfig config, Handler handler = Handler{}) {
        return call_on_reactor(
            [this, config = std::move(config), handler = std::move(handler)]() mutable {
                return client_.template connect<Handler>(std::move(config), std::move(handler));
            });
    }

    [[nodiscard]] bool stop() {
        return call_on_reactor([this] { return client_.stop(); });
    }

private:
    af::net::TcpClient<NetTcpClientRuntime> client_;
};

struct TcpClientEchoServerHandler {
    void on_read(af::net::TcpConnectionRef<NetTcpClientRuntime> conn,
                 af::BufferView bytes) noexcept {
        static_cast<void>(conn.send(bytes));
    }
};

struct TcpClientState {
    std::atomic<bool> connected{false};
    std::atomic<int> errors{0};
    std::atomic<int> last_error{0};
    std::array<char, 64> data{};
    std::atomic<std::size_t> size{0};
    af::net::TcpConnectionHandle<NetTcpClientRuntime> handle;
};

struct SendingClientHandler {
    std::shared_ptr<TcpClientState> state;

    void on_connect(af::net::TcpConnectionRef<NetTcpClientRuntime> conn) noexcept {
        if (state != nullptr) {
            state->handle = conn.handle();
            state->connected.store(true, std::memory_order_release);
        }
        static_cast<void>(conn.send(af::Buffer::copy("hello", 5)));
    }

    void on_read(af::net::TcpConnectionRef<NetTcpClientRuntime> conn,
                 af::BufferView bytes) noexcept {
        if (state == nullptr) {
            return;
        }
        const std::size_t size = std::min(bytes.size(), state->data.size());
        std::memcpy(state->data.data(), bytes.data(), size);
        state->size.store(size, std::memory_order_release);
        conn.close();
    }

    void on_connect_error(int error) noexcept {
        if (state != nullptr) {
            state->last_error.store(error, std::memory_order_release);
            state->errors.fetch_add(1, std::memory_order_release);
        }
    }
};

struct PassiveClientHandler {
    std::shared_ptr<TcpClientState> state;

    void on_connect(af::net::TcpConnectionRef<NetTcpClientRuntime> conn) noexcept {
        if (state != nullptr) {
            state->handle = conn.handle();
            state->connected.store(true, std::memory_order_release);
        }
    }

    void on_read(af::net::TcpConnectionRef<NetTcpClientRuntime> conn,
                 af::BufferView bytes) noexcept {
        if (state == nullptr) {
            return;
        }
        const std::size_t size = std::min(bytes.size(), state->data.size());
        std::memcpy(state->data.data(), bytes.data(), size);
        state->size.store(size, std::memory_order_release);
        conn.close();
    }
};

struct ErrorClientHandler {
    std::shared_ptr<TcpClientState> state;

    void on_connect(af::net::TcpConnectionRef<NetTcpClientRuntime>) noexcept {
        if (state != nullptr) {
            state->connected.store(true, std::memory_order_release);
        }
    }

    void on_connect_error(int error) noexcept {
        if (state != nullptr) {
            state->last_error.store(error, std::memory_order_release);
            state->errors.fetch_add(1, std::memory_order_release);
        }
    }
};

} // namespace

TEST(NetTcpClientTests, ConnectsAndReceivesEcho) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<TcpClientState>();

    NetTcpClientRuntime::init();
    ReactorTcpServer server;
    server.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    const af::net::ListenerResult listener = server.add_listener<TcpClientEchoServerHandler>({
        .name = "tcp-client-test-server",
        .endpoint = af::net::TcpEndpoint::loopback(port),
        .options = {.reuse_port = false},
    });
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    ReactorTcpClient client;
    client.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    ASSERT_TRUE(client.connect<SendingClientHandler>(
        {
            .name = "tcp-client",
            .remote_endpoint = af::net::TcpEndpoint::loopback(port),
            .connect_timeout = std::chrono::seconds(2),
        },
        SendingClientHandler{state}));

    ASSERT_TRUE(wait_until([&] { return state->size.load(std::memory_order_acquire) == 5U; }));
    EXPECT_EQ(std::string(state->data.data(), state->size.load(std::memory_order_acquire)),
              "hello");
    EXPECT_EQ(state->errors.load(std::memory_order_acquire), 0);

    EXPECT_TRUE(client.stop());
    EXPECT_TRUE(server.stop());
    NetTcpClientRuntime::wait_for_idle();
    NetTcpClientRuntime::shutdown();
}

TEST(NetTcpClientTests, ConnectsOverIpv6Loopback) {
    std::uint16_t port = 0;
    if (!reserve_loopback_v6_port(port)) {
        GTEST_SKIP() << "IPv6 loopback is not available";
    }
    auto state = std::make_shared<TcpClientState>();

    NetTcpClientRuntime::init();
    ReactorTcpServer server;
    server.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    const af::net::ListenerResult listener = server.add_listener<TcpClientEchoServerHandler>({
        .name = "tcp-client-v6-server",
        .endpoint = af::net::TcpEndpoint::loopback_v6(port),
        .options = {.reuse_port = false},
    });
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    ReactorTcpClient client;
    client.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    ASSERT_TRUE(client.connect<SendingClientHandler>(
        {
            .name = "tcp-client-v6",
            .remote_endpoint = af::net::TcpEndpoint::loopback_v6(port),
            .connect_timeout = std::chrono::seconds(2),
        },
        SendingClientHandler{state}));

    ASSERT_TRUE(wait_until([&] { return state->size.load(std::memory_order_acquire) == 5U; }));
    EXPECT_EQ(std::string(state->data.data(), state->size.load(std::memory_order_acquire)),
              "hello");
    EXPECT_EQ(state->errors.load(std::memory_order_acquire), 0);

    EXPECT_TRUE(client.stop());
    EXPECT_TRUE(server.stop());
    NetTcpClientRuntime::wait_for_idle();
    NetTcpClientRuntime::shutdown();
}

TEST(NetTcpClientTests, ExternalHandleSendIsQueuedToOwnerIoThread) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<TcpClientState>();

    NetTcpClientRuntime::init();
    ReactorTcpServer server;
    server.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    const af::net::ListenerResult listener = server.add_listener<TcpClientEchoServerHandler>({
        .name = "tcp-client-external-send-server",
        .endpoint = af::net::TcpEndpoint::loopback(port),
        .options = {.reuse_port = false},
    });
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    ReactorTcpClient client;
    client.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    ASSERT_TRUE(client.connect<PassiveClientHandler>(
        {
            .name = "tcp-client-external-send",
            .remote_endpoint = af::net::TcpEndpoint::loopback(port),
            .connect_timeout = std::chrono::seconds(2),
        },
        PassiveClientHandler{state}));

    ASSERT_TRUE(wait_until([&] { return state->connected.load(std::memory_order_acquire); }));
    EXPECT_EQ(state->handle.send(af::Buffer::copy("queued", 6)), af::net::SendResult::Queued);
    ASSERT_TRUE(wait_until([&] { return state->size.load(std::memory_order_acquire) == 6U; }));
    EXPECT_EQ(std::string(state->data.data(), state->size.load(std::memory_order_acquire)),
              "queued");

    EXPECT_TRUE(client.stop());
    EXPECT_TRUE(server.stop());
    NetTcpClientRuntime::wait_for_idle();
    NetTcpClientRuntime::shutdown();
}

TEST(NetTcpClientTests, StopRejectsWritesFromOldHandles) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<TcpClientState>();

    NetTcpClientRuntime::init();
    ReactorTcpServer server;
    server.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    const af::net::ListenerResult listener = server.add_listener<TcpClientEchoServerHandler>({
        .name = "tcp-client-stop-old-handle-server",
        .endpoint = af::net::TcpEndpoint::loopback(port),
        .options = {.reuse_port = false},
    });
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    ReactorTcpClient client;
    client.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    ASSERT_TRUE(client.connect<PassiveClientHandler>(
        {
            .name = "tcp-client-stop-old-handle",
            .remote_endpoint = af::net::TcpEndpoint::loopback(port),
            .connect_timeout = std::chrono::seconds(2),
        },
        PassiveClientHandler{state}));

    ASSERT_TRUE(wait_until([&] { return state->connected.load(std::memory_order_acquire); }));
    EXPECT_TRUE(client.stop());
    EXPECT_EQ(state->handle.send(af::Buffer::copy("late", 4)), af::net::SendResult::Closed);

    EXPECT_TRUE(server.stop());
    NetTcpClientRuntime::wait_for_idle();
    NetTcpClientRuntime::shutdown();
}

TEST(NetTcpClientTests, BindLocalEndpointConnectsWhenFamilyMatches) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<TcpClientState>();

    NetTcpClientRuntime::init();
    ReactorTcpServer server;
    server.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    const af::net::ListenerResult listener = server.add_listener<TcpClientEchoServerHandler>({
        .name = "tcp-client-bind-local-server",
        .endpoint = af::net::TcpEndpoint::loopback(port),
        .options = {.reuse_port = false},
    });
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    ReactorTcpClient client;
    client.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    ASSERT_TRUE(client.connect<SendingClientHandler>(
        {
            .name = "tcp-client-bind-local",
            .remote_endpoint = af::net::TcpEndpoint::loopback(port),
            .local_endpoint = af::net::TcpEndpoint::loopback(0),
            .bind_local = true,
            .connect_timeout = std::chrono::seconds(2),
        },
        SendingClientHandler{state}));

    ASSERT_TRUE(wait_until([&] { return state->size.load(std::memory_order_acquire) == 5U; }));
    EXPECT_EQ(std::string(state->data.data(), state->size.load(std::memory_order_acquire)),
              "hello");

    EXPECT_TRUE(client.stop());
    EXPECT_TRUE(server.stop());
    NetTcpClientRuntime::wait_for_idle();
    NetTcpClientRuntime::shutdown();
}

TEST(NetTcpClientTests, RejectsBindLocalAddressFamilyMismatchSynchronously) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<TcpClientState>();

    NetTcpClientRuntime::init();
    ReactorTcpClient client;
    client.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());

    EXPECT_FALSE(client.connect<ErrorClientHandler>(
        {
            .name = "tcp-client-bind-local-family-mismatch",
            .remote_endpoint = af::net::TcpEndpoint::loopback(port),
            .local_endpoint = af::net::TcpEndpoint::loopback_v6(0),
            .bind_local = true,
            .connect_timeout = std::chrono::seconds(2),
        },
        ErrorClientHandler{state}));
    EXPECT_EQ(state->errors.load(std::memory_order_acquire), 0);

    EXPECT_TRUE(client.stop());
    NetTcpClientRuntime::wait_for_idle();
    NetTcpClientRuntime::shutdown();
}

TEST(NetTcpClientTests, ReportsConnectionErrors) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<TcpClientState>();

    NetTcpClientRuntime::init();
    ReactorTcpClient client;
    client.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    ASSERT_TRUE(client.connect<ErrorClientHandler>(
        {
            .name = "tcp-client-error",
            .remote_endpoint = af::net::TcpEndpoint::loopback(port),
            .connect_timeout = std::chrono::seconds(2),
        },
        ErrorClientHandler{state}));

    ASSERT_TRUE(wait_until([&] { return state->errors.load(std::memory_order_acquire) == 1; }));
    EXPECT_FALSE(state->connected.load(std::memory_order_acquire));

    EXPECT_TRUE(client.stop());
    NetTcpClientRuntime::wait_for_idle();
    NetTcpClientRuntime::shutdown();
}

TEST(NetTcpClientTests, ConnectAfterStopRestartsClientControlPlane) {
    const std::uint16_t port = reserve_loopback_port();
    auto first_state = std::make_shared<TcpClientState>();
    auto second_state = std::make_shared<TcpClientState>();

    NetTcpClientRuntime::init();
    ReactorTcpServer server;
    server.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    const af::net::ListenerResult listener = server.add_listener<TcpClientEchoServerHandler>({
        .name = "tcp-client-restart-server",
        .endpoint = af::net::TcpEndpoint::loopback(port),
        .options = {.reuse_port = false},
    });
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    ReactorTcpClient client;
    client.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    ASSERT_TRUE(client.connect<SendingClientHandler>(
        {
            .name = "tcp-client-restart-first",
            .remote_endpoint = af::net::TcpEndpoint::loopback(port),
            .connect_timeout = std::chrono::seconds(2),
        },
        SendingClientHandler{first_state}));
    ASSERT_TRUE(
        wait_until([&] { return first_state->size.load(std::memory_order_acquire) == 5U; }));
    EXPECT_TRUE(client.stop());
    NetTcpClientRuntime::wait_for_idle();

    ASSERT_TRUE(client.connect<SendingClientHandler>(
        {
            .name = "tcp-client-restart-second",
            .remote_endpoint = af::net::TcpEndpoint::loopback(port),
            .connect_timeout = std::chrono::seconds(2),
        },
        SendingClientHandler{second_state}));
    ASSERT_TRUE(
        wait_until([&] { return second_state->size.load(std::memory_order_acquire) == 5U; }));
    EXPECT_EQ(
        std::string(second_state->data.data(), second_state->size.load(std::memory_order_acquire)),
        "hello");

    EXPECT_TRUE(client.stop());
    EXPECT_TRUE(server.stop());
    NetTcpClientRuntime::wait_for_idle();
    NetTcpClientRuntime::shutdown();
}

TEST(NetTcpClientTests, StopReturnsWithoutWaitingForConnectTimeout) {
    auto state = std::make_shared<TcpClientState>();

    NetTcpClientRuntime::init();
    ReactorTcpClient client;
    client.bind_threads(NetTcpClientRuntime::thread_group<NetTcpClientIoTag>());
    ASSERT_TRUE(client.connect<ErrorClientHandler>(
        {
            .name = "tcp-client-stop-pending-connect",
            .remote_endpoint =
                af::net::TcpEndpoint::host("203.0.113.1", 9, af::net::AddressFamily::IPv4),
            .connect_timeout = std::chrono::seconds(3),
        },
        ErrorClientHandler{state}));

    const auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(client.stop());
    NetTcpClientRuntime::wait_for_idle();
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, std::chrono::seconds(1));

    NetTcpClientRuntime::shutdown();
}

TEST(NetTcpClientTests, StalePendingConnectCompletionDoesNotStopNewConnect) {
    using State = af::net::detail::TcpServerState<NetTcpClientRuntime>;
    using ClientControlState = af::net::detail::TcpClientControlState<NetTcpClientRuntime>;
    using ConnectControl = af::net::detail::TcpClientConnectControl<NetTcpClientRuntime>;

    auto state = std::make_shared<State>();
    auto client_control = std::make_shared<ClientControlState>();
    auto stale_connect =
        std::make_shared<ConnectControl>(0, NetTcpClientRuntime::thread_from_index(0));
    auto current_connect =
        std::make_shared<ConnectControl>(0, NetTcpClientRuntime::thread_from_index(0));

    client_control->pending_connects.push_back(stale_connect);
    client_control->inflight_connects = 1;
    state->running = true;

    client_control->pending_connects.clear();
    client_control->inflight_connects = 0;
    client_control->has_connected = false;
    state->running = false;

    client_control->pending_connects.push_back(current_connect);
    client_control->inflight_connects = 1;
    state->running = true;

    af::net::detail::handle_tcp_client_connect_result_on_control(state, client_control,
                                                                 stale_connect, false);
    EXPECT_TRUE(state->running);
    EXPECT_EQ(client_control->inflight_connects, 1U);
    ASSERT_EQ(client_control->pending_connects.size(), 1U);
    EXPECT_EQ(client_control->pending_connects.front().lock(), current_connect);

    af::net::detail::handle_tcp_client_connect_result_on_control(state, client_control,
                                                                 current_connect, false);
    EXPECT_FALSE(state->running);
    EXPECT_EQ(client_control->inflight_connects, 0U);
    EXPECT_TRUE(client_control->pending_connects.empty());
}
