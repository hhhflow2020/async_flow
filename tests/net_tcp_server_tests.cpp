#include <atomic>
#include <array>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
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

struct NetServerIoTag;
struct NetServerTwoIoTag;
struct NetServerIoWorkerIoTag;
struct NetServerIoWorkerCpuTag;

struct NetServerRuntimeTraits {
    static constexpr auto threads =
        af::thread_layout(af::thread_group<NetServerIoTag, 1, af::thread_kind::io>("net-test-io"));
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using NetServerRuntime = af::AsyncRuntime<NetServerRuntimeTraits>;

struct NetServerTwoIoRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<NetServerTwoIoTag, 2, af::thread_kind::io>("net-test-io"));
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using NetServerTwoIoRuntime = af::AsyncRuntime<NetServerTwoIoRuntimeTraits>;

struct NetServerIoWorkerRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<NetServerIoWorkerIoTag, 1, af::thread_kind::io>("net-test-io"),
        af::thread_group<NetServerIoWorkerCpuTag, 1, af::thread_kind::cpu>("net-test-cpu"));
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using NetServerIoWorkerRuntime = af::AsyncRuntime<NetServerIoWorkerRuntimeTraits>;

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

[[nodiscard]] UniqueFd connect_loopback(std::uint16_t port) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);

    for (int attempt = 0; attempt < 50; ++attempt) {
        UniqueFd fd(::socket(AF_INET, SOCK_STREAM, 0));
        if (fd.get() < 0) {
            return UniqueFd{};
        }
        if (::connect(fd.get(), reinterpret_cast<const sockaddr *>(&address), sizeof(address)) ==
            0) {
            return fd;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return UniqueFd{};
}

[[nodiscard]] UniqueFd connect_loopback_with_recv_buffer(std::uint16_t port, int recv_buffer_size) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);

    for (int attempt = 0; attempt < 50; ++attempt) {
        UniqueFd fd(::socket(AF_INET, SOCK_STREAM, 0));
        if (fd.get() < 0) {
            return UniqueFd{};
        }
        static_cast<void>(::setsockopt(fd.get(), SOL_SOCKET, SO_RCVBUF, &recv_buffer_size,
                                       sizeof(recv_buffer_size)));
        if (::connect(fd.get(), reinterpret_cast<const sockaddr *>(&address), sizeof(address)) ==
            0) {
            return fd;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return UniqueFd{};
}

[[nodiscard]] UniqueFd connect_loopback_v6(std::uint16_t port) {
    sockaddr_in6 address{};
    address.sin6_family = AF_INET6;
    address.sin6_addr = in6addr_loopback;
    address.sin6_port = htons(port);

    for (int attempt = 0; attempt < 50; ++attempt) {
        UniqueFd fd(::socket(AF_INET6, SOCK_STREAM, 0));
        if (fd.get() < 0) {
            return UniqueFd{};
        }
        if (::connect(fd.get(), reinterpret_cast<const sockaddr *>(&address), sizeof(address)) ==
            0) {
            return fd;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return UniqueFd{};
}

[[nodiscard]] std::string roundtrip(std::uint16_t port, std::string_view payload) {
    UniqueFd fd = connect_loopback(port);
    EXPECT_GE(fd.get(), 0);
    if (fd.get() < 0) {
        return {};
    }
    timeval timeout{};
    timeout.tv_sec = 2;
    static_cast<void>(::setsockopt(fd.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
    EXPECT_EQ(::send(fd.get(), payload.data(), payload.size(), 0),
              static_cast<ssize_t>(payload.size()));

    char buffer[128]{};
    const ssize_t n = ::recv(fd.get(), buffer, sizeof(buffer), 0);
    EXPECT_GT(n, 0);
    return n > 0 ? std::string(buffer, static_cast<std::size_t>(n)) : std::string{};
}

[[nodiscard]] std::string roundtrip_v6(std::uint16_t port, std::string_view payload) {
    UniqueFd fd = connect_loopback_v6(port);
    EXPECT_GE(fd.get(), 0);
    if (fd.get() < 0) {
        return {};
    }
    timeval timeout{};
    timeout.tv_sec = 2;
    static_cast<void>(::setsockopt(fd.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
    EXPECT_EQ(::send(fd.get(), payload.data(), payload.size(), 0),
              static_cast<ssize_t>(payload.size()));

    char buffer[128]{};
    const ssize_t n = ::recv(fd.get(), buffer, sizeof(buffer), 0);
    EXPECT_GT(n, 0);
    return n > 0 ? std::string(buffer, static_cast<std::size_t>(n)) : std::string{};
}

void send_all(int fd, const void *data, std::size_t size) {
    const auto *bytes = static_cast<const std::byte *>(data);
    std::size_t sent = 0;
    while (sent < size) {
        const ssize_t n = ::send(fd, bytes + sent, size - sent, 0);
        ASSERT_GT(n, 0);
        sent += static_cast<std::size_t>(n);
    }
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

struct RuntimeTcpListenerState {
    std::atomic<bool> started{false};
    std::atomic<bool> start_ok{false};
    std::atomic<bool> stopped{false};
    std::atomic<bool> stop_ok{false};
    std::atomic<int> accepted{0};
    std::atomic<int> errors{0};
    std::atomic<int> last_error{0};
    std::atomic<std::uint16_t> port{0};
};

void runtime_tcp_listener_accept(void *owner, af::net::tcp_listener &listener, int fd,
                                 const sockaddr *peer, socklen_t peer_size) noexcept {
    static_cast<void>(listener);
    static_cast<void>(peer);
    static_cast<void>(peer_size);
    auto *state = static_cast<RuntimeTcpListenerState *>(owner);
    if (fd >= 0) {
        ::close(fd);
    }
    if (state != nullptr) {
        state->accepted.fetch_add(1, std::memory_order_release);
    }
}

void runtime_tcp_listener_error(void *owner, af::net::tcp_listener &listener, int error) noexcept {
    static_cast<void>(listener);
    auto *state = static_cast<RuntimeTcpListenerState *>(owner);
    if (state != nullptr) {
        state->last_error.store(error, std::memory_order_release);
        state->errors.fetch_add(1, std::memory_order_release);
    }
}

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

template <typename Runtime> class ReactorTcpServer {
public:
    using Thread = typename Runtime::Thread;
    using ListenerConfig = typename af::net::TcpServer<Runtime>::ListenerConfig;

    ReactorTcpServer() : control_thread_(first_reactor_thread<Runtime>()) {}

    explicit ReactorTcpServer(af::net::TcpServerConfig config)
        : server_(config), control_thread_(first_reactor_thread<Runtime>()) {}

    ReactorTcpServer &bind_threads(std::vector<Thread> threads) {
        static_cast<void>(call_on_reactor<Runtime>(control_thread_,
                                                   [this, threads = std::move(threads)]() mutable {
                                                       server_.bind_threads(std::move(threads));
                                                       return true;
                                                   }));
        return *this;
    }

    template <typename Group> ReactorTcpServer &bind_threads(Group) {
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

    [[nodiscard]] bool remove_listener(
        af::net::TcpListenerHandle listener,
        af::net::RemoveListenerPolicy policy = af::net::RemoveListenerPolicy::StopAcceptOnly) {
        return call_on_reactor<Runtime>(control_thread_, [this, listener, policy] {
            return server_.remove_listener(listener, policy);
        });
    }

    [[nodiscard]] af::net::TcpServer<Runtime> &raw() noexcept {
        return server_;
    }

private:
    af::net::TcpServer<Runtime> server_;
    Thread control_thread_{};
};

struct PrefixAHandler {
    void on_read(af::net::TcpConnectionRef<NetServerRuntime> conn, af::BufferView bytes) noexcept {
        std::string response = "A:";
        response.append(bytes.string_view());
        static_cast<void>(conn.send(af::Buffer::copy(response.data(), response.size())));
    }
};

struct PrefixBHandler {
    void on_read(af::net::TcpConnectionRef<NetServerRuntime> conn, af::BufferView bytes) noexcept {
        std::string response = "B:";
        response.append(bytes.string_view());
        static_cast<void>(conn.send(af::Buffer::copy(response.data(), response.size())));
    }
};

struct EchoHandler {
    void on_read(af::net::TcpConnectionRef<NetServerRuntime> conn, af::BufferView bytes) noexcept {
        static_cast<void>(conn.send(bytes));
    }
};

struct ListenerErrorState {
    std::atomic<int> errors{0};
    std::atomic<int> last_error{0};
};

struct ErrorEchoHandler {
    std::shared_ptr<ListenerErrorState> state;

    void on_read(af::net::TcpConnectionRef<NetServerRuntime> conn, af::BufferView bytes) noexcept {
        static_cast<void>(conn.send(bytes));
    }

    void on_error(af::net::TcpListenerHandle, int error) noexcept {
        if (state != nullptr) {
            state->last_error.store(error, std::memory_order_release);
            state->errors.fetch_add(1, std::memory_order_release);
        }
    }
};

struct EmptyThenEchoHandler {
    void on_read(af::net::TcpConnectionRef<NetServerRuntime> conn, af::BufferView bytes) noexcept {
        static_cast<void>(conn.send(af::Buffer{}));
        static_cast<void>(conn.send(bytes));
    }
};

struct MultiBufferWriteHandler {
    void on_read(af::net::TcpConnectionRef<NetServerRuntime> conn, af::BufferView bytes) noexcept {
        af::Buffer header = af::Buffer::with_capacity(16);
        static_cast<void>(header.try_append("header:", 7));
        static_cast<void>(conn.send(std::move(header)));
        static_cast<void>(conn.send(bytes));
        static_cast<void>(conn.send(af::Buffer::copy(":tail", 5)));
        conn.close_after_flush();
    }
};

struct PauseReadState {
    std::atomic<int> reads{0};
};

struct PauseAfterFirstReadHandler {
    std::shared_ptr<PauseReadState> state;

    void on_read(af::net::TcpConnectionRef<NetServerRuntime> conn, af::BufferView) noexcept {
        if (state != nullptr) {
            state->reads.fetch_add(1, std::memory_order_release);
        }
        conn.pause_read();
    }
};

struct ThrowingReadState {
    std::atomic<int> close_count{0};
    std::atomic<int> close_reason{-1};
};

struct ThrowingReadHandler {
    std::shared_ptr<ThrowingReadState> state;

    void on_read(af::net::TcpConnectionRef<NetServerRuntime>, af::BufferView) {
        throw std::runtime_error("net test read failure");
    }

    void on_close(af::net::TcpConnectionHandle<NetServerRuntime>,
                  af::net::CloseReason reason) noexcept {
        if (state != nullptr) {
            state->close_reason.store(static_cast<int>(reason), std::memory_order_release);
            state->close_count.fetch_add(1, std::memory_order_release);
        }
    }
};

struct ThrowOnCopyState {
    std::atomic<bool> throw_on_copy{false};
};

struct ThrowOnCopyHandler {
    std::shared_ptr<ThrowOnCopyState> state;

    ThrowOnCopyHandler() = default;
    explicit ThrowOnCopyHandler(std::shared_ptr<ThrowOnCopyState> state_in)
        : state(std::move(state_in)) {}

    ThrowOnCopyHandler(const ThrowOnCopyHandler &other) : state(other.state) {
        if (state != nullptr && state->throw_on_copy.load(std::memory_order_acquire)) {
            throw std::runtime_error("net test clone failure");
        }
    }

    ThrowOnCopyHandler &operator=(const ThrowOnCopyHandler &) = default;
    ThrowOnCopyHandler(ThrowOnCopyHandler &&) noexcept = default;
    ThrowOnCopyHandler &operator=(ThrowOnCopyHandler &&) noexcept = default;

    void on_read(af::net::TcpConnectionRef<NetServerRuntime> conn, af::BufferView bytes) noexcept {
        static_cast<void>(conn.send(bytes));
    }
};

struct WorkerSendState {
    std::atomic<int> result{-1};
};

struct OwnerHandleControlState {
    std::atomic<int> successful_ops{0};
    std::atomic<int> send_result{-1};
    std::atomic<int> close_count{0};
};

class WorkerSendTask final : public NetServerIoWorkerRuntime::Task {
public:
    explicit WorkerSendTask(NetServerIoWorkerRuntime::Task::FactoryToken token)
        : NetServerIoWorkerRuntime::Task(token) {}

    bool do_it(af::net::TcpConnectionHandle<NetServerIoWorkerRuntime> conn,
               std::shared_ptr<WorkerSendState> state) {
        conn_ = std::move(conn);
        state_ = std::move(state);
        return schedule(
            NetServerIoWorkerRuntime::thread_group<NetServerIoWorkerCpuTag>().template at<0>());
    }

private:
    af::TaskResult run() override {
        const af::net::SendResult result = conn_.send(af::Buffer::copy("worker", 6));
        if (state_ != nullptr) {
            state_->result.store(static_cast<int>(result), std::memory_order_release);
        }
        return done();
    }

    af::net::TcpConnectionHandle<NetServerIoWorkerRuntime> conn_;
    std::shared_ptr<WorkerSendState> state_;
};

struct WorkerSendOnAcceptHandler {
    std::shared_ptr<WorkerSendState> state;

    void on_accept(af::net::TcpConnectionRef<NetServerIoWorkerRuntime> conn) noexcept {
        static_cast<void>(
            NetServerIoWorkerRuntime::start_task<WorkerSendTask>(conn.handle(), state));
    }
};

struct OwnerHandleControlHandler {
    std::shared_ptr<OwnerHandleControlState> state;

    void on_read(af::net::TcpConnectionRef<NetServerRuntime> conn, af::BufferView bytes) noexcept {
        const af::net::TcpConnectionHandle<NetServerRuntime> handle = conn.handle();
        int successful = 0;
        successful += handle.pause_read() ? 1 : 0;
        successful += handle.resume_read() ? 1 : 0;
        successful += handle.set_no_delay(true) ? 1 : 0;
        successful += handle.set_keepalive(false) ? 1 : 0;
        const af::net::SendResult sent = handle.send(bytes);
        successful += sent == af::net::SendResult::Accepted ? 1 : 0;
        successful += handle.close_after_flush() ? 1 : 0;
        if (state != nullptr) {
            state->send_result.store(static_cast<int>(sent), std::memory_order_release);
            state->successful_ops.store(successful, std::memory_order_release);
        }
    }

    void on_close(af::net::TcpConnectionHandle<NetServerRuntime>, af::net::CloseReason) noexcept {
        if (state != nullptr) {
            state->close_count.fetch_add(1, std::memory_order_release);
        }
    }
};

struct SlotReuseState {
    SlotReuseState() {
        for (auto &published_value : published) {
            published_value.store(false, std::memory_order_relaxed);
        }
        for (auto &slot : slots) {
            slot.store(0, std::memory_order_relaxed);
        }
        for (auto &generation : generations) {
            generation.store(0, std::memory_order_relaxed);
        }
    }

    std::atomic<int> next_accept{0};
    std::atomic<int> close_count{0};
    std::atomic<int> stale_send_result{-1};
    std::array<std::atomic<bool>, 2> published;
    std::array<std::atomic<std::uint32_t>, 2> slots;
    std::array<std::atomic<std::uint32_t>, 2> generations;
    std::array<af::net::TcpConnectionHandle<NetServerRuntime>, 2> handles;
};

struct CapturedHandleState {
    std::atomic<bool> published{false};
    std::atomic<int> close_count{0};
    af::net::TcpConnectionHandle<NetServerRuntime> handle;
};

struct StopTimeoutState {
    std::atomic<bool> accepted{false};
    std::atomic<int> send_result{-1};
    std::atomic<int> close_count{0};
    std::atomic<int> close_reason{-1};
};

struct FillSendQueueOnAcceptHandler {
    std::shared_ptr<StopTimeoutState> state;

    void on_accept(af::net::TcpConnectionRef<NetServerRuntime> conn) noexcept {
        if (state != nullptr) {
            state->accepted.store(true, std::memory_order_release);
        }

        std::array<char, 16U * 1024U> payload{};
        payload.fill('x');
        af::net::SendResult last_result = af::net::SendResult::Accepted;
        for (int i = 0; i < 4096; ++i) {
            last_result = conn.send(af::BufferView(payload.data(), payload.size()));
            if (last_result == af::net::SendResult::Backpressure ||
                last_result == af::net::SendResult::Closed) {
                break;
            }
        }
        if (state != nullptr) {
            state->send_result.store(static_cast<int>(last_result), std::memory_order_release);
        }
    }

    void on_close(af::net::TcpConnectionHandle<NetServerRuntime>,
                  af::net::CloseReason reason) noexcept {
        if (state != nullptr) {
            state->close_reason.store(static_cast<int>(reason), std::memory_order_release);
            state->close_count.fetch_add(1, std::memory_order_release);
        }
    }
};

struct CloseCallbackHandleState {
    std::atomic<int> send_result{-1};
    std::atomic<int> close_after_flush_result{-1};
    std::atomic<int> close_count{0};
};

struct CaptureHandleHandler {
    std::shared_ptr<CapturedHandleState> state;

    void on_accept(af::net::TcpConnectionRef<NetServerRuntime> conn) noexcept {
        if (state == nullptr) {
            return;
        }
        state->handle = conn.handle();
        state->published.store(true, std::memory_order_release);
    }

    void on_close(af::net::TcpConnectionHandle<NetServerRuntime>, af::net::CloseReason) noexcept {
        if (state != nullptr) {
            state->close_count.fetch_add(1, std::memory_order_release);
        }
    }
};

struct CloseCallbackUsesHandleHandler {
    std::shared_ptr<CloseCallbackHandleState> state;

    void on_close(af::net::TcpConnectionHandle<NetServerRuntime> conn,
                  af::net::CloseReason) noexcept {
        if (state == nullptr) {
            return;
        }
        const af::net::SendResult send_result = conn.send(af::Buffer::copy("late", 4));
        state->send_result.store(static_cast<int>(send_result), std::memory_order_release);
        state->close_after_flush_result.store(conn.close_after_flush() ? 1 : 0,
                                              std::memory_order_release);
        state->close_count.fetch_add(1, std::memory_order_release);
    }
};

struct ClosingEchoHandler {
    std::shared_ptr<SlotReuseState> state;

    void on_accept(af::net::TcpConnectionRef<NetServerRuntime> conn) noexcept {
        const int index = state == nullptr ? -1 : state->next_accept.fetch_add(1);
        if (index < 0 || index >= static_cast<int>(state->handles.size())) {
            return;
        }
        state->handles[static_cast<std::size_t>(index)] = conn.handle();
        state->slots[static_cast<std::size_t>(index)].store(conn.slot(), std::memory_order_relaxed);
        state->generations[static_cast<std::size_t>(index)].store(conn.generation(),
                                                                  std::memory_order_relaxed);
        if (index == 1) {
            const af::net::SendResult result = state->handles[0].send(af::Buffer::copy("stale", 5));
            state->stale_send_result.store(static_cast<int>(result), std::memory_order_release);
        }
        state->published[static_cast<std::size_t>(index)].store(true, std::memory_order_release);
    }

    void on_read(af::net::TcpConnectionRef<NetServerRuntime> conn, af::BufferView bytes) noexcept {
        static_cast<void>(conn.send(bytes));
        conn.close_after_flush();
    }

    void on_close(af::net::TcpConnectionHandle<NetServerRuntime>, af::net::CloseReason) noexcept {
        if (state != nullptr) {
            state->close_count.fetch_add(1, std::memory_order_release);
        }
    }
};

struct TwoIoCounters {
    TwoIoCounters() {
        for (auto &count : reads) {
            count.store(0, std::memory_order_relaxed);
        }
    }

    std::array<std::atomic<int>, NetServerTwoIoRuntime::thread_count> reads;
};

struct TwoIoEchoHandler {
    std::shared_ptr<TwoIoCounters> counters;

    void on_read(af::net::TcpConnectionRef<NetServerTwoIoRuntime> conn,
                 af::BufferView bytes) noexcept {
        const std::uint16_t thread = NetServerTwoIoRuntime::current_thread_index();
        if (counters != nullptr && thread < counters->reads.size()) {
            counters->reads[thread].fetch_add(1, std::memory_order_relaxed);
        }
        static_cast<void>(conn.send(bytes));
    }
};

} // namespace

TEST(NetTcpServerTests, RuntimeTcpListenerAcceptsOnRuntimeReactorThread) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_listener listener(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-listener";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.options.reuse_port = false;
        af::net::tcp_listener_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_tcp_listener_accept;
        callbacks.on_error = &runtime_tcp_listener_error;

        const bool ok = listener.start(io_thread, std::move(listener_config), callbacks);
        if (ok) {
            state.port.store(listener.local_endpoint().port, std::memory_order_release);
        }
        state.start_ok.store(ok, std::memory_order_release);
        state.started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return state.started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(state.start_ok.load(std::memory_order_acquire));
    ASSERT_GT(state.port.load(std::memory_order_acquire), 0U);

    UniqueFd client = connect_loopback(state.port.load(std::memory_order_acquire));
    ASSERT_GE(client.get(), 0);
    ASSERT_TRUE(wait_until([&] { return state.accepted.load(std::memory_order_acquire) == 1; }));
    EXPECT_EQ(state.errors.load(std::memory_order_acquire), 0);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        state.stop_ok.store(listener.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpListenerReportsEarlyConfigErrors) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_listener listener(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-listener-invalid";
        listener_config.endpoint = af::net::tcp_endpoint::unix_path("/tmp/af-invalid-listener");
        af::net::tcp_listener_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_error = &runtime_tcp_listener_error;

        const bool ok = listener.start(io_thread, std::move(listener_config), callbacks);
        state.start_ok.store(ok, std::memory_order_release);
        state.started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return state.started.load(std::memory_order_acquire); }));
    EXPECT_FALSE(state.start_ok.load(std::memory_order_acquire));
    EXPECT_EQ(state.errors.load(std::memory_order_acquire), 1);
    EXPECT_EQ(state.last_error.load(std::memory_order_acquire), EAFNOSUPPORT);
    EXPECT_FALSE(listener.started());

    runtime.stop();
}

TEST(NetTcpServerTests, SupportsMultipleListenersWithDifferentHandlers) {
    const std::uint16_t port_a = reserve_loopback_port();
    const std::uint16_t port_b = reserve_loopback_port();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener_a = server.add_listener<PrefixAHandler>({
        .name = "prefix-a",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port_a),
        .options = {.reuse_port = true},
    });
    const auto listener_b = server.add_listener<PrefixBHandler>({
        .name = "prefix-b",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port_b),
        .options = {.reuse_port = true},
    });

    ASSERT_TRUE(listener_a.ok()) << listener_a.error;
    ASSERT_TRUE(listener_b.ok()) << listener_b.error;
    ASSERT_TRUE(server.start());

    EXPECT_EQ(roundtrip(port_a, "one"), "A:one");
    EXPECT_EQ(roundtrip(port_b, "two"), "B:two");

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, SupportsIpv6LoopbackListener) {
    std::uint16_t port = 0;
    if (!reserve_loopback_v6_port(port)) {
        GTEST_SKIP() << "IPv6 loopback is not available";
    }

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());
    const af::net::ListenerResult result = server.add_listener<EchoHandler>({
        .name = "tcp-v6-echo",
        .endpoint = af::net::TcpEndpoint::loopback_v6(port),
        .options = {.reuse_port = false},
    });
    ASSERT_TRUE(result.ok()) << result.error;
    ASSERT_TRUE(server.start());

    EXPECT_EQ(roundtrip_v6(port, "ipv6"), "ipv6");

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, RejectsBusyLoopListenerOptions) {
    const std::uint16_t accept_budget_port = reserve_loopback_port();
    const std::uint16_t read_budget_port = reserve_loopback_port();
    const std::uint16_t write_budget_port = reserve_loopback_port();
    const std::uint16_t watermark_port = reserve_loopback_port();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto zero_accept_budget = server.add_listener<EchoHandler>({
        .name = "zero-accept-budget",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", accept_budget_port),
        .options = {.accept_budget = 0},
    });
    const auto zero_read_budget = server.add_listener<EchoHandler>({
        .name = "zero-read-budget",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", read_budget_port),
        .options = {.read_budget_bytes = 0},
    });
    const auto zero_output_watermark = server.add_listener<EchoHandler>({
        .name = "zero-output-watermark",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", watermark_port),
        .options = {.output_high_watermark = 0},
    });
    const auto zero_write_budget = server.add_listener<EchoHandler>({
        .name = "zero-write-budget",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", write_budget_port),
        .options = {.write_budget_bytes = 0},
    });

    EXPECT_FALSE(zero_accept_budget.ok());
    EXPECT_EQ(zero_accept_budget.error, EINVAL);
    EXPECT_FALSE(zero_read_budget.ok());
    EXPECT_EQ(zero_read_budget.error, EINVAL);
    EXPECT_FALSE(zero_output_watermark.ok());
    EXPECT_EQ(zero_output_watermark.error, EINVAL);
    EXPECT_FALSE(zero_write_budget.ok());
    EXPECT_EQ(zero_write_budget.error, EINVAL);

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, ListenerStartFailureIsReportedAsynchronously) {
    const std::uint16_t port = reserve_loopback_port();
    auto error_state = std::make_shared<ListenerErrorState>();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener_a = server.add_listener<EchoHandler>({
        .name = "rollback-a",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
        .options = {.reuse_port = false},
    });
    const auto listener_b = server.add_listener<ErrorEchoHandler>(
        {
            .name = "conflicting-b",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
            .options = {.reuse_port = false},
        },
        ErrorEchoHandler{error_state});

    ASSERT_TRUE(listener_a.ok()) << listener_a.error;
    ASSERT_TRUE(listener_b.ok()) << listener_b.error;
    EXPECT_TRUE(server.start());

    EXPECT_EQ(roundtrip(port, "still-active"), "still-active");
    ASSERT_TRUE(
        wait_until([&] { return error_state->errors.load(std::memory_order_acquire) >= 1; }));
    EXPECT_NE(error_state->last_error.load(std::memory_order_acquire), 0);
    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, EmptyWriteDoesNotBlockFollowingPayload) {
    const std::uint16_t port = reserve_loopback_port();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener = server.add_listener<EmptyThenEchoHandler>({
        .name = "empty-then-echo",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
        .options = {.reuse_port = true},
    });
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    EXPECT_EQ(roundtrip(port, "payload"), "payload");

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, MultiBufferWriteFlushesInOrder) {
    const std::uint16_t port = reserve_loopback_port();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener = server.add_listener<MultiBufferWriteHandler>({
        .name = "multi-buffer-write",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
        .options = {.reuse_port = true},
    });
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    UniqueFd fd = connect_loopback(port);
    ASSERT_GE(fd.get(), 0);
    timeval timeout{};
    timeout.tv_sec = 2;
    static_cast<void>(::setsockopt(fd.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
    send_all(fd.get(), "payload", 7);

    std::string received;
    std::array<char, 8> buffer{};
    for (;;) {
        const ssize_t n = ::recv(fd.get(), buffer.data(), buffer.size(), 0);
        if (n == 0) {
            break;
        }
        ASSERT_GT(n, 0);
        received.append(buffer.data(), static_cast<std::size_t>(n));
    }
    EXPECT_EQ(received, "header:payload:tail");

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, PauseReadStopsCurrentDrainLoop) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<PauseReadState>();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener = server.add_listener<PauseAfterFirstReadHandler>(
        {
            .name = "pause-read",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
            .options =
                {
                    .read_budget_bytes = 256U * 1024U,
                    .read_buffer_size = 1024U,
                },
        },
        PauseAfterFirstReadHandler{state});
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    UniqueFd fd = connect_loopback(port);
    ASSERT_GE(fd.get(), 0);
    const std::vector<std::byte> payload(128U * 1024U, std::byte{0x78});
    send_all(fd.get(), payload.data(), payload.size());

    ASSERT_TRUE(wait_until([&] { return state->reads.load(std::memory_order_acquire) >= 1; }));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(state->reads.load(std::memory_order_acquire), 1);

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, ThrowingReadHandlerClosesConnectionWithoutTerminating) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<ThrowingReadState>();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener = server.add_listener<ThrowingReadHandler>(
        {
            .name = "throwing-read",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
        },
        ThrowingReadHandler{state});
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    UniqueFd fd = connect_loopback(port);
    ASSERT_GE(fd.get(), 0);
    send_all(fd.get(), "boom", 4);

    ASSERT_TRUE(
        wait_until([&] { return state->close_count.load(std::memory_order_acquire) >= 1; }));
    EXPECT_EQ(state->close_reason.load(std::memory_order_acquire),
              static_cast<int>(af::net::CloseReason::Error));

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, CrossThreadHandleSendReportsQueued) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<WorkerSendState>();

    NetServerIoWorkerRuntime::init();
    ReactorTcpServer<NetServerIoWorkerRuntime> server;
    server.bind_threads(NetServerIoWorkerRuntime::thread_group<NetServerIoWorkerIoTag>());

    const auto listener = server.add_listener<WorkerSendOnAcceptHandler>(
        {
            .name = "worker-send",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
        },
        WorkerSendOnAcceptHandler{state});
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    UniqueFd fd = connect_loopback(port);
    ASSERT_GE(fd.get(), 0);
    timeval timeout{};
    timeout.tv_sec = 2;
    static_cast<void>(::setsockopt(fd.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
    char buffer[32]{};
    const ssize_t n = ::recv(fd.get(), buffer, sizeof(buffer), 0);
    ASSERT_EQ(n, 6);
    EXPECT_EQ(std::string(buffer, static_cast<std::size_t>(n)), "worker");
    EXPECT_EQ(state->result.load(std::memory_order_acquire),
              static_cast<int>(af::net::SendResult::Queued));

    EXPECT_TRUE(server.stop());
    NetServerIoWorkerRuntime::wait_for_idle();
    NetServerIoWorkerRuntime::shutdown();
}

TEST(NetTcpServerTests, OwnerThreadHandleControlRunsInline) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<OwnerHandleControlState>();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener = server.add_listener<OwnerHandleControlHandler>(
        {
            .name = "owner-handle-control",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
        },
        OwnerHandleControlHandler{state});
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    EXPECT_EQ(roundtrip(port, "owner"), "owner");
    ASSERT_TRUE(
        wait_until([&] { return state->successful_ops.load(std::memory_order_acquire) >= 6; }));
    EXPECT_EQ(state->successful_ops.load(std::memory_order_acquire), 6);
    EXPECT_EQ(state->send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::SendResult::Accepted));
    EXPECT_TRUE(
        wait_until([&] { return state->close_count.load(std::memory_order_acquire) >= 1; }));

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, CloseCallbackCanSafelyUseConnectionHandle) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<CloseCallbackHandleState>();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener = server.add_listener<CloseCallbackUsesHandleHandler>(
        {
            .name = "close-callback-handle",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
            .options = {.reuse_port = true},
        },
        CloseCallbackUsesHandleHandler{state});
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    {
        UniqueFd fd = connect_loopback(port);
        ASSERT_GE(fd.get(), 0);
    }

    ASSERT_TRUE(
        wait_until([&] { return state->close_count.load(std::memory_order_acquire) >= 1; }));
    EXPECT_EQ(state->send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::SendResult::Closed));
    EXPECT_EQ(state->close_after_flush_result.load(std::memory_order_acquire), 0);

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, SingleAcceptorDistributesAcceptedConnectionsAcrossBoundIoThreads) {
    const std::uint16_t port = reserve_loopback_port();
    auto counters = std::make_shared<TwoIoCounters>();

    NetServerTwoIoRuntime::init();
    ReactorTcpServer<NetServerTwoIoRuntime> server;
    server.bind_threads(NetServerTwoIoRuntime::thread_group<NetServerTwoIoTag>());

    const auto listener = server.add_listener<TwoIoEchoHandler>(
        {
            .name = "single-acceptor-echo",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
            .options = {.reuse_port = false},
            .accept_strategy = af::net::AcceptStrategy::SingleAcceptor,
        },
        TwoIoEchoHandler{counters});
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    for (int i = 0; i < 8; ++i) {
        const std::string payload = "msg-" + std::to_string(i);
        EXPECT_EQ(roundtrip(port, payload), payload);
    }

    EXPECT_GT(counters->reads[0].load(std::memory_order_relaxed), 0);
    EXPECT_GT(counters->reads[1].load(std::memory_order_relaxed), 0);

    EXPECT_TRUE(server.stop());
    NetServerTwoIoRuntime::wait_for_idle();
    NetServerTwoIoRuntime::shutdown();
}

TEST(NetTcpServerTests, ReusesClosedConnectionSlotsAndRejectsStaleHandles) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<SlotReuseState>();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener = server.add_listener<ClosingEchoHandler>(
        {
            .name = "slot-reuse",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
            .options = {.reuse_port = true},
        },
        ClosingEchoHandler{state});
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    EXPECT_EQ(roundtrip(port, "first"), "first");
    ASSERT_TRUE(wait_until([&] {
        return state->published[0].load(std::memory_order_acquire) &&
               state->close_count.load(std::memory_order_acquire) >= 1;
    }));

    EXPECT_EQ(roundtrip(port, "second"), "second");
    ASSERT_TRUE(wait_until([&] {
        return state->published[1].load(std::memory_order_acquire) &&
               state->close_count.load(std::memory_order_acquire) >= 2;
    }));

    const std::uint32_t first_slot = state->slots[0].load(std::memory_order_relaxed);
    const std::uint32_t second_slot = state->slots[1].load(std::memory_order_relaxed);
    const std::uint32_t first_generation = state->generations[0].load(std::memory_order_relaxed);
    const std::uint32_t second_generation = state->generations[1].load(std::memory_order_relaxed);

    EXPECT_EQ(first_slot, second_slot);
    EXPECT_NE(first_generation, second_generation);
    EXPECT_EQ(state->stale_send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::SendResult::Closed));

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, AddsAndRemovesListenerWhileRunning) {
    const std::uint16_t port = reserve_loopback_port();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    ASSERT_TRUE(server.start());
    const auto listener = server.add_listener<EchoHandler>({
        .name = "dynamic-echo",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
        .options = {.reuse_port = true},
    });
    ASSERT_TRUE(listener.ok()) << listener.error;

    EXPECT_EQ(roundtrip(port, "hello"), "hello");
    EXPECT_TRUE(server.remove_listener(listener.listener));

    UniqueFd fd = connect_loopback(port);
    EXPECT_LT(fd.get(), 0);

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, RemovedDynamicListenerSlotIsReusedWithNewGeneration) {
    const std::uint16_t port_a = reserve_loopback_port();
    const std::uint16_t port_b = reserve_loopback_port();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    ASSERT_TRUE(server.start());
    const auto listener_a = server.add_listener<EchoHandler>({
        .name = "dynamic-echo-a",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port_a),
        .options = {.reuse_port = true},
    });
    ASSERT_TRUE(listener_a.ok()) << listener_a.error;
    EXPECT_EQ(roundtrip(port_a, "first"), "first");

    EXPECT_TRUE(server.remove_listener(listener_a.listener));
    UniqueFd closed = connect_loopback(port_a);
    EXPECT_LT(closed.get(), 0);

    const auto listener_b = server.add_listener<EchoHandler>({
        .name = "dynamic-echo-b",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port_b),
        .options = {.reuse_port = true},
    });
    ASSERT_TRUE(listener_b.ok()) << listener_b.error;
    EXPECT_EQ(listener_b.listener.slot(), listener_a.listener.slot());
    EXPECT_NE(listener_b.listener.generation(), listener_a.listener.generation());
    EXPECT_FALSE(server.remove_listener(listener_a.listener));
    EXPECT_EQ(roundtrip(port_b, "second"), "second");

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, FailedDynamicListenerCanBeRemovedAndSlotReused) {
    const std::uint16_t port_a = reserve_loopback_port();
    const std::uint16_t port_b = reserve_loopback_port();
    auto error_state = std::make_shared<ListenerErrorState>();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener_a = server.add_listener<EchoHandler>({
        .name = "active-no-reuse-port",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port_a),
        .options = {.reuse_port = false},
    });
    ASSERT_TRUE(listener_a.ok()) << listener_a.error;
    ASSERT_TRUE(server.start());

    const auto conflict = server.add_listener<ErrorEchoHandler>(
        {
            .name = "conflicting-no-reuse-port",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port_a),
            .options = {.reuse_port = false},
        },
        ErrorEchoHandler{error_state});
    ASSERT_TRUE(conflict.ok()) << conflict.error;
    ASSERT_TRUE(
        wait_until([&] { return error_state->errors.load(std::memory_order_acquire) >= 1; }));
    EXPECT_NE(error_state->last_error.load(std::memory_order_acquire), 0);
    EXPECT_TRUE(server.remove_listener(conflict.listener));

    const auto listener_b = server.add_listener<EchoHandler>({
        .name = "post-conflict-echo",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port_b),
        .options = {.reuse_port = true},
    });
    ASSERT_TRUE(listener_b.ok()) << listener_b.error;
    EXPECT_EQ(listener_b.listener.slot(), conflict.listener.slot());
    EXPECT_NE(listener_b.listener.generation(), conflict.listener.generation());
    EXPECT_EQ(roundtrip(port_b, "after-conflict"), "after-conflict");

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, HandlerCloneFailureDoesNotLeaveListenerStuck) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<ThrowOnCopyState>();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto broken = server.add_listener<ThrowOnCopyHandler>(
        {
            .name = "throw-on-clone",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
            .options = {.reuse_port = true},
        },
        ThrowOnCopyHandler{state});
    ASSERT_TRUE(broken.ok()) << broken.error;

    state->throw_on_copy.store(true, std::memory_order_release);
    EXPECT_FALSE(server.start());
    EXPECT_TRUE(server.remove_listener(broken.listener));

    const auto recovered = server.add_listener<EchoHandler>({
        .name = "recovered-after-clone-failure",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
        .options = {.reuse_port = true},
    });
    ASSERT_TRUE(recovered.ok()) << recovered.error;
    EXPECT_EQ(recovered.listener.slot(), broken.listener.slot());
    EXPECT_NE(recovered.listener.generation(), broken.listener.generation());
    ASSERT_TRUE(server.start());
    EXPECT_EQ(roundtrip(port, "recovered"), "recovered");

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, StopRejectsQueuedWritesFromOldHandles) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<CapturedHandleState>();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener = server.add_listener<CaptureHandleHandler>(
        {
            .name = "capture-for-stop",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
            .options = {.reuse_port = true},
        },
        CaptureHandleHandler{state});
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    UniqueFd fd = connect_loopback(port);
    ASSERT_GE(fd.get(), 0);
    ASSERT_TRUE(wait_until([&] { return state->published.load(std::memory_order_acquire); }));

    EXPECT_TRUE(server.stop());
    EXPECT_TRUE(
        wait_until([&] { return state->close_count.load(std::memory_order_acquire) >= 1; }));
    EXPECT_EQ(state->handle.send(af::Buffer::copy("after-stop", 10)), af::net::SendResult::Closed);

    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, StopClosesBufferedConnectionsAfterConfiguredTimeout) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<StopTimeoutState>();

    af::net::TcpServerConfig server_config;
    server_config.connection.write_budget_bytes = 16U * 1024U;
    server_config.connection.output_high_watermark = 64U * 1024U;
    server_config.connection_close_timeout = std::chrono::milliseconds(20);

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server(server_config);
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener = server.add_listener<FillSendQueueOnAcceptHandler>(
        {
            .name = "stop-timeout-fill",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
            .options = {.reuse_port = true},
        },
        FillSendQueueOnAcceptHandler{state});
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    UniqueFd fd = connect_loopback_with_recv_buffer(port, 4096);
    ASSERT_GE(fd.get(), 0);
    ASSERT_TRUE(
        wait_until([&] { return state->send_result.load(std::memory_order_acquire) != -1; }));
    ASSERT_EQ(state->send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::SendResult::Backpressure));

    const auto started = std::chrono::steady_clock::now();
    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    const auto elapsed = std::chrono::steady_clock::now() - started;

    EXPECT_LT(elapsed, std::chrono::seconds(1));
    EXPECT_GE(state->close_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(state->close_reason.load(std::memory_order_acquire),
              static_cast<int>(af::net::CloseReason::Local));

    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, RemovingListenerKeepsExistingConnectionScheduledSendPath) {
    const std::uint16_t port = reserve_loopback_port();
    auto state = std::make_shared<CapturedHandleState>();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener = server.add_listener<CaptureHandleHandler>(
        {
            .name = "capture-for-remove",
            .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
            .options = {.reuse_port = true},
        },
        CaptureHandleHandler{state});
    ASSERT_TRUE(listener.ok()) << listener.error;
    ASSERT_TRUE(server.start());

    UniqueFd fd = connect_loopback(port);
    ASSERT_GE(fd.get(), 0);
    timeval timeout{};
    timeout.tv_sec = 2;
    static_cast<void>(::setsockopt(fd.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
    ASSERT_TRUE(wait_until([&] { return state->published.load(std::memory_order_acquire); }));

    EXPECT_TRUE(server.remove_listener(listener.listener));
    EXPECT_EQ(state->handle.send(af::Buffer::copy("server-push", 11)), af::net::SendResult::Queued);

    char buffer[32]{};
    const ssize_t n = ::recv(fd.get(), buffer, sizeof(buffer), 0);
    ASSERT_EQ(n, 11);
    EXPECT_EQ(std::string(buffer, static_cast<std::size_t>(n)), "server-push");

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}

TEST(NetTcpServerTests, StopsAndRestartsConfiguredListeners) {
    const std::uint16_t port = reserve_loopback_port();

    NetServerRuntime::init();
    ReactorTcpServer<NetServerRuntime> server;
    server.bind_threads(NetServerRuntime::thread_group<NetServerIoTag>());

    const auto listener = server.add_listener<EchoHandler>({
        .name = "restart-echo",
        .endpoint = af::net::TcpEndpoint::host("127.0.0.1", port),
        .options = {.reuse_port = true},
    });
    ASSERT_TRUE(listener.ok()) << listener.error;

    ASSERT_TRUE(server.start());
    EXPECT_EQ(roundtrip(port, "before-stop"), "before-stop");
    EXPECT_TRUE(server.stop());

    UniqueFd closed = connect_loopback(port);
    EXPECT_LT(closed.get(), 0);

    ASSERT_TRUE(server.start());
    EXPECT_EQ(roundtrip(port, "after-restart"), "after-restart");

    EXPECT_TRUE(server.stop());
    NetServerRuntime::wait_for_idle();
    NetServerRuntime::shutdown();
}
