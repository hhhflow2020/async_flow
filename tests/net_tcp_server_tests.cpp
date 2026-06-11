#include <atomic>
#include <array>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "af/net.hpp"
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

struct RuntimeTcpListenerState {
    af::runtime *runtime{nullptr};
    af::thread_ref cpu_thread{};
    af::net::tcp_connection_handle handle;
    std::atomic<bool> started{false};
    std::atomic<bool> start_ok{false};
    std::atomic<bool> stopped{false};
    std::atomic<bool> stop_ok{false};
    std::atomic<int> accepted{0};
    std::atomic<int> reads{0};
    std::atomic<int> closes{0};
    std::atomic<int> errors{0};
    std::atomic<int> last_error{0};
    std::atomic<int> cpu_runs{0};
    std::atomic<int> handle_send_result{-1};
    std::atomic<int> successful_ops{0};
    std::atomic<std::uint64_t> runtime_thread_mask{0};
    std::atomic<std::size_t> first_read_size{0};
    std::atomic<int> connection_count_after_close{-1};
    std::atomic<bool> connection_count_checked{false};
    std::atomic<bool> handle_published{false};
    std::atomic<std::uint16_t> port{0};
    std::atomic<std::uint16_t> second_port{0};
};

struct RuntimeTcpSlotReuseState {
    RuntimeTcpSlotReuseState() {
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
    std::atomic<int> closes{0};
    std::atomic<int> stale_send_result{-1};
    std::array<std::atomic<bool>, 2> published;
    std::array<std::atomic<std::uint32_t>, 2> slots;
    std::array<std::atomic<std::uint32_t>, 2> generations;
    std::array<af::net::tcp_connection_handle, 2> handles;
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

void runtime_tcp_connection_accept(void *owner, af::net::tcp_connection_ref conn) noexcept {
    auto *state = static_cast<RuntimeTcpListenerState *>(owner);
    if (state != nullptr && conn.valid()) {
        state->accepted.fetch_add(1, std::memory_order_release);
    }
}

void runtime_tcp_connection_record_accept(void *owner, af::net::tcp_connection_ref conn) noexcept {
    auto *state = static_cast<RuntimeTcpListenerState *>(owner);
    if (state == nullptr || !conn.valid()) {
        return;
    }
    state->accepted.fetch_add(1, std::memory_order_release);
    const auto index = af::runtime::current_thread_index();
    if (index < 64U) {
        state->runtime_thread_mask.fetch_or(1ULL << index, std::memory_order_release);
    }
}

void runtime_tcp_connection_capture_accept(void *owner, af::net::tcp_connection_ref conn) noexcept {
    auto *state = static_cast<RuntimeTcpListenerState *>(owner);
    if (state != nullptr && conn.valid()) {
        state->handle = conn.handle();
        state->accepted.fetch_add(1, std::memory_order_release);
        state->handle_published.store(true, std::memory_order_release);
    }
}

void runtime_tcp_slot_reuse_accept(void *owner, af::net::tcp_connection_ref conn) noexcept {
    auto *state = static_cast<RuntimeTcpSlotReuseState *>(owner);
    if (state == nullptr || !conn.valid()) {
        return;
    }
    const int index = state->next_accept.fetch_add(1, std::memory_order_release);
    if (index < 0 || index >= static_cast<int>(state->handles.size())) {
        return;
    }
    const auto array_index = static_cast<std::size_t>(index);
    state->handles[array_index] = conn.handle();
    state->slots[array_index].store(conn.slot(), std::memory_order_relaxed);
    state->generations[array_index].store(conn.generation(), std::memory_order_relaxed);
    if (index == 1) {
        const af::net::send_result result = state->handles[0].send(af::buffer::copy("stale", 5));
        state->stale_send_result.store(static_cast<int>(result), std::memory_order_release);
    }
    state->published[array_index].store(true, std::memory_order_release);
}

void runtime_tcp_connection_read(void *owner, af::net::tcp_connection_ref conn,
                                 af::buffer_view bytes) noexcept {
    auto *state = static_cast<RuntimeTcpListenerState *>(owner);
    if (state != nullptr) {
        state->reads.fetch_add(1, std::memory_order_release);
    }
    static_cast<void>(conn.send(bytes));
}

void runtime_tcp_connection_record_read(void *owner, af::net::tcp_connection_ref conn,
                                        af::buffer_view bytes) noexcept {
    auto *state = static_cast<RuntimeTcpListenerState *>(owner);
    if (state != nullptr) {
        state->reads.fetch_add(1, std::memory_order_release);
        const auto index = af::runtime::current_thread_index();
        if (index < 64U) {
            state->runtime_thread_mask.fetch_or(1ULL << index, std::memory_order_release);
        }
    }
    static_cast<void>(conn.send(bytes));
}

void runtime_tcp_connection_read_and_close(void *owner, af::net::tcp_connection_ref conn,
                                           af::buffer_view bytes) noexcept {
    static_cast<void>(owner);
    static_cast<void>(conn.send(bytes));
    conn.close_after_flush();
}

void runtime_tcp_connection_read_via_handle(void *owner, af::net::tcp_connection_ref conn,
                                            af::buffer_view bytes) noexcept {
    auto *state = static_cast<RuntimeTcpListenerState *>(owner);
    if (state != nullptr) {
        state->reads.fetch_add(1, std::memory_order_release);
    }

    af::buffer payload;
    try {
        payload = af::buffer::copy(bytes);
    } catch (...) {
        if (state != nullptr) {
            state->handle_send_result.store(static_cast<int>(af::net::send_result::backpressure),
                                            std::memory_order_release);
        }
        conn.close(af::net::close_reason::error);
        return;
    }

    if (state == nullptr || state->runtime == nullptr || !state->cpu_thread) {
        if (state != nullptr) {
            state->handle_send_result.store(static_cast<int>(af::net::send_result::closed),
                                            std::memory_order_release);
        }
        conn.close(af::net::close_reason::error);
        return;
    }

    const af::net::tcp_connection_handle handle = conn.handle();
    const bool posted = state->runtime->post(
        state->cpu_thread, [state, handle, payload = std::move(payload)]() mutable {
            state->cpu_runs.fetch_add(1, std::memory_order_release);
            const af::net::send_result result = handle.send(std::move(payload));
            state->handle_send_result.store(static_cast<int>(result), std::memory_order_release);
        });
    if (!posted) {
        state->handle_send_result.store(static_cast<int>(af::net::send_result::backpressure),
                                        std::memory_order_release);
        conn.close(af::net::close_reason::error);
    }
}

void runtime_tcp_connection_owner_control_read(void *owner, af::net::tcp_connection_ref conn,
                                               af::buffer_view bytes) noexcept {
    auto *state = static_cast<RuntimeTcpListenerState *>(owner);
    if (state != nullptr) {
        state->reads.fetch_add(1, std::memory_order_release);
    }

    const af::net::tcp_connection_handle handle = conn.handle();
    int successful = 0;
    successful += handle.pause_read() ? 1 : 0;
    successful += handle.resume_read() ? 1 : 0;
    successful += handle.set_no_delay(true) ? 1 : 0;
    successful += handle.set_keepalive(false) ? 1 : 0;
    const af::net::send_result sent = handle.send(bytes);
    successful += sent == af::net::send_result::accepted ? 1 : 0;
    successful += handle.close_after_flush() ? 1 : 0;

    if (state != nullptr) {
        state->handle_send_result.store(static_cast<int>(sent), std::memory_order_release);
        state->successful_ops.store(successful, std::memory_order_release);
    }
}

void runtime_tcp_connection_pause_after_first_read(void *owner, af::net::tcp_connection_ref conn,
                                                   af::buffer_view bytes) noexcept {
    auto *state = static_cast<RuntimeTcpListenerState *>(owner);
    if (state != nullptr) {
        const int previous = state->reads.fetch_add(1, std::memory_order_release);
        if (previous == 0) {
            state->first_read_size.store(bytes.size(), std::memory_order_release);
        }
    }
    static_cast<void>(conn.pause_read());
}

void runtime_tcp_connection_close(void *owner, af::net::tcp_connection_ref conn,
                                  af::net::close_reason reason) noexcept {
    static_cast<void>(conn);
    static_cast<void>(reason);
    auto *state = static_cast<RuntimeTcpListenerState *>(owner);
    if (state != nullptr) {
        state->closes.fetch_add(1, std::memory_order_release);
    }
}

void runtime_tcp_slot_reuse_close(void *owner, af::net::tcp_connection_ref conn,
                                  af::net::close_reason reason) noexcept {
    static_cast<void>(conn);
    static_cast<void>(reason);
    auto *state = static_cast<RuntimeTcpSlotReuseState *>(owner);
    if (state != nullptr) {
        state->closes.fetch_add(1, std::memory_order_release);
    }
}

struct StopTimeoutState {
    StopTimeoutState() {
        payload.fill('x');
    }

    std::atomic<bool> accepted{false};
    std::atomic<int> send_result{-1};
    std::atomic<int> close_count{0};
    std::atomic<int> close_reason{-1};
    std::array<char, 16U * 1024U> payload{};
};

void runtime_tcp_fill_send_queue_on_accept(void *owner, af::net::tcp_connection_ref conn) noexcept {
    auto *state = static_cast<StopTimeoutState *>(owner);
    if (state == nullptr || !conn.valid()) {
        return;
    }

    state->accepted.store(true, std::memory_order_release);
    af::net::send_result last_result = af::net::send_result::accepted;
    for (int i = 0; i < 4096; ++i) {
        last_result = conn.send(af::buffer_view(state->payload.data(), state->payload.size()));
        if (last_result == af::net::send_result::backpressure ||
            last_result == af::net::send_result::closed) {
            break;
        }
    }
    state->send_result.store(static_cast<int>(last_result), std::memory_order_release);
}

void runtime_tcp_stop_timeout_close(void *owner, af::net::tcp_connection_ref conn,
                                    af::net::close_reason reason) noexcept {
    static_cast<void>(conn);
    auto *state = static_cast<StopTimeoutState *>(owner);
    if (state == nullptr) {
        return;
    }
    state->close_reason.store(static_cast<int>(reason), std::memory_order_release);
    state->close_count.fetch_add(1, std::memory_order_release);
}

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
        listener_config.endpoint = af::net::tcp_endpoint::unix_path("");
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
    EXPECT_EQ(state.last_error.load(std::memory_order_acquire), EINVAL);
    EXPECT_FALSE(listener.started());

    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpServerStartsConfiguredListenersOnRuntimeReactorThread) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_listener_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_tcp_listener_accept;
        callbacks.on_error = &runtime_tcp_listener_error;

        af::net::tcp_listener_config first_config;
        first_config.name = "runtime-server-a";
        first_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        first_config.threads = {io_thread};
        first_config.options.reuse_port = false;

        af::net::tcp_listener_config second_config;
        second_config.name = "runtime-server-b";
        second_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        second_config.threads = {io_thread};
        second_config.options.reuse_port = false;

        const af::net::listener_result first =
            server.add_listener(std::move(first_config), callbacks);
        const af::net::listener_result second =
            server.add_listener(std::move(second_config), callbacks);
        bool ok = first.ok() && second.ok() && server.start();
        const af::net::tcp_endpoint *first_endpoint = server.local_endpoint(first.listener);
        const af::net::tcp_endpoint *second_endpoint = server.local_endpoint(second.listener);
        ok = ok && first_endpoint != nullptr && second_endpoint != nullptr;
        if (ok) {
            state.port.store(first_endpoint->port, std::memory_order_release);
            state.second_port.store(second_endpoint->port, std::memory_order_release);
        }
        state.start_ok.store(ok, std::memory_order_release);
        state.started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return state.started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(state.start_ok.load(std::memory_order_acquire));
    const std::uint16_t first_port = state.port.load(std::memory_order_acquire);
    const std::uint16_t second_port = state.second_port.load(std::memory_order_acquire);
    ASSERT_GT(first_port, 0U);
    ASSERT_GT(second_port, 0U);
    EXPECT_NE(first_port, second_port);

    UniqueFd first_client = connect_loopback(first_port);
    ASSERT_GE(first_client.get(), 0);
    UniqueFd second_client = connect_loopback(second_port);
    ASSERT_GE(second_client.get(), 0);
    ASSERT_TRUE(wait_until([&] { return state.accepted.load(std::memory_order_acquire) == 2; }));
    EXPECT_EQ(state.errors.load(std::memory_order_acquire), 0);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        state.stop_ok.store(server.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpServerAllowsSequentialControlFromAnotherIoThread) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 2)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 2; }));

    const af::thread_ref io_0 = runtime.select_thread_ref(af::thread_selector::io(0));
    const af::thread_ref io_1 = runtime.select_thread_ref(af::thread_selector::io(1));
    af::net::tcp_server server(runtime);

    std::atomic<bool> second_done{false};
    std::atomic<bool> second_ok{false};
    std::atomic<int> second_error{0};
    std::atomic<int> second_port{0};

    ASSERT_TRUE(runtime.post(io_0, [&] {
        af::net::tcp_listener_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_tcp_listener_accept;
        callbacks.on_error = &runtime_tcp_listener_error;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-server-owner";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_0};
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        state.start_ok.store(listener.ok() && server.start(), std::memory_order_release);
        state.started.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(state.start_ok.load(std::memory_order_acquire));

    ASSERT_TRUE(runtime.post(io_1, [&] {
        af::net::tcp_listener_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_tcp_listener_accept;
        callbacks.on_error = &runtime_tcp_listener_error;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-server-wrong-owner";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_1};
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        const af::net::tcp_endpoint *endpoint =
            listener.ok() ? server.local_endpoint(listener.listener) : nullptr;
        second_ok.store(listener.ok() && endpoint != nullptr, std::memory_order_release);
        second_error.store(listener.error, std::memory_order_release);
        second_port.store(endpoint == nullptr ? 0 : endpoint->port, std::memory_order_release);
        second_done.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return second_done.load(std::memory_order_acquire); }));
    EXPECT_TRUE(second_ok.load(std::memory_order_acquire));
    EXPECT_EQ(second_error.load(std::memory_order_acquire), 0);

    if (second_ok.load(std::memory_order_acquire)) {
        UniqueFd client = connect_loopback(
            static_cast<std::uint16_t>(second_port.load(std::memory_order_acquire)));
        EXPECT_GE(client.get(), 0);
        EXPECT_TRUE(
            wait_until([&] { return state.accepted.load(std::memory_order_acquire) >= 1; }));
        EXPECT_EQ(state.errors.load(std::memory_order_acquire), 0);
    }

    ASSERT_TRUE(runtime.post(io_0, [&] {
        state.stop_ok.store(server.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpServerReusePortListenerStartsOnMultipleIoThreads) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 2)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 2; }));

    const std::uint16_t port = reserve_loopback_port();
    const af::thread_ref io_0 = runtime.select_thread_ref(af::thread_selector::io(0));
    const af::thread_ref io_1 = runtime.select_thread_ref(af::thread_selector::io(1));
    af::net::tcp_server server(runtime);
    af::net::tcp_listener_handle listener;

    ASSERT_TRUE(runtime.post(io_0, [&] {
        af::net::tcp_connection_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_tcp_connection_record_accept;
        callbacks.on_read = &runtime_tcp_connection_record_read;
        callbacks.on_close = &runtime_tcp_connection_close;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-server-reuse-port-shards";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(port);
        listener_config.threads = {io_0, io_1};
        listener_config.options.reuse_port = true;

        const af::net::listener_result result =
            server.add_listener(std::move(listener_config), callbacks);
        listener = result.listener;
        const bool ok = result.ok() && server.start();
        state.start_ok.store(ok, std::memory_order_release);
        state.started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return state.started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(state.start_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(listener.valid());

    std::atomic<bool> io_1_checked{false};
    std::atomic<bool> io_1_active{false};
    ASSERT_TRUE(runtime.post(io_1, [&] {
        io_1_active.store(server.state(listener) == af::net::listener_state::active,
                          std::memory_order_release);
        io_1_checked.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return io_1_checked.load(std::memory_order_acquire); }));
    EXPECT_TRUE(io_1_active.load(std::memory_order_acquire));

    EXPECT_EQ(roundtrip(port, "reuse-port"), "reuse-port");
    ASSERT_TRUE(wait_until([&] { return state.accepted.load(std::memory_order_acquire) >= 1; }));
    EXPECT_NE(state.runtime_thread_mask.load(std::memory_order_acquire), 0U);

    ASSERT_TRUE(runtime.post(io_0, [&] {
        state.stop_ok.store(server.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpServerStartsListenerAddedWhileRunning) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_listener_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_tcp_listener_accept;
        callbacks.on_error = &runtime_tcp_listener_error;

        bool ok = server.start();
        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-server-hot-add";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
        ok = ok && listener.ok() && endpoint != nullptr;
        if (ok) {
            state.port.store(endpoint->port, std::memory_order_release);
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
        state.stop_ok.store(server.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpServerAdoptsConnectionsAndEchoesReads) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_connection_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_tcp_connection_accept;
        callbacks.on_read = &runtime_tcp_connection_read;
        callbacks.on_close = &runtime_tcp_connection_close;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-server-echo";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_thread};
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        bool ok = listener.ok() && server.start();
        const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
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

    EXPECT_EQ(roundtrip(state.port.load(std::memory_order_acquire), "runtime-echo"),
              "runtime-echo");
    ASSERT_TRUE(wait_until([&] { return state.accepted.load(std::memory_order_acquire) == 1; }));
    ASSERT_TRUE(wait_until([&] { return state.reads.load(std::memory_order_acquire) >= 1; }));
    EXPECT_EQ(state.errors.load(std::memory_order_acquire), 0);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        state.stop_ok.store(server.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpConnectionHandleSendsFromCpuThread) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1), af::cpu_threads("net-rt-cpu", 1)};
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState state;
    state.runtime = &runtime;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 2; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    state.cpu_thread = runtime.select_thread_ref(af::thread_selector::cpu(0));
    ASSERT_TRUE(state.cpu_thread);

    af::net::tcp_server server(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_connection_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_tcp_connection_accept;
        callbacks.on_read = &runtime_tcp_connection_read_via_handle;
        callbacks.on_close = &runtime_tcp_connection_close;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-server-handle-echo";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_thread};
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        bool ok = listener.ok() && server.start();
        const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
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

    EXPECT_EQ(roundtrip(state.port.load(std::memory_order_acquire), "runtime-handle"),
              "runtime-handle");
    ASSERT_TRUE(wait_until([&] { return state.accepted.load(std::memory_order_acquire) == 1; }));
    ASSERT_TRUE(wait_until([&] { return state.reads.load(std::memory_order_acquire) >= 1; }));
    ASSERT_TRUE(wait_until([&] { return state.cpu_runs.load(std::memory_order_acquire) == 1; }));
    EXPECT_EQ(state.handle_send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::send_result::queued));
    EXPECT_EQ(state.errors.load(std::memory_order_acquire), 0);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        state.stop_ok.store(server.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpConnectionHandleOwnerThreadControlsConnection) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_connection_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_tcp_connection_accept;
        callbacks.on_read = &runtime_tcp_connection_owner_control_read;
        callbacks.on_close = &runtime_tcp_connection_close;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-server-owner-handle-control";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_thread};
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        bool ok = listener.ok() && server.start();
        const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
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

    EXPECT_EQ(roundtrip(state.port.load(std::memory_order_acquire), "owner"), "owner");
    ASSERT_TRUE(wait_until([&] {
        return state.successful_ops.load(std::memory_order_acquire) == 6 &&
               state.closes.load(std::memory_order_acquire) >= 1;
    }));
    EXPECT_EQ(state.handle_send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::send_result::accepted));

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        state.stop_ok.store(server.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpConnectionPauseReadStopsCurrentDrainLoop) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_connection_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_tcp_connection_accept;
        callbacks.on_read = &runtime_tcp_connection_pause_after_first_read;
        callbacks.on_close = &runtime_tcp_connection_close;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-server-pause-read";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_thread};
        listener_config.options.reuse_port = false;
        listener_config.options.read_budget_bytes = 256U * 1024U;
        listener_config.options.read_buffer_size = 1024U;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        bool ok = listener.ok() && server.start();
        const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
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

    UniqueFd fd = connect_loopback(state.port.load(std::memory_order_acquire));
    ASSERT_GE(fd.get(), 0);
    const std::vector<std::byte> payload(128U * 1024U, std::byte{0x78});
    send_all(fd.get(), payload.data(), payload.size());

    ASSERT_TRUE(wait_until([&] { return state.reads.load(std::memory_order_acquire) >= 1; }));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(state.reads.load(std::memory_order_acquire), 1);
    EXPECT_GT(state.first_read_size.load(std::memory_order_acquire), 0U);
    EXPECT_LE(state.first_read_size.load(std::memory_order_acquire), 1024U);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        state.stop_ok.store(server.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpConnectionHandleReportsClosedAfterServerStop) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_connection_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_tcp_connection_capture_accept;
        callbacks.on_read = &runtime_tcp_connection_read;
        callbacks.on_close = &runtime_tcp_connection_close;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-server-handle-stop";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_thread};
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        bool ok = listener.ok() && server.start();
        const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
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

    UniqueFd client = connect_loopback(state.port.load(std::memory_order_acquire));
    ASSERT_GE(client.get(), 0);
    ASSERT_TRUE(wait_until([&] { return state.handle_published.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.handle.valid());

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        state.stop_ok.store(server.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));
    EXPECT_TRUE(wait_until([&] { return state.closes.load(std::memory_order_acquire) >= 1; }));
    EXPECT_EQ(state.handle.send(af::buffer::copy("after-stop", 10)), af::net::send_result::closed);

    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpServerStopClosesBufferedConnectionsAfterConfiguredTimeout) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState lifecycle;
    auto state = std::make_shared<StopTimeoutState>();

    af::net::tcp_server_config server_config;
    server_config.connection.write_budget_bytes = 16U * 1024U;
    server_config.connection.output_high_watermark = 64U * 1024U;
    server_config.connection_close_timeout = std::chrono::milliseconds(20);

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime, server_config);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_connection_callbacks callbacks;
        callbacks.owner = state.get();
        callbacks.on_accept = &runtime_tcp_fill_send_queue_on_accept;
        callbacks.on_close = &runtime_tcp_stop_timeout_close;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-server-stop-timeout";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_thread};
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        bool ok = listener.ok() && server.start();
        const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
        ok = ok && endpoint != nullptr;
        if (ok) {
            lifecycle.port.store(endpoint->port, std::memory_order_release);
        }
        lifecycle.start_ok.store(ok, std::memory_order_release);
        lifecycle.started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return lifecycle.started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(lifecycle.start_ok.load(std::memory_order_acquire));
    ASSERT_GT(lifecycle.port.load(std::memory_order_acquire), 0U);

    UniqueFd client =
        connect_loopback_with_recv_buffer(lifecycle.port.load(std::memory_order_acquire), 4096);
    ASSERT_GE(client.get(), 0);
    ASSERT_TRUE(
        wait_until([&] { return state->send_result.load(std::memory_order_acquire) != -1; }));
    ASSERT_EQ(state->send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::send_result::backpressure));

    const auto started = std::chrono::steady_clock::now();
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        lifecycle.stop_ok.store(server.stop(), std::memory_order_release);
        lifecycle.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return lifecycle.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(lifecycle.stop_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(
        wait_until([&] { return state->close_count.load(std::memory_order_acquire) >= 1; }));
    const auto elapsed = std::chrono::steady_clock::now() - started;

    EXPECT_LT(elapsed, std::chrono::seconds(1));
    EXPECT_GE(elapsed, std::chrono::milliseconds(5));
    EXPECT_EQ(state->close_reason.load(std::memory_order_acquire),
              static_cast<int>(af::net::close_reason::local));

    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpConnectionHandleCloseReclaimsSlot) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_connection_callbacks callbacks;
        callbacks.owner = &state;
        callbacks.on_accept = &runtime_tcp_connection_capture_accept;
        callbacks.on_read = &runtime_tcp_connection_read;
        callbacks.on_close = &runtime_tcp_connection_close;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-server-handle-close";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_thread};
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        bool ok = listener.ok() && server.start();
        const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
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

    UniqueFd client = connect_loopback(state.port.load(std::memory_order_acquire));
    ASSERT_GE(client.get(), 0);
    ASSERT_TRUE(wait_until([&] { return state.handle_published.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.handle.valid());
    EXPECT_TRUE(state.handle.close());
    ASSERT_TRUE(wait_until([&] { return state.closes.load(std::memory_order_acquire) >= 1; }));

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        state.connection_count_after_close.store(static_cast<int>(server.connection_count()),
                                                 std::memory_order_release);
        state.connection_count_checked.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(
        wait_until([&] { return state.connection_count_checked.load(std::memory_order_acquire); }));
    EXPECT_EQ(state.connection_count_after_close.load(std::memory_order_acquire), 0);

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        state.stop_ok.store(server.stop(), std::memory_order_release);
        state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(NetTcpServerTests, RuntimeTcpConnectionHandleRejectsStaleGenerationAfterSlotReuse) {
    af::runtime_config config;
    config.threads = {af::io_threads("net-rt-io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);

    af::runtime runtime(config);
    RuntimeTcpListenerState listener_state;
    RuntimeTcpSlotReuseState reuse_state;

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_until([&] { return runtime.active_thread_count() == 1; }));

    const af::thread_ref io_thread = runtime.select_thread_ref(af::thread_selector::io(0));
    af::net::tcp_server server(runtime);
    ASSERT_TRUE(runtime.post(io_thread, [&] {
        af::net::tcp_connection_callbacks callbacks;
        callbacks.owner = &reuse_state;
        callbacks.on_accept = &runtime_tcp_slot_reuse_accept;
        callbacks.on_read = &runtime_tcp_connection_read_and_close;
        callbacks.on_close = &runtime_tcp_slot_reuse_close;

        af::net::tcp_listener_config listener_config;
        listener_config.name = "runtime-server-slot-reuse";
        listener_config.endpoint = af::net::tcp_endpoint::loopback_v4(0);
        listener_config.threads = {io_thread};
        listener_config.options.reuse_port = false;

        const af::net::listener_result listener =
            server.add_listener(std::move(listener_config), callbacks);
        bool ok = listener.ok() && server.start();
        const af::net::tcp_endpoint *endpoint = server.local_endpoint(listener.listener);
        ok = ok && endpoint != nullptr;
        if (ok) {
            listener_state.port.store(endpoint->port, std::memory_order_release);
        }
        listener_state.start_ok.store(ok, std::memory_order_release);
        listener_state.started.store(true, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_until([&] { return listener_state.started.load(std::memory_order_acquire); }));
    ASSERT_TRUE(listener_state.start_ok.load(std::memory_order_acquire));
    const std::uint16_t port = listener_state.port.load(std::memory_order_acquire);
    ASSERT_GT(port, 0U);

    EXPECT_EQ(roundtrip(port, "first"), "first");
    ASSERT_TRUE(wait_until([&] {
        return reuse_state.published[0].load(std::memory_order_acquire) &&
               reuse_state.closes.load(std::memory_order_acquire) >= 1;
    }));

    EXPECT_EQ(roundtrip(port, "second"), "second");
    ASSERT_TRUE(wait_until([&] {
        return reuse_state.published[1].load(std::memory_order_acquire) &&
               reuse_state.closes.load(std::memory_order_acquire) >= 2;
    }));

    const std::uint32_t first_slot = reuse_state.slots[0].load(std::memory_order_relaxed);
    const std::uint32_t second_slot = reuse_state.slots[1].load(std::memory_order_relaxed);
    const std::uint32_t first_generation =
        reuse_state.generations[0].load(std::memory_order_relaxed);
    const std::uint32_t second_generation =
        reuse_state.generations[1].load(std::memory_order_relaxed);
    EXPECT_EQ(first_slot, second_slot);
    EXPECT_NE(first_generation, second_generation);
    EXPECT_EQ(reuse_state.stale_send_result.load(std::memory_order_acquire),
              static_cast<int>(af::net::send_result::closed));

    ASSERT_TRUE(runtime.post(io_thread, [&] {
        listener_state.stop_ok.store(server.stop(), std::memory_order_release);
        listener_state.stopped.store(true, std::memory_order_release);
    }));
    ASSERT_TRUE(wait_until([&] { return listener_state.stopped.load(std::memory_order_acquire); }));
    EXPECT_TRUE(listener_state.stop_ok.load(std::memory_order_acquire));

    runtime.stop();
}
