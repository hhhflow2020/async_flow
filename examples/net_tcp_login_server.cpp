#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "af/log.hpp"
#include "af/net.hpp"
#include "af/platform.hpp"
#include "af/runtime.hpp"
#include "af/signal.hpp"
#include "login.pb.h"

#include <arpa/inet.h>

namespace {

constexpr std::uint16_t login_request_id = 1;
constexpr std::uint16_t login_response_id = 2;
constexpr std::size_t packet_header_size = sizeof(std::uint32_t) + sizeof(std::uint16_t);
constexpr std::size_t max_packet_payload_size = 64U * 1024U;

enum class packet_parse_result : std::uint8_t {
    ok,
    protocol_error,
    out_of_memory,
};

struct server_lifecycle_state {
    std::atomic<std::size_t> start_attempts{0};
    std::atomic<std::size_t> started{0};
    std::atomic<std::size_t> stopped{0};
    std::atomic<bool> failed{false};
    std::atomic<int> error{0};

    void record_start(bool ok, int err = 0) noexcept {
        if (ok) {
            started.fetch_add(1U, std::memory_order_release);
        } else {
            record_failure(err);
        }
        start_attempts.fetch_add(1U, std::memory_order_release);
    }

    void record_stop() noexcept {
        stopped.fetch_add(1U, std::memory_order_release);
    }

    void record_failure(int err) noexcept {
        error.store(err == 0 ? EIO : err, std::memory_order_release);
        failed.store(true, std::memory_order_release);
    }
};

struct stream_parser {
    std::vector<std::byte> buffered;

    template <typename Fn> [[nodiscard]] packet_parse_result feed(af::BufferView bytes, Fn &&fn);
};

struct login_shard_state {
    af::runtime *runtime{nullptr};
    af::thread_ref cpu_thread{};
    std::shared_ptr<server_lifecycle_state> lifecycle;
    absl::flat_hash_map<std::uint64_t, stream_parser> parsers;
};

template <typename Predicate>
[[nodiscard]] bool wait_until(Predicate predicate,
                              std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

[[nodiscard]] std::uint64_t connection_key(std::uint32_t slot, std::uint32_t generation) noexcept {
    return (static_cast<std::uint64_t>(generation) << 32U) | slot;
}

[[nodiscard]] std::uint64_t connection_key(af::net::tcp_connection_handle conn) noexcept {
    return connection_key(conn.slot(), conn.generation());
}

[[nodiscard]] af::Buffer make_packet(std::uint16_t packet_id, std::string payload) {
    if (payload.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()) -
                             sizeof(std::uint16_t)) {
        return {};
    }
    const std::uint32_t length = static_cast<std::uint32_t>(sizeof(std::uint16_t) + payload.size());
    const std::uint32_t network_length = htonl(length);
    const std::uint16_t network_id = htons(packet_id);
    af::Buffer packet = af::Buffer::with_capacity(packet_header_size + payload.size());
    if (!packet.try_append(&network_length, sizeof(network_length)) ||
        !packet.try_append(&network_id, sizeof(network_id)) ||
        !packet.try_append(payload.data(), payload.size())) {
        return {};
    }
    return packet;
}

class login_task final : public af::runtime_task {
public:
    login_task(af::runtime_task::factory_token token, af::runtime &owner) noexcept
        : af::runtime_task(token, owner) {}

    bool do_it(af::thread_ref cpu_thread, af::net::tcp_connection_handle conn, std::string user_id,
               std::string token) {
        conn_ = conn;
        user_id_ = std::move(user_id);
        token_ = std::move(token);
        return schedule_to(cpu_thread);
    }

private:
    af::task_result run_task() noexcept override {
        LOG(INFO) << "login task running user=" << user_id_ << " slot=" << conn_.slot()
                  << " generation=" << conn_.generation();

        asyncflow::examples::net::LoginResponse response;
        response.set_ok(!user_id_.empty() && !token_.empty());
        response.set_message(response.ok() ? "hello " + user_id_ : "login rejected");

        std::string payload;
        if (!response.SerializeToString(&payload)) {
            LOG(ERROR) << "failed to serialize login response user=" << user_id_;
            return done();
        }

        af::Buffer packet = make_packet(login_response_id, std::move(payload));
        if (packet.empty()) {
            LOG(ERROR) << "failed to encode login response user=" << user_id_;
            return done();
        }

        const af::net::send_result result = conn_.send(std::move(packet));
        if (result == af::net::send_result::backpressure) {
            LOG(WARNING) << "login response backpressure user=" << user_id_
                         << " slot=" << conn_.slot();
        } else if (result == af::net::send_result::closed) {
            LOG(INFO) << "login response skipped closed user=" << user_id_
                      << " slot=" << conn_.slot();
        }
        return done();
    }

    af::net::tcp_connection_handle conn_;
    std::string user_id_;
    std::string token_;
};

template <typename Fn>
[[nodiscard]] packet_parse_result stream_parser::feed(af::BufferView bytes, Fn &&fn) {
    const auto old_size = buffered.size();
    try {
        buffered.resize(old_size + bytes.size());
    } catch (...) {
        return packet_parse_result::out_of_memory;
    }
    if (!bytes.empty()) {
        std::memcpy(buffered.data() + old_size, bytes.data(), bytes.size());
    }

    std::size_t offset = 0;
    while (buffered.size() - offset >= packet_header_size) {
        std::uint32_t network_length = 0;
        std::uint16_t network_id = 0;
        std::memcpy(&network_length, buffered.data() + offset, sizeof(network_length));
        const std::uint32_t length = ntohl(network_length);
        if (length < sizeof(std::uint16_t) ||
            length > sizeof(std::uint16_t) + max_packet_payload_size) {
            buffered.clear();
            return packet_parse_result::protocol_error;
        }

        const std::size_t full_size = sizeof(std::uint32_t) + length;
        if (buffered.size() - offset < full_size) {
            break;
        }

        std::memcpy(&network_id, buffered.data() + offset + sizeof(network_length),
                    sizeof(network_id));
        const std::uint16_t packet_id = ntohs(network_id);
        const std::byte *payload = buffered.data() + offset + packet_header_size;
        const std::size_t payload_size = length - sizeof(std::uint16_t);
        fn(packet_id, af::BufferView(payload, payload_size));
        offset += full_size;
    }

    if (offset != 0U) {
        buffered.erase(buffered.begin(), buffered.begin() + static_cast<std::ptrdiff_t>(offset));
    }
    return packet_parse_result::ok;
}

void login_on_accept(void *owner, af::net::tcp_connection_ref conn) noexcept {
    static_cast<void>(owner);
    LOG(INFO) << "login connection accepted slot=" << conn.slot()
              << " generation=" << conn.generation();
}

void login_on_read(void *owner, af::net::tcp_connection_ref conn, af::BufferView bytes) noexcept {
    auto *state = static_cast<login_shard_state *>(owner);
    if (state == nullptr || state->runtime == nullptr) {
        conn.close(af::net::close_reason::error);
        return;
    }

    const af::net::tcp_connection_handle handle = conn.handle();
    stream_parser &parser = state->parsers[connection_key(handle)];
    const packet_parse_result result =
        parser.feed(bytes, [&](std::uint16_t packet_id, af::BufferView payload) {
            if (packet_id != login_request_id) {
                LOG(WARNING) << "unknown login packet id=" << packet_id
                             << " slot=" << handle.slot();
                return;
            }

            asyncflow::examples::net::LoginRequest request;
            if (!request.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
                LOG(WARNING) << "invalid login protobuf slot=" << handle.slot();
                return;
            }

            LOG(INFO) << "login packet parsed user=" << request.user_id()
                      << " slot=" << handle.slot() << " generation=" << handle.generation();
            auto task = af::make_task<login_task>(*state->runtime);
            if (!task->do_it(state->cpu_thread, handle, std::string(request.user_id()),
                             std::string(request.token()))) {
                LOG(ERROR) << "failed to schedule login task user=" << request.user_id()
                           << " slot=" << handle.slot();
                static_cast<void>(handle.close());
            }
        });

    if (result == packet_parse_result::protocol_error) {
        LOG(WARNING) << "login protocol error slot=" << conn.slot()
                     << " generation=" << conn.generation();
        conn.close(af::net::close_reason::error);
    } else if (result == packet_parse_result::out_of_memory) {
        LOG(ERROR) << "login parser out of memory slot=" << conn.slot()
                   << " generation=" << conn.generation();
        conn.close(af::net::close_reason::error);
    }
}

void login_on_close(void *owner, af::net::tcp_connection_ref conn,
                    af::net::close_reason reason) noexcept {
    auto *state = static_cast<login_shard_state *>(owner);
    if (state != nullptr) {
        state->parsers.erase(connection_key(conn.handle()));
    }
    LOG(INFO) << "login connection closed slot=" << conn.slot()
              << " generation=" << conn.generation() << " reason=" << static_cast<unsigned>(reason);
}

[[nodiscard]] af::net::tcp_server_config make_server_config() noexcept {
    af::net::tcp_server_config config;
    config.connection.read_budget_bytes = 512U * 1024U;
    config.connection.read_buffer_size = 16U * 1024U;
    config.connection.write_budget_bytes = 512U * 1024U;
    config.connection.output_high_watermark = 8U * 1024U * 1024U;
    config.connection.no_delay = true;
    config.connection.keepalive = true;
    config.connection_close_timeout = std::chrono::seconds(5);
    return config;
}

void start_server_shard(af::net::tcp_server &server, login_shard_state &state,
                        af::thread_ref io_thread, std::uint16_t port, bool ipv6,
                        bool reuse_port) noexcept {
    af::net::tcp_connection_callbacks callbacks;
    callbacks.owner = &state;
    callbacks.on_accept = &login_on_accept;
    callbacks.on_read = &login_on_read;
    callbacks.on_close = &login_on_close;

    af::net::tcp_listener_config listener_config;
    listener_config.name = ipv6 ? "login-v6" : "login-v4";
    listener_config.endpoint =
        ipv6 ? af::net::tcp_endpoint::any_v6(port) : af::net::tcp_endpoint::any(port);
    listener_config.threads = {io_thread};
    listener_config.options.backlog = 4096;
    listener_config.options.reuse_port = reuse_port;
    listener_config.options.ipv6_only = ipv6;
    listener_config.options.accept_budget = 256;
    listener_config.accept_strategy = af::net::accept_strategy::single_acceptor;

    const af::net::listener_result listener =
        server.add_listener(std::move(listener_config), callbacks);
    const bool ok = listener.ok() && server.start();
    if (ok) {
        LOG(INFO) << "tcp login shard started thread=" << io_thread.index
                  << " listener_slot=" << listener.listener.slot()
                  << " listener_generation=" << listener.listener.generation();
    } else {
        LOG(ERROR) << "tcp login shard start failed thread=" << io_thread.index
                   << " error=" << (listener.error == 0 ? EIO : listener.error);
    }
    state.lifecycle->record_start(ok, listener.error == 0 ? EIO : listener.error);
}

[[nodiscard]] bool wait_for_shutdown_signal(af::SignalSet &signals,
                                            const server_lifecycle_state &lifecycle) {
    for (;;) {
        if (lifecycle.failed.load(std::memory_order_acquire)) {
            return false;
        }
        const af::SignalWaitResult result = signals.wait_for(std::chrono::seconds(1));
        if (result.ok()) {
            LOG(INFO) << "tcp login received signal=" << result.signal;
            return true;
        }
        if (result.error != EAGAIN) {
            std::cerr << "signal wait failed error=" << result.error << '\n';
            return false;
        }
    }
}

} // namespace

int main(int argc, char **argv) {
    std::uint16_t port = 9091;
    if (argc > 1) {
        port = static_cast<std::uint16_t>(std::strtoul(argv[1], nullptr, 10));
    }
    const bool ipv6 = argc > 2 && std::strcmp(argv[2], "--ipv6") == 0;

    af::SignalSet signals = af::make_termination_signal_set();
    if (!signals.valid()) {
        std::cerr << "failed to install signal set error=" << signals.error() << '\n';
        return 1;
    }
    static_cast<void>(af::ignore_process_signal(SIGPIPE));

    af::runtime_config runtime_config;
    runtime_config.threads = {af::io_threads("login-io", 2), af::cpu_threads("login-cpu", 1)};
    runtime_config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(runtime_config);
    if (!runtime.start()) {
        std::cerr << "failed to start runtime\n";
        return 1;
    }
    if (!wait_until([&] { return runtime.active_thread_count() == runtime.thread_count(); })) {
        std::cerr << "runtime did not start all threads\n";
        runtime.stop();
        return 1;
    }

    const af::thread_group_ref io_threads = runtime.io_threads();
    const af::thread_ref cpu_thread = runtime.cpu_threads().front();
    const bool reuse_port = io_threads.size() > 1U;
    auto lifecycle = std::make_shared<server_lifecycle_state>();
    std::vector<std::unique_ptr<af::net::tcp_server>> servers;
    std::vector<std::unique_ptr<login_shard_state>> shard_states;

    try {
        servers.reserve(io_threads.size());
        shard_states.reserve(io_threads.size());
        const af::net::tcp_server_config server_config = make_server_config();
        for (std::size_t i = 0; i < io_threads.size(); ++i) {
            servers.push_back(std::make_unique<af::net::tcp_server>(runtime, server_config));
            auto state = std::make_unique<login_shard_state>();
            state->runtime = &runtime;
            state->cpu_thread = cpu_thread;
            state->lifecycle = lifecycle;
            shard_states.push_back(std::move(state));
        }
    } catch (...) {
        std::cerr << "failed to allocate tcp login server shards\n";
        runtime.stop();
        return 1;
    }

    for (std::size_t i = 0; i < io_threads.size(); ++i) {
        const af::thread_ref io_thread = io_threads[i];
        af::net::tcp_server *server = servers[i].get();
        login_shard_state *state = shard_states[i].get();
        if (!runtime.post(io_thread, [server, state, io_thread, port, ipv6, reuse_port] {
                start_server_shard(*server, *state, io_thread, port, ipv6, reuse_port);
            })) {
            lifecycle->record_start(false, EIO);
        }
    }

    const bool started = wait_until([&] {
        return lifecycle->start_attempts.load(std::memory_order_acquire) == io_threads.size();
    });
    if (!started || lifecycle->failed.load(std::memory_order_acquire)) {
        std::cerr << "failed to start tcp login server error="
                  << lifecycle->error.load(std::memory_order_acquire) << '\n';
        runtime.stop();
        return 1;
    }

    std::cout << "tcp login server listening on " << std::string_view(ipv6 ? "[::]" : "0.0.0.0")
              << ':' << port << " with " << io_threads.size() << " io shard(s)\n";
    static_cast<void>(wait_for_shutdown_signal(signals, *lifecycle));

    for (std::size_t i = 0; i < io_threads.size(); ++i) {
        const af::thread_ref io_thread = io_threads[i];
        af::net::tcp_server *server = servers[i].get();
        if (!runtime.post(io_thread, [server, lifecycle] {
                if (server == nullptr || !server->stop()) {
                    LOG(ERROR) << "tcp login server shard stop failed";
                } else {
                    LOG(INFO) << "tcp login shard stopped";
                }
                lifecycle->record_stop();
            })) {
            lifecycle->record_stop();
        }
    }
    static_cast<void>(wait_until(
        [&] { return lifecycle->stopped.load(std::memory_order_acquire) == io_threads.size(); }));
    runtime.stop();
    return lifecycle->failed.load(std::memory_order_acquire) ? 1 : 0;
}
