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
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "af/async_runtime.hpp"
#include "af/log.hpp"
#include "af/net.hpp"
#include "af/platform.hpp"
#include "af/signal.hpp"
#include "login.pb.h"

#include <arpa/inet.h>

namespace {

constexpr std::uint16_t login_request_id = 1;
constexpr std::uint16_t login_response_id = 2;
constexpr std::size_t packet_header_size = sizeof(std::uint32_t) + sizeof(std::uint16_t);
constexpr std::size_t max_packet_payload_size = 64U * 1024U;

struct LoginIoThreadTag;
struct LoginComputeThreadTag;

struct LoginRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<LoginIoThreadTag, 2, af::thread_kind::io>("login-io"),
        af::thread_group<LoginComputeThreadTag, 1, af::thread_kind::cpu>("login-cpu"));
    static constexpr af::shutdown_policy shutdown_policy = af::shutdown_policy::wait_for_tasks;
};

using LoginRuntime = af::AsyncRuntime<LoginRuntimeTraits>;
using LoginConnectionHandle = af::net::tcp_connection_handle<LoginRuntime>;

struct ServerLifecycleState {
    std::atomic<bool> started{false};
    std::atomic<bool> failed{false};
    std::atomic<int> error{0};

    void record_failure(int err) noexcept {
        error.store(err == 0 ? EIO : err, std::memory_order_release);
        failed.store(true, std::memory_order_release);
    }
};

[[nodiscard]] std::vector<LoginRuntime::Thread> login_io_threads() {
    return af::net::thread_list<LoginRuntime>(LoginRuntime::thread_group<LoginIoThreadTag>());
}

[[nodiscard]] LoginRuntime::Thread login_control_thread() {
    return LoginRuntime::thread_group<LoginIoThreadTag>().template at<0>();
}

[[nodiscard]] std::uint64_t connection_key(std::uint32_t slot, std::uint32_t generation) noexcept {
    return (static_cast<std::uint64_t>(generation) << 32U) | slot;
}

[[nodiscard]] std::uint64_t connection_key(LoginConnectionHandle conn) noexcept {
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

enum class PacketParseResult : std::uint8_t {
    Ok,
    ProtocolError,
    OutOfMemory,
};

struct StreamParser {
    std::vector<std::byte> buffered;

    template <typename Fn> [[nodiscard]] PacketParseResult feed(af::BufferView bytes, Fn &&fn) {
        const auto old_size = buffered.size();
        try {
            buffered.resize(old_size + bytes.size());
        } catch (...) {
            return PacketParseResult::OutOfMemory;
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
                return PacketParseResult::ProtocolError;
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
            buffered.erase(buffered.begin(),
                           buffered.begin() + static_cast<std::ptrdiff_t>(offset));
        }
        return PacketParseResult::Ok;
    }
};

class LoginTask final : public LoginRuntime::Task {
public:
    explicit LoginTask(LoginRuntime::Task::FactoryToken token) : LoginRuntime::Task(token) {}

    bool do_it(LoginConnectionHandle conn, std::string user_id, std::string token) {
        conn_ = conn;
        user_id_ = std::move(user_id);
        token_ = std::move(token);
        return schedule_to(LoginRuntime::thread_group<LoginComputeThreadTag>().template at<0>());
    }

private:
    af::task_result run() override {
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

    LoginConnectionHandle conn_;
    std::string user_id_;
    std::string token_;
};

struct LoginHandler {
    std::shared_ptr<ServerLifecycleState> lifecycle;
    absl::flat_hash_map<std::uint64_t, StreamParser> parsers;

    void on_accept(af::net::tcp_connection_ref<LoginRuntime> conn) {
        LOG(INFO) << "login connection accepted listener=" << conn.listener_name()
                  << " slot=" << conn.slot() << " generation=" << conn.generation();
    }

    void on_read(af::net::tcp_connection_ref<LoginRuntime> conn, af::BufferView bytes) {
        const LoginConnectionHandle handle = conn.handle();
        StreamParser &parser = parsers[connection_key(handle)];
        const PacketParseResult result =
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
                if (!LoginRuntime::start_task<LoginTask>(handle, std::string(request.user_id()),
                                                         std::string(request.token()))) {
                    LOG(ERROR) << "failed to schedule login task user=" << request.user_id()
                               << " slot=" << handle.slot();
                    static_cast<void>(handle.close());
                }
            });

        if (result == PacketParseResult::ProtocolError) {
            LOG(WARNING) << "login protocol error slot=" << conn.slot()
                         << " generation=" << conn.generation();
            conn.close(af::net::close_reason::error);
        } else if (result == PacketParseResult::OutOfMemory) {
            LOG(ERROR) << "login parser out of memory slot=" << conn.slot()
                       << " generation=" << conn.generation();
            conn.close(af::net::close_reason::error);
        }
    }

    void on_close(af::net::tcp_connection_handle<LoginRuntime> conn, af::net::close_reason reason) {
        parsers.erase(connection_key(conn));
        LOG(INFO) << "login connection closed slot=" << conn.slot()
                  << " generation=" << conn.generation()
                  << " reason=" << static_cast<unsigned>(reason);
    }

    void on_error(af::net::tcp_listener_handle listener, int error) noexcept {
        LOG(ERROR) << "login listener error slot=" << listener.slot()
                   << " generation=" << listener.generation() << " error=" << error;
        if (lifecycle != nullptr) {
            lifecycle->record_failure(error);
        }
    }
};

class StartServerTask final : public LoginRuntime::Task {
public:
    explicit StartServerTask(LoginRuntime::Task::FactoryToken token) : LoginRuntime::Task(token) {}

    bool do_it(af::net::tcp_server<LoginRuntime> *server, std::uint16_t port, bool ipv6,
               std::shared_ptr<ServerLifecycleState> lifecycle) {
        server_ = server;
        port_ = port;
        ipv6_ = ipv6;
        lifecycle_ = std::move(lifecycle);
        return schedule_to_ordered(login_control_thread());
    }

private:
    af::task_result run() override {
        if (server_ == nullptr) {
            if (lifecycle_ != nullptr) {
                lifecycle_->record_failure(EINVAL);
            }
            return done();
        }

        const af::net::listener_result listener = server_->add_listener<LoginHandler>(
            {
                .name = ipv6_ ? "login-v6" : "login-v4",
                .endpoint = ipv6_ ? af::net::tcp_endpoint::any_v6(port_)
                                  : af::net::tcp_endpoint::any(port_),
                .threads = login_io_threads(),
                .options =
                    {
                        .backlog = 4096,
                        .reuse_port = true,
                        .ipv6_only = true,
                        .accept_budget = 256,
                    },
                .accept_strategy = af::net::accept_strategy::reuse_port_per_io_thread,
            },
            LoginHandler{lifecycle_});
        if (!listener.ok()) {
            if (lifecycle_ != nullptr) {
                lifecycle_->record_failure(listener.error);
            }
            return done();
        }

        if (!server_->start()) {
            if (lifecycle_ != nullptr) {
                lifecycle_->record_failure(EIO);
            }
            return done();
        }

        LOG(INFO) << "tcp login start submitted listener_slot=" << listener.listener.slot()
                  << " listener_generation=" << listener.listener.generation();
        if (lifecycle_ != nullptr) {
            lifecycle_->started.store(true, std::memory_order_release);
        }
        return done();
    }

    af::net::tcp_server<LoginRuntime> *server_{nullptr};
    std::shared_ptr<ServerLifecycleState> lifecycle_;
    std::uint16_t port_{0};
    bool ipv6_{false};
};

class StopServerTask final : public LoginRuntime::Task {
public:
    explicit StopServerTask(LoginRuntime::Task::FactoryToken token) : LoginRuntime::Task(token) {}

    bool do_it(af::net::tcp_server<LoginRuntime> *server) {
        server_ = server;
        return schedule_to_ordered(login_control_thread());
    }

private:
    af::task_result run() override {
        if (server_ == nullptr || !server_->stop()) {
            LOG(ERROR) << "tcp login server stop failed";
        } else {
            LOG(INFO) << "tcp login stop submitted";
        }
        return done();
    }

    af::net::tcp_server<LoginRuntime> *server_{nullptr};
};

[[nodiscard]] bool wait_for_shutdown_signal(af::SignalSet &signals,
                                            const ServerLifecycleState &lifecycle) {
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

    LoginRuntime::init();

    af::net::tcp_server_config server_config;
    server_config.connection.read_budget_bytes = 512U * 1024U;
    server_config.connection.read_buffer_size = 16U * 1024U;
    server_config.connection.write_budget_bytes = 512U * 1024U;
    server_config.connection.output_high_watermark = 8U * 1024U * 1024U;
    server_config.connection.no_delay = true;
    server_config.connection.keepalive = true;
    server_config.connection_close_timeout = std::chrono::seconds(5);

    af::net::tcp_server<LoginRuntime> server(server_config);
    auto lifecycle = std::make_shared<ServerLifecycleState>();
    if (!LoginRuntime::start_task<StartServerTask>(&server, port, ipv6, lifecycle)) {
        std::cerr << "failed to schedule tcp login server start\n";
        LoginRuntime::shutdown();
        return 1;
    }

    LoginRuntime::wait_for_idle();
    if (!lifecycle->started.load(std::memory_order_acquire) ||
        lifecycle->failed.load(std::memory_order_acquire)) {
        std::cerr << "failed to start tcp login server error="
                  << lifecycle->error.load(std::memory_order_acquire) << '\n';
        LoginRuntime::shutdown();
        return 1;
    }

    std::cout << "tcp login server listening on " << std::string_view(ipv6 ? "[::]" : "0.0.0.0")
              << ':' << port << '\n';
    static_cast<void>(wait_for_shutdown_signal(signals, *lifecycle));

    if (!LoginRuntime::start_task<StopServerTask>(&server)) {
        std::cerr << "failed to schedule tcp login server stop\n";
    }
    LoginRuntime::wait_for_idle();
    LoginRuntime::shutdown();
    return lifecycle->failed.load(std::memory_order_acquire) ? 1 : 0;
}
