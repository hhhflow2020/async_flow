#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "af/async_runtime.hpp"
#include "af/log.hpp"
#include "af/net.hpp"
#include "login.pb.h"

#include <arpa/inet.h>

namespace {

constexpr std::uint16_t login_request_id = 1;
constexpr std::uint16_t login_response_id = 2;
constexpr std::size_t packet_header_size = sizeof(std::uint32_t) + sizeof(std::uint16_t);

struct LoginIoThreadTag;
struct LoginComputeThreadTag;

struct LoginRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<LoginIoThreadTag, 2, af::ThreadKind::Epoll, "login-io">(),
        af::thread_group<LoginComputeThreadTag, 1, af::ThreadKind::Worker, "login-cpu">());
    static constexpr std::size_t spsc_queue_capacity = 4096;
    static constexpr std::size_t external_queue_capacity = 4096;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
};

using LoginRuntime = af::AsyncRuntime<LoginRuntimeTraits>;

struct LoginHandler;
using LoginConnectionHandle = af::net::TcpConnectionHandle<LoginRuntime, LoginHandler>;

std::atomic<bool> stop_requested{false};

void handle_signal(int) {
    stop_requested.store(true, std::memory_order_release);
}

[[nodiscard]] std::vector<std::byte> make_packet(std::uint16_t packet_id, std::string payload) {
    const std::uint32_t length = static_cast<std::uint32_t>(sizeof(std::uint16_t) + payload.size());
    const std::uint32_t network_length = htonl(length);
    const std::uint16_t network_id = htons(packet_id);
    std::vector<std::byte> packet(packet_header_size + payload.size());
    std::memcpy(packet.data(), &network_length, sizeof(network_length));
    std::memcpy(packet.data() + sizeof(network_length), &network_id, sizeof(network_id));
    if (!payload.empty()) {
        std::memcpy(packet.data() + packet_header_size, payload.data(), payload.size());
    }
    return packet;
}

struct StreamParser {
    std::vector<std::byte> buffered;

    template <typename Fn> void feed(af::BufferView bytes, Fn &&fn) {
        const auto old_size = buffered.size();
        buffered.resize(old_size + bytes.size());
        std::memcpy(buffered.data() + old_size, bytes.data(), bytes.size());

        std::size_t offset = 0;
        while (buffered.size() - offset >= packet_header_size) {
            std::uint32_t network_length = 0;
            std::uint16_t network_id = 0;
            std::memcpy(&network_length, buffered.data() + offset, sizeof(network_length));
            const std::uint32_t length = ntohl(network_length);
            if (length < sizeof(std::uint16_t)) {
                buffered.clear();
                return;
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
    }
};

class LoginTask final : public LoginRuntime::Task {
public:
    explicit LoginTask(LoginRuntime::Task::FactoryToken token) : LoginRuntime::Task(token) {}

    bool do_it(LoginConnectionHandle conn, std::string user_id) {
        conn_ = std::move(conn);
        user_id_ = std::move(user_id);
        return schedule(LoginRuntime::thread_group<LoginComputeThreadTag>().template at<0>());
    }

private:
    af::TaskResult run() override {
        LOG(INFO) << "login task running user=" << user_id_;
        asyncflow::examples::net::LoginResponse response;
        response.set_ok(true);
        response.set_message("hello " + user_id_);
        std::string payload;
        if (!response.SerializeToString(&payload)) {
            LOG(ERROR) << "failed to serialize login response user=" << user_id_;
            return done();
        }
        const std::vector<std::byte> packet = make_packet(login_response_id, std::move(payload));
        static_cast<void>(conn_.send(af::Buffer::copy(packet.data(), packet.size())));
        return done();
    }

    LoginConnectionHandle conn_;
    std::string user_id_;
};

struct LoginHandler {
    absl::flat_hash_map<std::uint64_t, StreamParser> parsers;

    void on_read(af::net::TcpConnectionRef<LoginRuntime, LoginHandler> conn, af::BufferView bytes) {
        const auto handle = conn.handle();
        const std::uint64_t key =
            (static_cast<std::uint64_t>(handle.generation()) << 32U) | handle.slot();
        StreamParser &parser = parsers[key];
        parser.feed(bytes, [&](std::uint16_t packet_id, af::BufferView payload) {
            if (packet_id != login_request_id) {
                return;
            }
            asyncflow::examples::net::LoginRequest request;
            if (!request.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
                return;
            }
            static_cast<void>(
                LoginRuntime::start_task<LoginTask>(handle, std::string(request.user_id())));
        });
    }

    void on_close(af::net::TcpConnectionHandle<LoginRuntime, LoginHandler> conn,
                  af::net::CloseReason reason) {
        const std::uint64_t key =
            (static_cast<std::uint64_t>(conn.generation()) << 32U) | conn.slot();
        parsers.erase(key);
        LOG(INFO) << "login connection closed slot=" << conn.slot()
                  << " generation=" << conn.generation()
                  << " reason=" << static_cast<unsigned>(reason);
    }
};

} // namespace

int main(int argc, char **argv) {
    std::uint16_t port = 9091;
    if (argc > 1) {
        port = static_cast<std::uint16_t>(std::strtoul(argv[1], nullptr, 10));
    }

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    std::signal(SIGPIPE, SIG_IGN);

    LoginRuntime::init();
    af::net::TcpServer<LoginRuntime, LoginHandler> server({
        .threads =
            af::net::thread_list<LoginRuntime>(LoginRuntime::thread_group<LoginIoThreadTag>()),
        .endpoint = af::net::TcpEndpoint::any(port),
        .options = {.reuse_port = true},
    });

    if (!server.start()) {
        std::cerr << "failed to start tcp login server\n";
        LoginRuntime::shutdown();
        return 1;
    }

    std::cout << "tcp login server listening on 0.0.0.0:" << port << '\n';
    while (!stop_requested.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LoginRuntime::shutdown();
    return 0;
}
