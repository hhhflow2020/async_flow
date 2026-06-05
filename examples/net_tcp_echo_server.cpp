#include <atomic>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "af/async_runtime.hpp"
#include "af/log.hpp"
#include "af/net.hpp"
#include "af/platform.hpp"
#include "af/signal.hpp"

namespace {

struct EchoIoThreadTag;
struct EchoComputeThreadTag;

struct EchoRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<EchoIoThreadTag, 2, af::thread_kind::io>("echo-io"),
        af::thread_group<EchoComputeThreadTag, 1, af::thread_kind::cpu>("echo-cpu"));
    static constexpr af::shutdown_policy shutdown_policy = af::shutdown_policy::wait_for_tasks;
};

using EchoRuntime = af::AsyncRuntime<EchoRuntimeTraits>;
using EchoConnectionHandle = af::net::TcpConnectionHandle<EchoRuntime>;

struct ServerLifecycleState {
    std::atomic<bool> started{false};
    std::atomic<bool> failed{false};
    std::atomic<int> error{0};

    void record_failure(int err) noexcept {
        error.store(err == 0 ? EIO : err, std::memory_order_release);
        failed.store(true, std::memory_order_release);
    }
};

[[nodiscard]] std::vector<EchoRuntime::Thread> echo_io_threads() {
    return af::net::thread_list<EchoRuntime>(EchoRuntime::thread_group<EchoIoThreadTag>());
}

[[nodiscard]] EchoRuntime::Thread echo_control_thread() {
    return EchoRuntime::thread_group<EchoIoThreadTag>().template at<0>();
}

class LowercaseEchoTask final : public EchoRuntime::Task {
public:
    explicit LowercaseEchoTask(EchoRuntime::Task::FactoryToken token) : EchoRuntime::Task(token) {}

    bool do_it(EchoConnectionHandle conn, af::Buffer payload) {
        conn_ = conn;
        payload_ = std::move(payload);
        return schedule_to(EchoRuntime::thread_group<EchoComputeThreadTag>().template at<0>());
    }

private:
    af::task_result run() override {
        auto *data = payload_.mutable_data();
        const std::size_t size = payload_.size();
        for (std::size_t i = 0; i < size; ++i) {
            const auto ch = static_cast<unsigned char>(data[i]);
            data[i] = static_cast<std::byte>(
                static_cast<unsigned char>(std::tolower(static_cast<int>(ch))));
        }

        LOG(INFO) << "tcp echo lowercased bytes=" << size << " slot=" << conn_.slot()
                  << " generation=" << conn_.generation();
        const af::net::send_result result = conn_.send(std::move(payload_));
        if (result == af::net::send_result::backpressure) {
            LOG(WARNING) << "tcp echo send backpressure slot=" << conn_.slot()
                         << " generation=" << conn_.generation();
        } else if (result == af::net::send_result::closed) {
            LOG(INFO) << "tcp echo send skipped closed slot=" << conn_.slot()
                      << " generation=" << conn_.generation();
        }
        return done();
    }

    EchoConnectionHandle conn_;
    af::Buffer payload_;
};

struct EchoHandler {
    std::shared_ptr<ServerLifecycleState> lifecycle;

    void on_accept(af::net::TcpConnectionRef<EchoRuntime> conn) noexcept {
        LOG(INFO) << "tcp echo accepted listener=" << conn.listener_name()
                  << " slot=" << conn.slot() << " generation=" << conn.generation();
    }

    void on_read(af::net::TcpConnectionRef<EchoRuntime> conn, af::BufferView bytes) noexcept {
        LOG(INFO) << "tcp echo received bytes=" << bytes.size() << " slot=" << conn.slot()
                  << " generation=" << conn.generation();
        if (bytes.empty()) {
            return;
        }

        af::Buffer payload;
        try {
            payload = af::Buffer::copy(bytes);
        } catch (...) {
            LOG(ERROR) << "tcp echo failed to copy payload slot=" << conn.slot()
                       << " generation=" << conn.generation();
            conn.close(af::net::close_reason::error);
            return;
        }

        if (!EchoRuntime::start_task<LowercaseEchoTask>(conn.handle(), std::move(payload))) {
            LOG(ERROR) << "tcp echo failed to schedule compute task slot=" << conn.slot()
                       << " generation=" << conn.generation();
            conn.close(af::net::close_reason::error);
        }
    }

    void on_close(af::net::TcpConnectionHandle<EchoRuntime> conn,
                  af::net::close_reason reason) noexcept {
        LOG(INFO) << "tcp echo closed slot=" << conn.slot() << " generation=" << conn.generation()
                  << " reason=" << static_cast<unsigned>(reason);
    }

    void on_error(af::net::tcp_listener_handle listener, int error) noexcept {
        LOG(ERROR) << "tcp echo listener error slot=" << listener.slot()
                   << " generation=" << listener.generation() << " error=" << error;
        if (lifecycle != nullptr) {
            lifecycle->record_failure(error);
        }
    }
};

class StartServerTask final : public EchoRuntime::Task {
public:
    explicit StartServerTask(EchoRuntime::Task::FactoryToken token) : EchoRuntime::Task(token) {}

    bool do_it(af::net::TcpServer<EchoRuntime> *server, std::uint16_t port, bool ipv6,
               std::shared_ptr<ServerLifecycleState> lifecycle) {
        server_ = server;
        port_ = port;
        ipv6_ = ipv6;
        lifecycle_ = std::move(lifecycle);
        return schedule_to_ordered(echo_control_thread());
    }

private:
    af::task_result run() override {
        if (server_ == nullptr) {
            if (lifecycle_ != nullptr) {
                lifecycle_->record_failure(EINVAL);
            }
            return done();
        }

        const af::net::listener_result listener = server_->add_listener<EchoHandler>(
            {
                .name = ipv6_ ? "echo-v6" : "echo-v4",
                .endpoint = ipv6_ ? af::net::tcp_endpoint::any_v6(port_)
                                  : af::net::tcp_endpoint::any(port_),
                .threads = echo_io_threads(),
                .options =
                    {
                        .backlog = 4096,
                        .reuse_port = true,
                        .ipv6_only = true,
                        .accept_budget = 256,
                    },
                .accept_strategy = af::net::accept_strategy::reuse_port_per_io_thread,
            },
            EchoHandler{lifecycle_});
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

        LOG(INFO) << "tcp echo start submitted listener_slot=" << listener.listener.slot()
                  << " listener_generation=" << listener.listener.generation();
        if (lifecycle_ != nullptr) {
            lifecycle_->started.store(true, std::memory_order_release);
        }
        return done();
    }

    af::net::TcpServer<EchoRuntime> *server_{nullptr};
    std::shared_ptr<ServerLifecycleState> lifecycle_;
    std::uint16_t port_{0};
    bool ipv6_{false};
};

class StopServerTask final : public EchoRuntime::Task {
public:
    explicit StopServerTask(EchoRuntime::Task::FactoryToken token) : EchoRuntime::Task(token) {}

    bool do_it(af::net::TcpServer<EchoRuntime> *server) {
        server_ = server;
        return schedule_to_ordered(echo_control_thread());
    }

private:
    af::task_result run() override {
        if (server_ == nullptr || !server_->stop()) {
            LOG(ERROR) << "tcp echo server stop failed";
        } else {
            LOG(INFO) << "tcp echo stop submitted";
        }
        return done();
    }

    af::net::TcpServer<EchoRuntime> *server_{nullptr};
};

[[nodiscard]] bool wait_for_shutdown_signal(af::SignalSet &signals,
                                            const ServerLifecycleState &lifecycle) {
    for (;;) {
        if (lifecycle.failed.load(std::memory_order_acquire)) {
            return false;
        }
        const af::SignalWaitResult result = signals.wait_for(std::chrono::seconds(1));
        if (result.ok()) {
            LOG(INFO) << "tcp echo received signal=" << result.signal;
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
    std::uint16_t port = 9090;
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

    EchoRuntime::init();

    af::net::tcp_server_config server_config;
    server_config.connection.read_budget_bytes = 512U * 1024U;
    server_config.connection.read_buffer_size = 16U * 1024U;
    server_config.connection.write_budget_bytes = 512U * 1024U;
    server_config.connection.output_high_watermark = 8U * 1024U * 1024U;
    server_config.connection.no_delay = true;
    server_config.connection.keepalive = true;
    server_config.connection_close_timeout = std::chrono::seconds(5);

    af::net::TcpServer<EchoRuntime> server(server_config);
    auto lifecycle = std::make_shared<ServerLifecycleState>();
    if (!EchoRuntime::start_task<StartServerTask>(&server, port, ipv6, lifecycle)) {
        std::cerr << "failed to schedule tcp echo server start\n";
        EchoRuntime::shutdown();
        return 1;
    }

    EchoRuntime::wait_for_idle();
    if (!lifecycle->started.load(std::memory_order_acquire) ||
        lifecycle->failed.load(std::memory_order_acquire)) {
        std::cerr << "failed to start tcp echo server error="
                  << lifecycle->error.load(std::memory_order_acquire) << '\n';
        EchoRuntime::shutdown();
        return 1;
    }

    std::cout << "tcp echo server listening on " << std::string_view(ipv6 ? "[::]" : "0.0.0.0")
              << ':' << port << '\n';
    static_cast<void>(wait_for_shutdown_signal(signals, *lifecycle));

    if (!EchoRuntime::start_task<StopServerTask>(&server)) {
        std::cerr << "failed to schedule tcp echo server stop\n";
    }
    EchoRuntime::wait_for_idle();
    EchoRuntime::shutdown();
    return lifecycle->failed.load(std::memory_order_acquire) ? 1 : 0;
}
