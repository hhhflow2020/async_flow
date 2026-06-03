#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>

#include "af/async_runtime.hpp"
#include "af/log.hpp"
#include "af/net.hpp"
#include "af/platform.hpp"

namespace {

struct EchoIoThreadTag;

struct EchoRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<EchoIoThreadTag, 2, af::preferred_io_thread_kind, "echo-io">());
    static constexpr std::size_t spsc_queue_capacity = 4096;
    static constexpr std::size_t external_queue_capacity = 4096;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using EchoRuntime = af::AsyncRuntime<EchoRuntimeTraits>;

std::atomic<bool> stop_requested{false};

void handle_signal(int) {
    stop_requested.store(true, std::memory_order_release);
}

struct EchoHandler {
    void on_accept(af::net::TcpConnectionRef<EchoRuntime, EchoHandler> conn) noexcept {
        LOG(INFO) << "tcp echo accepted slot=" << conn.handle().slot()
                  << " generation=" << conn.handle().generation();
    }

    void on_read(af::net::TcpConnectionRef<EchoRuntime, EchoHandler> conn,
                 af::BufferView bytes) noexcept {
        static_cast<void>(conn.send(bytes));
    }

    void on_close(af::net::TcpConnectionHandle<EchoRuntime, EchoHandler> conn,
                  af::net::CloseReason reason) noexcept {
        LOG(INFO) << "tcp echo closed slot=" << conn.slot() << " generation=" << conn.generation()
                  << " reason=" << static_cast<unsigned>(reason);
    }
};

} // namespace

int main(int argc, char **argv) {
    std::uint16_t port = 9090;
    if (argc > 1) {
        port = static_cast<std::uint16_t>(std::strtoul(argv[1], nullptr, 10));
    }
    const bool ipv6 = argc > 2 && std::strcmp(argv[2], "--ipv6") == 0;

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    std::signal(SIGPIPE, SIG_IGN);

    EchoRuntime::init();

    af::net::TcpServer<EchoRuntime, EchoHandler> server({
        .threads = af::net::thread_list<EchoRuntime>(EchoRuntime::thread_group<EchoIoThreadTag>()),
        .endpoint = ipv6 ? af::net::TcpEndpoint::any_v6(port) : af::net::TcpEndpoint::any(port),
        .options = {.reuse_port = true},
    });

    if (!server.start()) {
        std::cerr << "failed to start tcp echo server\n";
        EchoRuntime::shutdown();
        return 1;
    }

    std::cout << "tcp echo server listening on " << (ipv6 ? "[::]" : "0.0.0.0") << ':' << port
              << '\n';
    while (!stop_requested.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    static_cast<void>(server.stop());
    EchoRuntime::wait_for_idle();
    EchoRuntime::shutdown();
    return 0;
}
