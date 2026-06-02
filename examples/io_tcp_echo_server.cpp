#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>
#include <utility>

#include "support/io_tcp_echo_client_task.hpp"
#include "support/io_tcp_echo_server_task.hpp"
#include "support/io_tcp_echo_sockets.hpp"

int main(int argc, char **argv) {
#if !defined(_WIN32)
    using namespace std::chrono_literals;
    using namespace io_tcp_echo_example;

    const std::filesystem::path log_path = argc > 1 ? argv[1] : "asyncflow-tcp-echo-server.log";
    std::filesystem::remove(log_path);

    echo_async::init();

    af::AsyncLogConfig log_config;
    log_config.queue_capacity = 1024;
    log_config.runtime_queue_capacity = 1024;
    log_config.max_batch_size = 64;
    log_config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    log_config.backends.push_back(af::make_file_log_backend({.path = log_path, .append = false}));
    auto logging = af::start_async_logging_for_runtime<echo_async>(std::move(log_config),
                                                                   EchoThreads::Compute_0);

    bool log_flushed = false;
    af::AsyncLogStats log_stats;
    auto flush_logging = [&] {
        log_flushed = logging->flush(2s);
        log_stats = logging->stats();
        logging->stop();
        return log_flushed && log_stats.dropped == 0U;
    };

    LOG(INFO) << "tcp echo server example starting log=" << log_path.string();
    if (!echo_async::io_backend_available(EchoThreads::IO_0) ||
        !echo_async::io_backend_available(EchoThreads::IO_1)) {
        LOG(ERROR) << "tcp echo IO backend unavailable";
        std::cout << "IO backend unavailable\n";
        const bool logs_ok = flush_logging();
        echo_async::shutdown();
        return logs_ok ? 0 : 1;
    }

    std::cout << "tcp echo backends=" << echo_backend_name(EchoThreads::IO_0) << ','
              << echo_backend_name(EchoThreads::IO_1) << '\n';
    LOG(INFO) << "tcp echo backends io0=" << echo_backend_name(EchoThreads::IO_0)
              << " io1=" << echo_backend_name(EchoThreads::IO_1);

    EchoLoopbackListener listener{};
    if (!listener.create()) {
        LOG(ERROR) << "tcp echo listener failed";
        std::cout << "tcp echo listener failed\n";
        const bool logs_ok = flush_logging();
        echo_async::shutdown();
        return logs_ok ? 1 : 2;
    }

    std::array<EchoSessionResult, echo_client_count> sessions{};
    std::array<EchoClientResult, echo_client_count> clients{};
    bool server_ok = false;
    int server_error = 0;
    std::atomic<bool> server_completed{false};

    const bool server_started =
        echo_async::start_task<EchoServerTask>(listener.fd.get(), sessions.data(), sessions.size(),
                                               &server_ok, &server_error, &server_completed);

    constexpr EchoPayload request0{{'H', 'E', 'L', 'L', 'O', 'I', 'O', 'A'}};
    constexpr EchoPayload request1{{'A', 'S', 'Y', 'N', 'C', 'I', 'O', 'B'}};
    constexpr EchoPayload expected0{{'h', 'e', 'l', 'l', 'o', 'i', 'o', 'a'}};
    constexpr EchoPayload expected1{{'a', 's', 'y', 'n', 'c', 'i', 'o', 'b'}};

    af::UniqueFd client0 = echo_make_tcp_socket();
    af::UniqueFd client1 = echo_make_tcp_socket();
    const bool clients_ready = static_cast<bool>(client0) && static_cast<bool>(client1);
    const bool client0_started =
        clients_ready && echo_async::start_task<EchoClientTask>(
                             std::move(client0), EchoThreads::IO_0, listener.address,
                             listener.address_size, request0, &clients[0]);
    const bool client1_started =
        clients_ready && echo_async::start_task<EchoClientTask>(
                             std::move(client1), EchoThreads::IO_1, listener.address,
                             listener.address_size, request1, &clients[1]);

    AF_ASSERT(server_started && client0_started && client1_started);
    if (!server_started || !client0_started || !client1_started) {
        LOG(ERROR) << "tcp echo task start failed server_started=" << server_started
                   << " client0_started=" << client0_started
                   << " client1_started=" << client1_started;
        std::cout << "tcp echo task start failed\n";
        const bool logs_ok = flush_logging();
        echo_async::shutdown();
        return logs_ok ? 1 : 2;
    }

    const auto deadline = std::chrono::steady_clock::now() + 5s;
    bool completed = false;
    while (std::chrono::steady_clock::now() < deadline) {
        completed = server_completed.load(std::memory_order_acquire);
        for (const EchoSessionResult &session : sessions) {
            completed = completed && session.completed.load(std::memory_order_acquire);
        }
        for (const EchoClientResult &client : clients) {
            completed = completed && client.completed.load(std::memory_order_acquire);
        }
        if (completed) {
            break;
        }
        std::this_thread::sleep_for(1ms);
    }

    LOG(INFO) << "tcp echo tasks completed=" << completed << " server_ok=" << server_ok
              << " server_error=" << server_error << " client0_ok=" << clients[0].ok
              << " client1_ok=" << clients[1].ok;

    const bool logs_ok = flush_logging();
    echo_async::shutdown();

    const bool ok = completed && server_ok && server_error == 0 && sessions[0].ok &&
                    sessions[1].ok && clients[0].ok && clients[1].ok &&
                    clients[0].response == expected0 && clients[1].response == expected1 && logs_ok;
    if (!ok) {
        std::cout << "tcp echo failed: server_error=" << server_error
                  << " client0_error=" << clients[0].error << " client1_error=" << clients[1].error
                  << '\n';
        return 1;
    }

    std::cout << "echo0=";
    for (char ch : clients[0].response) {
        std::cout << ch;
    }
    std::cout << " echo1=";
    for (char ch : clients[1].response) {
        std::cout << ch;
    }
    std::cout << " log=" << log_path << " log_accepted=" << log_stats.accepted
              << " log_dropped=" << log_stats.dropped << " log_flushed=" << log_flushed << '\n';
    return 0;
#else
    std::cout << "tcp echo server example requires POSIX sockets\n";
    return 0;
#endif
}
