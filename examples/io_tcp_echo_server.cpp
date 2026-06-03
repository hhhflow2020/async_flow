#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <system_error>
#include <thread>

#include "support/io_tcp_echo_client_task.hpp"
#include "support/io_tcp_echo_server_cli.hpp"
#include "support/io_tcp_echo_server_task.hpp"
#include "support/io_tcp_echo_sockets.hpp"

#include "absl/base/log_severity.h"
#include "absl/log/globals.h"

namespace {

[[nodiscard]] absl::LogSeverityAtLeast
echo_absl_min_log_level(io_tcp_echo_example::EchoLogLevel level) noexcept {
    switch (level) {
    case io_tcp_echo_example::EchoLogLevel::Info:
        return absl::LogSeverityAtLeast::kInfo;
    case io_tcp_echo_example::EchoLogLevel::Warning:
        return absl::LogSeverityAtLeast::kWarning;
    case io_tcp_echo_example::EchoLogLevel::Error:
        return absl::LogSeverityAtLeast::kError;
    case io_tcp_echo_example::EchoLogLevel::Fatal:
        return absl::LogSeverityAtLeast::kFatal;
    }
    return absl::LogSeverityAtLeast::kInfo;
}

} // namespace

int main(int argc, char **argv) {
    using namespace std::chrono_literals;
    using namespace io_tcp_echo_example;

    af::SignalSet stop_signals = af::make_termination_signal_set();
    if (!stop_signals.valid()) {
        std::cerr << "failed to configure signal wait set error=" << stop_signals.error() << '\n';
        return 2;
    }
    static_cast<void>(af::ignore_process_signal(SIGPIPE));

    EchoServerOptions options;
    if (!echo_parse_server_options(argc, argv, &options, std::cerr)) {
        echo_print_server_usage(std::cerr);
        return 2;
    }
    if (options.help) {
        echo_print_server_usage(std::cout);
        return 0;
    }

    std::error_code remove_error;
    std::filesystem::remove(options.log_path, remove_error);
    absl::SetMinLogLevel(echo_absl_min_log_level(options.log_level));

    echo_async::init();

    af::AsyncLogConfig log_config = af::AsyncLogConfig::ordered(echo_async::thread_count);
    log_config.queue_capacity = 8192;
    log_config.max_batch_size = 256;
    log_config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    log_config.backends.push_back(
        af::make_file_log_backend({.path = options.log_path, .append = false}));
    auto logging = af::start_async_logging_for_runtime<echo_async>(std::move(log_config),
                                                                   EchoThreads::Compute_0);

    bool logging_stopped = false;
    bool log_flushed = false;
    af::AsyncLogStats log_stats;
    auto stop_logging = [&] {
        if (logging_stopped) {
            return log_flushed && log_stats.dropped == 0U;
        }
        log_flushed = logging->flush(2s);
        log_stats = logging->stats();
        logging->stop();
        logging_stopped = true;
        return log_flushed && log_stats.dropped == 0U;
    };

    auto stop_runtime = [&] {
        const bool logs_ok = stop_logging();
        echo_async::shutdown();
        return logs_ok;
    };

    LOG(INFO) << "tcp echo server starting bind=" << options.bind_address
              << " port=" << options.port << " backlog=" << options.backlog
              << " self_test=" << options.self_test << " log=" << options.log_path.string()
              << " log_level=" << echo_log_level_name(options.log_level);

    if (!echo_async::io_backend_available(EchoThreads::IO_0) ||
        !echo_async::io_backend_available(EchoThreads::IO_1)) {
        LOG(ERROR) << "tcp echo IO backend unavailable";
        std::cout << "IO backend unavailable\n";
        return stop_runtime() ? 0 : 1;
    }

    EchoTcpListener listener{};
    if (!listener.create(options.bind_address.c_str(), options.port, options.backlog)) {
        LOG(ERROR) << "tcp echo listener failed error=" << listener.error;
        std::cout << "tcp echo listener failed error=" << listener.error << '\n';
        return stop_runtime() ? 1 : 2;
    }

    EchoServerState server_state{};
    EchoShutdownNotifier shutdown_notifier;
    if (!shutdown_notifier.open()) {
        LOG(ERROR) << "tcp echo shutdown notifier failed error=" << shutdown_notifier.error;
        std::cout << "tcp echo shutdown notifier failed error=" << shutdown_notifier.error << '\n';
        return stop_runtime() ? 1 : 2;
    }
    server_state.shutdown_notify_fd = shutdown_notifier.write_fd.get();

    const std::uint64_t max_accepts = options.self_test ? echo_self_test_client_count : 0U;
    LOG(INFO) << "tcp echo accept task starting io_thread="
              << echo_async::thread_index(EchoThreads::IO_0) << " max_accepts=" << max_accepts;
    const bool server_started =
        echo_async::start_task<EchoServerTask>(listener.fd.get(), &server_state, max_accepts);
    if (!server_started) {
        LOG(ERROR) << "tcp echo accept task start failed";
        std::cout << "tcp echo accept task start failed\n";
        return stop_runtime() ? 1 : 2;
    }

    const std::string listen_address = echo_listen_address_text(listener.address);
    std::cout << "tcp echo server listening on " << listen_address
              << " backends=" << echo_backend_name(EchoThreads::IO_0) << ','
              << echo_backend_name(EchoThreads::IO_1) << " log=" << options.log_path << '\n';
    LOG(INFO) << "tcp echo server listening address=" << listen_address
              << " io0=" << echo_backend_name(EchoThreads::IO_0)
              << " io1=" << echo_backend_name(EchoThreads::IO_1);

    auto wait_for_shutdown = [&](std::chrono::steady_clock::time_point deadline) {
        return echo_wait_for_shutdown(server_state, shutdown_notifier.read_fd.get(), deadline);
    };

    if (options.self_test) {
        std::array<EchoClientResult, echo_self_test_client_count> clients{};
        constexpr std::array<EchoPayload, echo_self_test_client_count> requests{
            EchoPayload{{'H', 'E', 'L', 'L', 'O', 'I', 'O', 'A'}},
            EchoPayload{{'A', 'S', 'Y', 'N', 'C', 'I', 'O', 'B'}},
        };
        constexpr std::array<EchoPayload, echo_self_test_client_count> expected{
            EchoPayload{{'h', 'e', 'l', 'l', 'o', 'i', 'o', 'a'}},
            EchoPayload{{'a', 's', 'y', 'n', 'c', 'i', 'o', 'b'}},
        };

        bool clients_started = true;
        for (std::size_t index = 0; index < clients.size(); ++index) {
            af::UniqueFd client = echo_make_tcp_socket();
            clients_started = clients_started && static_cast<bool>(client);
            if (client) {
                echo_set_tcp_nodelay(client.get());
                const std::uint64_t client_id = index + 1U;
                LOG(INFO) << "tcp echo self-test client task starting client=" << client_id
                          << " io_thread=" << echo_async::thread_index(echo_io_thread(index));
                clients_started =
                    clients_started &&
                    echo_async::start_task<EchoClientTask>(
                        std::move(client), echo_io_thread(index), listener.address,
                        listener.address_size, requests[index], &clients[index], client_id);
            }
        }

        if (!clients_started) {
            LOG(ERROR) << "tcp echo self-test clients failed to start";
            server_state.stop_requested.store(true, std::memory_order_release);
            echo_wake_listener(listener.address);
            wait_for_shutdown(std::chrono::steady_clock::now() + 1s);
            const bool logs_ok = stop_runtime();
            return logs_ok ? 1 : 2;
        }

        const auto deadline = std::chrono::steady_clock::now() + 5s;
        bool stop_requested = false;
        bool completed = false;
        while (std::chrono::steady_clock::now() < deadline) {
            completed = true;
            for (const EchoClientResult &client : clients) {
                completed = completed && client.completed.load(std::memory_order_acquire);
            }
            if (completed && !stop_requested) {
                server_state.stop_requested.store(true, std::memory_order_release);
                echo_wake_listener(listener.address);
                stop_requested = true;
            }
            if (completed && server_state.accept_stopped.load(std::memory_order_acquire) &&
                server_state.active_sessions.load(std::memory_order_acquire) == 0U) {
                break;
            }
            std::this_thread::sleep_for(1ms);
        }

        if (!stop_requested) {
            server_state.stop_requested.store(true, std::memory_order_release);
            echo_wake_listener(listener.address);
        }
        wait_for_shutdown(std::chrono::steady_clock::now() + 1s);

        const EchoServerSnapshot snapshot = echo_server_snapshot(server_state);
        LOG(INFO) << "tcp echo self-test completed=" << completed
                  << " accepted=" << snapshot.accepted << " active=" << snapshot.active_sessions
                  << " failed_sessions=" << snapshot.failed_sessions
                  << " bytes_in=" << snapshot.bytes_received
                  << " bytes_out=" << snapshot.bytes_sent;

        const bool logs_ok = stop_runtime();
        bool clients_ok = true;
        for (std::size_t index = 0; index < clients.size(); ++index) {
            clients_ok = clients_ok && clients[index].ok && clients[index].error == 0 &&
                         clients[index].response == expected[index];
        }
        const bool ok = completed && clients_ok && snapshot.accepted == clients.size() &&
                        snapshot.active_sessions == 0U && snapshot.failed_sessions == 0U &&
                        snapshot.accept_error == 0 && logs_ok;
        if (!ok) {
            std::cout << "tcp echo self-test failed accepted=" << snapshot.accepted
                      << " active=" << snapshot.active_sessions
                      << " failed_sessions=" << snapshot.failed_sessions
                      << " accept_error=" << snapshot.accept_error
                      << " log_dropped=" << log_stats.dropped << '\n';
            return 1;
        }

        std::cout << "tcp echo self-test ok accepted=" << snapshot.accepted
                  << " completed_sessions=" << snapshot.completed_sessions
                  << " bytes_in=" << snapshot.bytes_received << " bytes_out=" << snapshot.bytes_sent
                  << " log_accepted=" << log_stats.accepted << " log_dropped=" << log_stats.dropped
                  << " log_flushed=" << log_flushed << '\n';
        return 0;
    }

    const af::SignalWaitResult stop_signal = stop_signals.wait();
    if (!stop_signal.ok()) {
        LOG(ERROR) << "tcp echo signal wait failed error=" << stop_signal.error;
    }

    LOG(INFO) << "tcp echo server received signal=" << stop_signal.signal
              << " error=" << stop_signal.error;
    server_state.stop_requested.store(true, std::memory_order_release);
    echo_wake_listener(listener.address);
    const bool drained =
        wait_for_shutdown(std::chrono::steady_clock::now() + options.shutdown_grace);
    const EchoServerSnapshot snapshot = echo_server_snapshot(server_state);
    if (!drained) {
        LOG(WARNING) << "tcp echo shutdown grace expired active_sessions="
                     << snapshot.active_sessions;
    }

    const bool logs_ok = stop_runtime();
    std::cout << "tcp echo server stopped signal=" << stop_signal.signal
              << " accepted=" << snapshot.accepted << " rejected=" << snapshot.rejected
              << " active=" << snapshot.active_sessions
              << " completed_sessions=" << snapshot.completed_sessions
              << " failed_sessions=" << snapshot.failed_sessions
              << " bytes_in=" << snapshot.bytes_received << " bytes_out=" << snapshot.bytes_sent
              << " drained=" << drained << " log_dropped=" << log_stats.dropped << '\n';
    return logs_ok ? 0 : 1;
}
