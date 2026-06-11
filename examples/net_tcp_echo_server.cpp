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
#include <thread>
#include <utility>
#include <vector>

#include "af/log.hpp"
#include "af/net.hpp"
#include "af/platform.hpp"
#include "af/runtime.hpp"
#include "af/signal.hpp"

namespace {

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

struct echo_shard_state {
    af::runtime *runtime{nullptr};
    af::thread_ref cpu_thread{};
    std::shared_ptr<server_lifecycle_state> lifecycle;
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

class lowercase_echo_task final : public af::runtime_task {
public:
    lowercase_echo_task(af::runtime_task::factory_token token, af::runtime &owner) noexcept
        : af::runtime_task(token, owner) {}

    bool do_it(af::thread_ref cpu_thread, af::net::tcp_connection_handle conn,
               af::buffer payload) noexcept {
        conn_ = conn;
        payload_ = std::move(payload);
        return schedule_to(cpu_thread);
    }

private:
    af::task_result run_task() noexcept override {
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

    af::net::tcp_connection_handle conn_;
    af::buffer payload_;
};

void echo_on_accept(void *owner, af::net::tcp_connection_ref conn) noexcept {
    static_cast<void>(owner);
    LOG(INFO) << "tcp echo accepted slot=" << conn.slot() << " generation=" << conn.generation();
}

void echo_on_read(void *owner, af::net::tcp_connection_ref conn, af::buffer_view bytes) noexcept {
    auto *state = static_cast<echo_shard_state *>(owner);
    LOG(INFO) << "tcp echo received bytes=" << bytes.size() << " slot=" << conn.slot()
              << " generation=" << conn.generation();
    if (state == nullptr || state->runtime == nullptr || bytes.empty()) {
        return;
    }

    af::buffer payload;
    try {
        payload = af::buffer::copy(bytes);
    } catch (...) {
        LOG(ERROR) << "tcp echo failed to copy payload slot=" << conn.slot()
                   << " generation=" << conn.generation();
        conn.close(af::net::close_reason::error);
        return;
    }

    auto task = af::make_task<lowercase_echo_task>(*state->runtime);
    const bool started = task->do_it(state->cpu_thread, conn.handle(), std::move(payload));
    if (!started) {
        LOG(ERROR) << "tcp echo failed to schedule compute task slot=" << conn.slot()
                   << " generation=" << conn.generation();
        conn.close(af::net::close_reason::error);
    }
}

void echo_on_close(void *owner, af::net::tcp_connection_ref conn,
                   af::net::close_reason reason) noexcept {
    static_cast<void>(owner);
    LOG(INFO) << "tcp echo closed slot=" << conn.slot() << " generation=" << conn.generation()
              << " reason=" << static_cast<unsigned>(reason);
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

void start_server_shard(af::net::tcp_server &server, echo_shard_state &state,
                        af::thread_ref io_thread, std::uint16_t port, bool ipv6,
                        bool reuse_port) noexcept {
    af::net::tcp_connection_callbacks callbacks;
    callbacks.owner = &state;
    callbacks.on_accept = &echo_on_accept;
    callbacks.on_read = &echo_on_read;
    callbacks.on_close = &echo_on_close;

    af::net::tcp_listener_config listener_config;
    listener_config.name = ipv6 ? "echo-v6" : "echo-v4";
    listener_config.endpoint =
        ipv6 ? af::net::tcp_endpoint::any_v6(port) : af::net::tcp_endpoint::any(port);
    listener_config.threads = {io_thread};
    listener_config.options.backlog = 4096;
    listener_config.options.reuse_port = reuse_port;
    listener_config.options.ipv6_only = ipv6;
    listener_config.options.accept_budget = 256;
    listener_config.accept_strategy = af::net::tcp_accept_strategy::single_acceptor;

    const af::net::listener_result listener =
        server.add_listener(std::move(listener_config), callbacks);
    const bool ok = listener.ok() && server.start();
    if (ok) {
        LOG(INFO) << "tcp echo shard started thread=" << io_thread.index
                  << " listener_slot=" << listener.listener.slot()
                  << " listener_generation=" << listener.listener.generation();
    } else {
        LOG(ERROR) << "tcp echo shard start failed thread=" << io_thread.index
                   << " error=" << (listener.error == 0 ? EIO : listener.error);
    }
    state.lifecycle->record_start(ok, listener.error == 0 ? EIO : listener.error);
}

[[nodiscard]] bool wait_for_shutdown_signal(af::signal_set &signals,
                                            const server_lifecycle_state &lifecycle) {
    for (;;) {
        if (lifecycle.failed.load(std::memory_order_acquire)) {
            return false;
        }
        const af::signal_wait_result result = signals.wait_for(std::chrono::seconds(1));
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

    af::signal_set signals = af::make_termination_signal_set();
    if (!signals.valid()) {
        std::cerr << "failed to install signal set error=" << signals.error() << '\n';
        return 1;
    }
    static_cast<void>(af::ignore_process_signal(SIGPIPE));

    af::runtime_config runtime_config;
    runtime_config.threads = {af::io_threads("echo-io", 2), af::cpu_threads("echo-cpu", 1)};
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
    std::vector<std::unique_ptr<echo_shard_state>> shard_states;

    try {
        servers.reserve(io_threads.size());
        shard_states.reserve(io_threads.size());
        const af::net::tcp_server_config server_config = make_server_config();
        for (std::size_t i = 0; i < io_threads.size(); ++i) {
            servers.push_back(std::make_unique<af::net::tcp_server>(runtime, server_config));
            auto state = std::make_unique<echo_shard_state>();
            state->runtime = &runtime;
            state->cpu_thread = cpu_thread;
            state->lifecycle = lifecycle;
            shard_states.push_back(std::move(state));
        }
    } catch (...) {
        std::cerr << "failed to allocate tcp echo server shards\n";
        runtime.stop();
        return 1;
    }

    for (std::size_t i = 0; i < io_threads.size(); ++i) {
        const af::thread_ref io_thread = io_threads[i];
        af::net::tcp_server *server = servers[i].get();
        echo_shard_state *state = shard_states[i].get();
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
        std::cerr << "failed to start tcp echo server error="
                  << lifecycle->error.load(std::memory_order_acquire) << '\n';
        runtime.stop();
        return 1;
    }

    std::cout << "tcp echo server listening on " << std::string_view(ipv6 ? "[::]" : "0.0.0.0")
              << ':' << port << " with " << io_threads.size() << " io shard(s)\n";
    static_cast<void>(wait_for_shutdown_signal(signals, *lifecycle));

    for (std::size_t i = 0; i < io_threads.size(); ++i) {
        const af::thread_ref io_thread = io_threads[i];
        af::net::tcp_server *server = servers[i].get();
        if (!runtime.post(io_thread, [server, lifecycle] {
                if (server == nullptr || !server->stop()) {
                    LOG(ERROR) << "tcp echo server shard stop failed";
                } else {
                    LOG(INFO) << "tcp echo shard stopped";
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
