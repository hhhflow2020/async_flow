#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <utility>

#include "af/log.hpp"

namespace {

[[nodiscard]] std::uint16_t parse_port(const char *value) noexcept {
    if (value == nullptr || *value == '\0') {
        return 0;
    }

    char *end = nullptr;
    const auto port = std::strtoul(value, &end, 10);
    if (end == value || *end != '\0' || port > 65535UL) {
        return 0;
    }
    return static_cast<std::uint16_t>(port);
}

} // namespace

int main(int argc, char **argv) {
    using namespace std::chrono_literals;

    af::AsyncLogConfig config;
    config.queue_capacity = 1U << 16U;
    config.queue_shard_count = 0;
    config.max_batch_size = 512;
    config.overflow_spin_count = 128;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.flush_poll_interval = 1ms;
    config.consumer_thread_name = "log";

    const std::filesystem::path log_path = argc > 1 ? argv[1] : "asyncflow-example.log";
    config.backends.push_back(af::make_file_log_backend({.path = log_path, .append = false}));

    if (argc > 2) {
        if (const std::uint16_t udp_port = parse_port(argv[2]); udp_port != 0U) {
            config.backends.push_back(
                af::make_udp_log_backend({.host = "127.0.0.1", .port = udp_port}));
        }
    }

    if (argc > 3) {
        if (const std::uint16_t tcp_port = parse_port(argv[3]); tcp_port != 0U) {
            af::TcpLogBackendConfig tcp_config;
            tcp_config.host = "127.0.0.1";
            tcp_config.port = tcp_port;
            tcp_config.reconnect_interval = 100ms;
            config.backends.push_back(af::make_tcp_log_backend(std::move(tcp_config)));
        }
    }

    auto logging = af::start_async_logging(std::move(config));
    for (std::uint32_t i = 0; i < 10000U; ++i) {
        LOG(INFO) << "async log event seq=" << i << " shard=" << (i & 63U);
    }

    const bool flushed = logging->flush(5s);
    const af::AsyncLogStats stats = logging->stats();
    logging->stop();

    std::cout << "accepted=" << stats.accepted << " dropped=" << stats.dropped
              << " flushed=" << flushed << " file=" << log_path << '\n';
    return flushed ? 0 : 1;
}
