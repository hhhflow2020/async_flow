#pragma once

#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>

#include "io_tcp_echo_sockets.hpp"

#include <arpa/inet.h>

namespace io_tcp_echo_example {

enum class EchoLogLevel : std::uint8_t {
    Info,
    Warning,
    Error,
    Fatal,
};

struct EchoServerOptions {
    std::string bind_address{"0.0.0.0"};
    std::filesystem::path log_path{"asyncflow-tcp-echo-server.log"};
    std::chrono::milliseconds shutdown_grace{5000};
    std::uint16_t port{7000};
    int backlog{1024};
    EchoLogLevel log_level{EchoLogLevel::Info};
    bool bind_set{false};
    bool port_set{false};
    bool self_test{false};
    bool help{false};
};

[[nodiscard]] inline bool echo_parse_u64(std::string_view text, std::uint64_t max,
                                         std::uint64_t *value) noexcept {
    if (text.empty() || value == nullptr) {
        return false;
    }

    std::uint64_t parsed = 0;
    const char *first = text.data();
    const char *last = text.data() + text.size();
    const auto result = std::from_chars(first, last, parsed);
    if (result.ec != std::errc{} || result.ptr != last || parsed > max) {
        return false;
    }

    *value = parsed;
    return true;
}

[[nodiscard]] inline bool echo_parse_log_level(std::string_view text,
                                               EchoLogLevel *level) noexcept {
    if (level == nullptr) {
        return false;
    }
    if (text == "info" || text == "INFO") {
        *level = EchoLogLevel::Info;
        return true;
    }
    if (text == "warning" || text == "warn" || text == "WARNING" || text == "WARN") {
        *level = EchoLogLevel::Warning;
        return true;
    }
    if (text == "error" || text == "err" || text == "ERROR" || text == "ERR") {
        *level = EchoLogLevel::Error;
        return true;
    }
    if (text == "fatal" || text == "FATAL") {
        *level = EchoLogLevel::Fatal;
        return true;
    }
    return false;
}

[[nodiscard]] inline std::string_view echo_log_level_name(EchoLogLevel level) noexcept {
    switch (level) {
    case EchoLogLevel::Info:
        return "info";
    case EchoLogLevel::Warning:
        return "warning";
    case EchoLogLevel::Error:
        return "error";
    case EchoLogLevel::Fatal:
        return "fatal";
    }
    return "info";
}

inline void echo_print_server_usage(std::ostream &out) {
    out << "Usage: asyncflow_io_tcp_echo_server_example [options]\n"
        << "  --bind ADDR              IPv4 address to bind, default 0.0.0.0\n"
        << "  --port PORT              TCP port to bind, default 7000\n"
        << "  --backlog N              listen backlog, default 1024\n"
        << "  --log PATH               async file log path\n"
        << "  --log-level LEVEL        minimum log level: info, warning, error, fatal\n"
        << "  --shutdown-grace-ms N    graceful shutdown wait, default 5000\n"
        << "  --self-test              run loopback clients and exit\n";
}

[[nodiscard]] inline bool echo_parse_server_options(int argc, char **argv,
                                                    EchoServerOptions *options,
                                                    std::ostream &error_stream) {
    if (options == nullptr) {
        return false;
    }

    for (int index = 1; index < argc; ++index) {
        const std::string_view arg(argv[index]);
        auto require_value = [&](std::string_view name) -> std::string_view {
            if (index + 1 >= argc) {
                error_stream << "missing value for " << name << '\n';
                return {};
            }
            ++index;
            return argv[index];
        };

        if (arg == "--help" || arg == "-h") {
            options->help = true;
            return true;
        }
        if (arg == "--self-test") {
            options->self_test = true;
            continue;
        }
        if (arg == "--bind" || arg == "--host") {
            const std::string_view value = require_value(arg);
            if (value.empty()) {
                return false;
            }
            options->bind_address = std::string(value);
            options->bind_set = true;
            continue;
        }
        if (arg.starts_with("--bind=") || arg.starts_with("--host=")) {
            const std::size_t offset = arg.find('=');
            options->bind_address = std::string(arg.substr(offset + 1));
            options->bind_set = true;
            continue;
        }
        if (arg == "--log") {
            const std::string_view value = require_value(arg);
            if (value.empty()) {
                return false;
            }
            options->log_path = std::filesystem::path(std::string(value));
            continue;
        }
        if (arg.starts_with("--log=")) {
            options->log_path = std::filesystem::path(std::string(arg.substr(6)));
            continue;
        }
        if (arg == "--log-level") {
            const std::string_view value = require_value(arg);
            if (!echo_parse_log_level(value, &options->log_level)) {
                error_stream << "invalid log level: " << value << '\n';
                return false;
            }
            continue;
        }
        if (arg.starts_with("--log-level=")) {
            constexpr std::string_view prefix = "--log-level=";
            if (!echo_parse_log_level(arg.substr(prefix.size()), &options->log_level)) {
                error_stream << "invalid log level: " << arg.substr(prefix.size()) << '\n';
                return false;
            }
            continue;
        }

        std::uint64_t parsed = 0;
        if (arg == "--port") {
            const std::string_view value = require_value(arg);
            if (!echo_parse_u64(value, 65535, &parsed)) {
                error_stream << "invalid port: " << value << '\n';
                return false;
            }
            options->port = static_cast<std::uint16_t>(parsed);
            options->port_set = true;
            continue;
        }
        if (arg.starts_with("--port=")) {
            if (!echo_parse_u64(arg.substr(7), 65535, &parsed)) {
                error_stream << "invalid port: " << arg.substr(7) << '\n';
                return false;
            }
            options->port = static_cast<std::uint16_t>(parsed);
            options->port_set = true;
            continue;
        }
        if (arg == "--backlog") {
            const std::string_view value = require_value(arg);
            if (!echo_parse_u64(value, 65535, &parsed) || parsed == 0U) {
                error_stream << "invalid backlog: " << value << '\n';
                return false;
            }
            options->backlog = static_cast<int>(parsed);
            continue;
        }
        if (arg.starts_with("--backlog=")) {
            if (!echo_parse_u64(arg.substr(10), 65535, &parsed) || parsed == 0U) {
                error_stream << "invalid backlog: " << arg.substr(10) << '\n';
                return false;
            }
            options->backlog = static_cast<int>(parsed);
            continue;
        }
        if (arg == "--shutdown-grace-ms") {
            const std::string_view value = require_value(arg);
            if (!echo_parse_u64(value, 600000, &parsed)) {
                error_stream << "invalid shutdown grace: " << value << '\n';
                return false;
            }
            options->shutdown_grace = std::chrono::milliseconds(parsed);
            continue;
        }
        if (arg.starts_with("--shutdown-grace-ms=")) {
            constexpr std::string_view prefix = "--shutdown-grace-ms=";
            if (!echo_parse_u64(arg.substr(prefix.size()), 600000, &parsed)) {
                error_stream << "invalid shutdown grace: " << arg.substr(prefix.size()) << '\n';
                return false;
            }
            options->shutdown_grace = std::chrono::milliseconds(parsed);
            continue;
        }
        if (!arg.empty() && arg.front() != '-') {
            options->log_path = std::filesystem::path(std::string(arg));
            continue;
        }

        error_stream << "unknown option: " << arg << '\n';
        return false;
    }

    if (options->self_test) {
        if (!options->bind_set) {
            options->bind_address = "127.0.0.1";
        }
        if (!options->port_set) {
            options->port = 0;
        }
    }
    return true;
}

[[nodiscard]] inline std::string echo_listen_address_text(sockaddr_in address) {
    std::array<char, INET_ADDRSTRLEN> text{};
    const char *converted = ::inet_ntop(AF_INET, &address.sin_addr, text.data(), text.size());
    const std::string host = converted == nullptr ? "<invalid>" : std::string(converted);
    return host + ":" + std::to_string(ntohs(address.sin_port));
}

} // namespace io_tcp_echo_example
