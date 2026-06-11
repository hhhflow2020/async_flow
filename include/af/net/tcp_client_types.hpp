#pragma once

#include "af/net/tcp_types.hpp"

namespace af::net {

struct tcp_client_options {
    tcp_listener_options connection;
    bool no_delay{true};
    bool keep_alive{false};
};

struct tcp_client_runtime_config {};

using TcpClientOptions = tcp_client_options;
using TcpClientRuntimeConfig = tcp_client_runtime_config;

} // namespace af::net
