#pragma once

#include "af/net/tcp_types.hpp"

namespace af::net {

struct TcpClientOptions {
    TcpListenerOptions connection;
    bool no_delay{true};
    bool keep_alive{false};
};

struct TcpClientRuntimeConfig {};

using tcp_client_options = TcpClientOptions;
using tcp_client_runtime_config = TcpClientRuntimeConfig;

} // namespace af::net
