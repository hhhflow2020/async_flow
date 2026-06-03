#pragma once

#include <cstdint>
#include <string>

namespace af::net {

struct TcpEndpoint {
    std::string address{"0.0.0.0"};
    std::uint16_t port{0};

    [[nodiscard]] static TcpEndpoint any(std::uint16_t port) {
        return {"0.0.0.0", port};
    }

    [[nodiscard]] static TcpEndpoint loopback(std::uint16_t port) {
        return {"127.0.0.1", port};
    }
};

} // namespace af::net
