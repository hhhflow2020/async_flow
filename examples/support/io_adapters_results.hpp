#pragma once

namespace io_adapters_example {

struct StreamEchoResult {
    bool ok{false};
    int error{0};
    char request{0};
    char response{0};
};

struct StreamPeerResult {
    bool ok{false};
    int error{0};
    char response{0};
};

struct UdpReceiveResult {
    bool ok{false};
    int error{0};
    char value{0};
};

struct UdpSendResult {
    bool ok{false};
    int error{0};
    char value{0};
};

} // namespace io_adapters_example
