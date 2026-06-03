#include <iostream>

#include "support/io_adapters_socket_helpers.hpp"
#include "support/io_adapters_stream_tasks.hpp"
#include "support/io_adapters_udp_tasks.hpp"

int main() {
    using namespace io_adapters_example;

    if constexpr (!af::supports_epoll) {
        std::cout << "IO adapter example is Linux-only\n";
        return 0;
    }

    async::init();
    if (!async::io_backend_available(AppThreads::IO_0)) {
        std::cout << "epoll backend unavailable\n";
        async::shutdown();
        return 0;
    }

    StreamSocketPair stream{};
    if (!stream.create()) {
        std::cout << "socketpair failed\n";
        async::shutdown();
        return 1;
    }

    UdpLoopbackSockets udp{};
    if (!udp.create()) {
        std::cout << "udp socket failed\n";
        async::shutdown();
        return 1;
    }

    StreamEchoResult stream_echo{};
    StreamPeerResult stream_peer{};
    UdpReceiveResult udp_receive{};
    UdpSendResult udp_send{};

    const bool stream_echo_started =
        async::start_task<StreamEchoTask>(stream.server.get(), &stream_echo);
    const bool stream_peer_started =
        async::start_task<StreamPeerTask>(stream.client.get(), &stream_peer);
    const bool udp_receive_started =
        async::start_task<UdpReceiveTask>(udp.receiver.get(), &udp_receive);
    const bool udp_send_started =
        async::start_task<UdpSendTask>(udp.sender.get(), udp.endpoint(), &udp_send);
    AF_ASSERT(stream_echo_started && stream_peer_started && udp_receive_started &&
              udp_send_started);
    if (!stream_echo_started || !stream_peer_started || !udp_receive_started || !udp_send_started) {
        std::cout << "IO adapter task start failed\n";
        async::shutdown();
        return 1;
    }

    async::shutdown();

    if (!stream_echo.ok || !stream_peer.ok || !udp_receive.ok || !udp_send.ok) {
        std::cout << "IO adapter task failed"
                  << " stream_echo_error=" << stream_echo.error
                  << " stream_peer_error=" << stream_peer.error
                  << " udp_receive_error=" << udp_receive.error
                  << " udp_send_error=" << udp_send.error << '\n';
        return 1;
    }

    std::cout << "stream request=" << stream_echo.request << " response=" << stream_echo.response
              << " peer_received=" << stream_peer.response << '\n';
    std::cout << "udp datagram=" << udp_receive.value << " sent=" << udp_send.value << '\n';
    return 0;
}
