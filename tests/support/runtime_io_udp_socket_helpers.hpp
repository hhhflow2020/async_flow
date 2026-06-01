#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_io_udp_socket_helpers.hpp is a runtime_io_test_support implementation detail"
#endif

struct UdpLoopbackSockets {
    af::UniqueFd receiver{};
    af::UniqueFd sender{};
    sockaddr_in address{};
    socklen_t address_size{sizeof(address)};
};

bool create_udp_loopback_sockets(UdpLoopbackSockets& sockets) {
    sockets = UdpLoopbackSockets{};
    sockets.receiver.reset(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!sockets.receiver) {
        return false;
    }
    sockets.sender.reset(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!sockets.sender) {
        return false;
    }

    sockets.address = sockaddr_in{};
    sockets.address.sin_family = AF_INET;
    sockets.address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sockets.address.sin_port = 0;
    if (::bind(
            sockets.receiver.get(),
            reinterpret_cast<sockaddr*>(&sockets.address),
            sizeof(sockets.address)) != 0) {
        return false;
    }

    sockets.address_size = sizeof(sockets.address);
    return ::getsockname(
               sockets.receiver.get(),
               reinterpret_cast<sockaddr*>(&sockets.address),
               &sockets.address_size) == 0;
}

bool connect_udp_loopback_sockets(UdpLoopbackSockets& sockets) {
    sockaddr_in sender_address{};
    sender_address.sin_family = AF_INET;
    sender_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sender_address.sin_port = 0;
    if (::bind(
            sockets.sender.get(),
            reinterpret_cast<sockaddr*>(&sender_address),
            sizeof(sender_address)) != 0) {
        return false;
    }

    socklen_t sender_address_size = sizeof(sender_address);
    if (::getsockname(
            sockets.sender.get(),
            reinterpret_cast<sockaddr*>(&sender_address),
            &sender_address_size) != 0) {
        return false;
    }

    if (::connect(
            sockets.receiver.get(),
            reinterpret_cast<sockaddr*>(&sender_address),
            sender_address_size) != 0) {
        return false;
    }
    return ::connect(
               sockets.sender.get(),
               reinterpret_cast<sockaddr*>(&sockets.address),
               sockets.address_size) == 0;
}

ssize_t send_udp_payload(const UdpLoopbackSockets& sockets, const void* data, std::size_t size) {
    return ::sendto(
        sockets.sender.get(),
        data,
        size,
        0,
        reinterpret_cast<const sockaddr*>(&sockets.address),
        sockets.address_size);
}

ssize_t recv_udp_payload(const UdpLoopbackSockets& sockets, void* data, std::size_t size) {
    sockaddr_storage peer{};
    socklen_t peer_size = sizeof(peer);
    return ::recvfrom(
        sockets.receiver.get(),
        data,
        size,
        0,
        reinterpret_cast<sockaddr*>(&peer),
        &peer_size);
}
