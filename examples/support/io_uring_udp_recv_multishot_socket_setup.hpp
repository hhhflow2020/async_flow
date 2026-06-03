#pragma once

#include <cstddef>

#include "io_uring_udp_recv_multishot_runtime.hpp"

#if defined(__linux__)

namespace io_uring_udp_recv_multishot_example {

[[nodiscard]] inline af::UniqueFd make_udp_socket() noexcept {
    return af::UniqueFd(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
}

inline bool bind_loopback(int fd, sockaddr_in &address, socklen_t &address_size) {
    address = sockaddr_in{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        return false;
    }
    address_size = sizeof(address);
    return ::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &address_size) == 0;
}

struct UdpRecvMultishotSockets {
    [[nodiscard]] bool connect_loopback() noexcept {
        receiver = make_udp_socket();
        sender = make_udp_socket();
        if (!receiver || !sender) {
            return false;
        }

        sockaddr_in receiver_address{};
        socklen_t receiver_address_size = sizeof(receiver_address);
        sockaddr_in sender_address{};
        socklen_t sender_address_size = sizeof(sender_address);
        return bind_loopback(receiver.get(), receiver_address, receiver_address_size) &&
               bind_loopback(sender.get(), sender_address, sender_address_size) &&
               ::connect(receiver.get(), reinterpret_cast<sockaddr *>(&sender_address),
                         sender_address_size) == 0 &&
               ::connect(sender.get(), reinterpret_cast<sockaddr *>(&receiver_address),
                         receiver_address_size) == 0;
    }

    [[nodiscard]] bool send_payload(const char *payload, std::size_t size) noexcept {
        if (!sender || payload == nullptr) {
            return false;
        }
        for (std::size_t i = 0; i < size; ++i) {
            if (::send(sender.get(), payload + i, 1, 0) != 1) {
                return false;
            }
        }
        return true;
    }

    af::UniqueFd receiver{};
    af::UniqueFd sender{};
};

} // namespace io_uring_udp_recv_multishot_example

#else

namespace io_uring_udp_recv_multishot_example {

struct UdpRecvMultishotSockets {
    [[nodiscard]] bool connect_loopback() noexcept {
        return false;
    }

    [[nodiscard]] bool send_payload(const char *payload, std::size_t size) noexcept {
        static_cast<void>(payload);
        static_cast<void>(size);
        return false;
    }

    af::UniqueFd receiver{};
    af::UniqueFd sender{};
};

} // namespace io_uring_udp_recv_multishot_example

#endif
