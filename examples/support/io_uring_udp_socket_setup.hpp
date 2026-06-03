#pragma once

#include <cstddef>
#include <cstdint>

#include "io_uring_udp_recvmsg_multishot_runtime.hpp"

#if defined(__linux__)

namespace io_uring_udp_recvmsg_multishot_example {

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

struct UdpRecvmsgMultishotSockets {
    [[nodiscard]] bool bind_loopback_pair() noexcept {
        receiver = make_udp_socket();
        sender = make_udp_socket();
        if (!receiver || !sender) {
            return false;
        }

        receiver_address_size_ = sizeof(receiver_address_);
        sender_address_size_ = sizeof(sender_address_);
        return bind_loopback(receiver.get(), receiver_address_, receiver_address_size_) &&
               bind_loopback(sender.get(), sender_address_, sender_address_size_);
    }

    [[nodiscard]] std::uint16_t sender_port() const noexcept {
        return sender_address_.sin_port;
    }

    [[nodiscard]] bool send_payload_to_receiver(const char *payload, std::size_t size) noexcept {
        if (!sender || payload == nullptr) {
            return false;
        }
        for (std::size_t i = 0; i < size; ++i) {
            if (::sendto(sender.get(), payload + i, 1, 0,
                         reinterpret_cast<sockaddr *>(&receiver_address_),
                         receiver_address_size_) != 1) {
                return false;
            }
        }
        return true;
    }

    af::UniqueFd receiver{};
    af::UniqueFd sender{};

private:
    sockaddr_in receiver_address_{};
    socklen_t receiver_address_size_{sizeof(receiver_address_)};
    sockaddr_in sender_address_{};
    socklen_t sender_address_size_{sizeof(sender_address_)};
};

} // namespace io_uring_udp_recvmsg_multishot_example

#else

namespace io_uring_udp_recvmsg_multishot_example {

struct UdpRecvmsgMultishotSockets {
    [[nodiscard]] bool bind_loopback_pair() noexcept {
        return false;
    }

    [[nodiscard]] std::uint16_t sender_port() const noexcept {
        return 0;
    }

    [[nodiscard]] bool send_payload_to_receiver(const char *payload, std::size_t size) noexcept {
        static_cast<void>(payload);
        static_cast<void>(size);
        return false;
    }

    af::UniqueFd receiver{};
    af::UniqueFd sender{};
};

} // namespace io_uring_udp_recvmsg_multishot_example

#endif
