#pragma once

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>

#include "io_uring_udp_recvmsg_multishot_runtime.hpp"

#if defined(__linux__)

namespace io_uring_udp_recvmsg_multishot_example {

class UdpRecvmsgMultishotTask final : public UdpRecvmsgTaskBase {
public:
    explicit UdpRecvmsgMultishotTask(UdpRecvmsgTaskBase::FactoryToken token)
        : UdpRecvmsgTaskBase(token) {}

    bool do_it(
        int fd,
        in_port_t expected_port,
        std::atomic<int>* armed,
        int* packed_read,
        int* peer_count,
        std::atomic<int>* error) {
        socket_.reset(UdpRecvmsgThread::IO_0, fd);
        expected_port_ = expected_port;
        armed_ = armed;
        packed_read_ = packed_read;
        peer_count_ = peer_count;
        error_ = error;
        return schedule(UdpRecvmsgThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Recv,
        Cancel,
        Unregister,
    };

#define AF_EXAMPLE_IO_URING_UDP_RECVMSG_MULTISHOT_TASK_FRAGMENT_INCLUDE 1
#include "io_uring_udp_recvmsg_multishot_task_flow.hpp"
#include "io_uring_udp_recvmsg_multishot_task_ring.hpp"
#include "io_uring_udp_recvmsg_multishot_task_recv.hpp"
#undef AF_EXAMPLE_IO_URING_UDP_RECVMSG_MULTISHOT_TASK_FRAGMENT_INCLUDE

    static constexpr std::uint16_t buffer_group = 11;
    static constexpr unsigned buffer_count = 2;
    static constexpr int target_reads = 2;
    static constexpr socklen_t name_capacity = sizeof(sockaddr_storage);
    static constexpr std::size_t buffer_size =
        sizeof(af::detail::IoUringRecvmsgOut) + name_capacity + 16U;

    State state_{State::Register};
    af::UdpSocket<UdpRecvmsgThread> socket_{};
    af::IoProvidedBufferRing ring_{};
    alignas(64) char buffers_[buffer_count][buffer_size]{};
    in_port_t expected_port_{0};
    int received_{0};
    int finish_error_{0};
    bool armed_once_{false};
    bool registered_{false};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    int* packed_read_{nullptr};
    int* peer_count_{nullptr};
    std::atomic<int>* error_{nullptr};
};

} // namespace io_uring_udp_recvmsg_multishot_example

#endif
