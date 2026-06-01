#pragma once

#include <atomic>
#include <cerrno>
#include <cstdint>

#include "io_uring_recv_multishot_runtime.hpp"

#if defined(__linux__)

namespace io_uring_recv_multishot_example {

class RecvMultishotTask final : public RecvTaskBase {
public:
    explicit RecvMultishotTask(RecvTaskBase::FactoryToken token) : RecvTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        int* packed_read,
        std::atomic<int>* error) {
        stream_.reset(RecvThread::IO_0, fd);
        armed_ = armed;
        packed_read_ = packed_read;
        error_ = error;
        return schedule(RecvThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Recv,
        Cancel,
        Unregister,
    };

#define AF_EXAMPLE_IO_URING_RECV_MULTISHOT_TASK_FRAGMENT_INCLUDE 1
#include "io_uring_recv_multishot_task_flow.hpp"
#include "io_uring_recv_multishot_task_ring.hpp"
#include "io_uring_recv_multishot_task_recv.hpp"
#undef AF_EXAMPLE_IO_URING_RECV_MULTISHOT_TASK_FRAGMENT_INCLUDE

    static constexpr std::uint16_t buffer_group = 9;
    static constexpr unsigned buffer_count = 2;
    static constexpr int target_reads = 2;

    State state_{State::Register};
    af::TcpStream<RecvThread> stream_{};
    af::IoProvidedBufferRing ring_{};
    char buffers_[buffer_count]{};
    int received_{0};
    int finish_error_{0};
    bool armed_once_{false};
    bool registered_{false};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    int* packed_read_{nullptr};
    std::atomic<int>* error_{nullptr};
};

} // namespace io_uring_recv_multishot_example

#endif
