#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_uring_socket_recv_multishot_tasks_fragment.hpp is a runtime_io_uring_socket_tasks implementation fragment"
#endif

#if defined(__linux__)
class UringRecvMultishotTask final : public UringIoTaskBase {
public:
    explicit UringRecvMultishotTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        int target_reads,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* read_count,
        std::atomic<int>* packed_read,
        std::atomic<int>* error) {
        return do_it(
            fd,
            target_reads,
            armed,
            completed,
            read_count,
            packed_read,
            error,
            false);
    }

    bool do_it(
        int fd,
        int target_reads,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* read_count,
        std::atomic<int>* packed_read,
        std::atomic<int>* error,
        bool datagram) {
        stream_.reset(IoTestThread::IO_0, fd);
        datagram_.reset(IoTestThread::IO_0, fd);
        target_reads_ = target_reads;
        datagram_mode_ = datagram;
        armed_ = armed;
        completed_ = completed;
        read_count_ = read_count;
        packed_read_ = packed_read;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Recv,
        Cancel,
        Unregister,
    };

#define AF_RUNTIME_IO_URING_RECV_MULTISHOT_TASK_FRAGMENT_INCLUDE 1
#include "runtime_io_uring_socket_recv_multishot_task_flow_fragment.hpp"
#include "runtime_io_uring_socket_recv_multishot_task_ring_fragment.hpp"
#include "runtime_io_uring_socket_recv_multishot_task_recv_fragment.hpp"
#undef AF_RUNTIME_IO_URING_RECV_MULTISHOT_TASK_FRAGMENT_INCLUDE

    static constexpr std::uint16_t buffer_group = 7;
    static constexpr unsigned buffer_count = 2;
    State state_{State::Register};
    af::TcpStream<IoTestThread> stream_{};
    af::UdpSocket<IoTestThread> datagram_{};
    af::IoProvidedBufferRing ring_{};
    char buffers_[buffer_count]{};
    int target_reads_{0};
    int finish_error_{0};
    bool datagram_mode_{false};
    bool armed_once_{false};
    bool registered_{false};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* read_count_{nullptr};
    std::atomic<int>* packed_read_{nullptr};
    std::atomic<int>* error_{nullptr};
};
#endif
