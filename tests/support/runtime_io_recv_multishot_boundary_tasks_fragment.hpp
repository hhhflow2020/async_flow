#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_recv_multishot_boundary_tasks_fragment.hpp is a runtime_io_accept_tasks implementation fragment"
#endif

#if defined(__linux__)
class RecvMultishotBoundaryTask final : public IoTaskBase {
public:
    explicit RecvMultishotBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* completed,
        std::atomic<int>* invalid_error,
        std::atomic<int>* null_error,
        std::atomic<int>* unavailable_error,
        std::atomic<int>* register_error) {
        stream_.reset(IoTestThread::IO_0, fd);
        datagram_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        invalid_error_ = invalid_error;
        null_error_ = null_error;
        unavailable_error_ = unavailable_error;
        register_error_ = register_error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        int init_error = 0;
        af::IoProvidedBufferRing ring;
        if (ring.init(3, init_error) || init_error != EINVAL) {
            return failed();
        }
        if (!ring.init(2, init_error)) {
            return failed();
        }
        char first = 0;
        char second = 0;
        const af::IoProvidedBuffer buffers[] = {
            af::IoProvidedBuffer{&first, sizeof(first), 0},
            af::IoProvidedBuffer{&second, sizeof(second), 1},
        };
        int add_error = 0;
        if (!ring.add(buffers, 2, add_error)) {
            return failed();
        }

        int null_register_error = 0;
        const bool null_registered = IoRuntime::io_register_provided_buffer_ring(
            IoTestThread::IO_0,
            nullptr,
            2,
            0,
            &null_register_error);
        int register_error = 0;
        const bool registered = IoRuntime::io_register_provided_buffer_ring(
            IoTestThread::IO_0,
            ring.ring(),
            ring.entries(),
            0,
            &register_error);
        if (null_registered || null_register_error != EINVAL ||
            registered || register_error != ENOSYS) {
            return failed();
        }

        af::TcpStream<IoTestThread> invalid_stream(IoTestThread::IO_0, -1);
        af::UdpSocket<IoTestThread> invalid_datagram(IoTestThread::IO_0, -1);
        af::IoOpState invalid_state{};
        af::IoOpState invalid_datagram_state{};
        af::IoOpState invalid_recvmsg_datagram_state{};
        af::IoOpState null_state{};
        af::IoOpState null_datagram_state{};
        af::IoOpState null_recvmsg_datagram_state{};
        af::IoOpState unavailable_state{};
        af::IoOpState unavailable_datagram_state{};
        af::IoOpState unavailable_recvmsg_datagram_state{};
        std::uint16_t buffer_id = 0;
        const af::IoStatus invalid_status =
            invalid_stream.recv_multishot(*this, 0, &buffer_id, invalid_state);
        const af::IoStatus invalid_datagram_status =
            invalid_datagram.recv_multishot(*this, 0, &buffer_id, invalid_datagram_state);
        const af::IoStatus invalid_recvmsg_datagram_status =
            invalid_datagram.recv_from_multishot(
                *this,
                0,
                sizeof(sockaddr_storage),
                0,
                &buffer_id,
                invalid_recvmsg_datagram_state);
        const af::IoStatus null_status =
            stream_.recv_multishot(*this, 0, nullptr, null_state);
        const af::IoStatus null_datagram_status =
            datagram_.recv_multishot(*this, 0, nullptr, null_datagram_state);
        const af::IoStatus null_recvmsg_datagram_status =
            datagram_.recv_from_multishot(
                *this,
                0,
                sizeof(sockaddr_storage),
                0,
                nullptr,
                null_recvmsg_datagram_state);
        const af::IoStatus unavailable_status =
            stream_.recv_multishot(*this, 0, &buffer_id, unavailable_state);
        const af::IoStatus unavailable_datagram_status =
            datagram_.recv_multishot(*this, 0, &buffer_id, unavailable_datagram_state);
        const af::IoStatus unavailable_recvmsg_datagram_status =
            datagram_.recv_from_multishot(
                *this,
                0,
                sizeof(sockaddr_storage),
                0,
                &buffer_id,
                unavailable_recvmsg_datagram_state);
        if (!invalid_status.failed() || invalid_status.error != EBADF ||
            !invalid_datagram_status.failed() || invalid_datagram_status.error != EBADF ||
            !invalid_recvmsg_datagram_status.failed() ||
            invalid_recvmsg_datagram_status.error != EBADF ||
            !null_status.failed() || null_status.error != EINVAL ||
            !null_datagram_status.failed() || null_datagram_status.error != EINVAL ||
            !null_recvmsg_datagram_status.failed() ||
            null_recvmsg_datagram_status.error != EINVAL ||
            !unavailable_status.failed() || unavailable_status.error != ENOSYS) {
            return failed();
        }
        if (!unavailable_datagram_status.failed() ||
            unavailable_datagram_status.error != ENOSYS ||
            !unavailable_recvmsg_datagram_status.failed() ||
            unavailable_recvmsg_datagram_status.error != ENOSYS) {
            return failed();
        }

        alignas(af::detail::IoUringRecvmsgOut) char raw_buffer[256]{};
        auto* raw_header = reinterpret_cast<af::detail::IoUringRecvmsgOut*>(raw_buffer);
        raw_header->namelen = sizeof(sockaddr_in);
        raw_header->controllen = 0;
        raw_header->payloadlen = 1;
        raw_header->flags = 0;
        af::IoRecvmsgMultishotView view{};
        int parse_error = 0;
        constexpr socklen_t name_capacity = sizeof(sockaddr_storage);
        constexpr std::size_t received_size =
            sizeof(af::detail::IoUringRecvmsgOut) + name_capacity + 1U;
        if (!af::io_parse_recvmsg_multishot_buffer(
                raw_buffer,
                sizeof(raw_buffer),
                received_size,
                name_capacity,
                0,
                view,
                parse_error) ||
            view.name_offset != sizeof(af::detail::IoUringRecvmsgOut) ||
            view.name_size != sizeof(sockaddr_in) ||
            view.payload_offset != received_size - 1U ||
            view.payload_size != 1U) {
            return failed();
        }
        if (af::io_parse_recvmsg_multishot_buffer(
                nullptr,
                sizeof(raw_buffer),
                received_size,
                name_capacity,
                0,
                view,
                parse_error) ||
            parse_error != EINVAL) {
            return failed();
        }

        invalid_error_->store(invalid_status.error, std::memory_order_release);
        null_error_->store(null_status.error, std::memory_order_release);
        unavailable_error_->store(unavailable_status.error, std::memory_order_release);
        register_error_->store(register_error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    af::UdpSocket<IoTestThread> datagram_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* invalid_error_{nullptr};
    std::atomic<int>* null_error_{nullptr};
    std::atomic<int>* unavailable_error_{nullptr};
    std::atomic<int>* register_error_{nullptr};
};
#endif
