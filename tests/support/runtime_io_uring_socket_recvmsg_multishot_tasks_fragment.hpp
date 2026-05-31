#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_uring_socket_recvmsg_multishot_tasks_fragment.hpp is a runtime_io_uring_socket_tasks implementation fragment"
#endif

#if defined(__linux__)
class UringUdpRecvmsgMultishotTask final : public UringIoTaskBase {
public:
    explicit UringUdpRecvmsgMultishotTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        in_port_t expected_port,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* read_count,
        std::atomic<int>* packed_read,
        std::atomic<int>* peer_count,
        std::atomic<int>* error) {
        socket_.reset(IoTestThread::IO_0, fd);
        expected_port_ = expected_port;
        armed_ = armed;
        completed_ = completed;
        read_count_ = read_count;
        packed_read_ = packed_read;
        peer_count_ = peer_count;
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

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_ring();
        case State::Recv:
            return recv_one();
        case State::Cancel:
            return finish_cancel();
        case State::Unregister:
            return unregister_ring();
        }
        return complete(EIO);
    }

    af::TaskResult register_ring() {
        int init_error = 0;
        if (!ring_.init(buffer_count, init_error)) {
            return complete(init_error == 0 ? EIO : init_error);
        }
        af::IoProvidedBuffer buffers[buffer_count]{};
        for (std::uint16_t i = 0; i < buffer_count; ++i) {
            buffers[i] = af::IoProvidedBuffer{buffers_[i], buffer_size, i};
        }
        int add_error = 0;
        if (!ring_.add(buffers, buffer_count, add_error)) {
            return complete(add_error == 0 ? EIO : add_error);
        }

        int register_error = 0;
        if (!UringIoRuntime::io_register_provided_buffer_ring(
                IoTestThread::IO_0,
                ring_.ring(),
                ring_.entries(),
                buffer_group,
                &register_error)) {
            return complete(register_error == 0 ? EIO : register_error);
        }
        registered_ = true;
        state_ = State::Recv;
        return again();
    }

    af::TaskResult recv_one() {
        std::uint16_t buffer_id = 0;
        const af::IoStatus status = socket_.recv_from_multishot(
            *this,
            buffer_group,
            name_capacity,
            0,
            &buffer_id,
            recv_);
        if (status.pending()) {
            if (!armed_once_) {
                armed_once_ = true;
                armed_->fetch_add(1, std::memory_order_release);
            }
            return pending();
        }
        if (status.failed()) {
            return complete(status.error);
        }
        if (!status.ready() || buffer_id >= buffer_count) {
            return complete(EIO);
        }

        af::IoRecvmsgMultishotView view{};
        int parse_error = 0;
        if (!af::io_parse_recvmsg_multishot_buffer(
                buffers_[buffer_id],
                buffer_size,
                status.bytes,
                name_capacity,
                0,
                view,
                parse_error)) {
            return stop_recv(parse_error == 0 ? EIO : parse_error);
        }
        if (view.name_size < sizeof(sockaddr_in) || view.payload_size != 1U) {
            return stop_recv(EIO);
        }

        const auto* address = reinterpret_cast<const sockaddr_in*>(
            buffers_[buffer_id] + view.name_offset);
        if (address->sin_family != AF_INET || address->sin_port != expected_port_) {
            return stop_recv(EIO);
        }
        peer_count_->fetch_add(1, std::memory_order_acq_rel);

        const int previous = read_count_->fetch_add(1, std::memory_order_acq_rel);
        const int shifted = previous == 0 ? 8 : 0;
        packed_read_->fetch_or(
            static_cast<int>(
                static_cast<unsigned char>(buffers_[buffer_id][view.payload_offset])) << shifted,
            std::memory_order_acq_rel);

        const af::IoProvidedBuffer buffer{buffers_[buffer_id], buffer_size, buffer_id};
        int add_error = 0;
        if (!ring_.add(&buffer, 1, add_error)) {
            return stop_recv(add_error == 0 ? EIO : add_error);
        }

        if (previous + 1 < target_reads) {
            return pending();
        }
        if (!recv_.waiting) {
            state_ = State::Unregister;
            return again();
        }
        finish_error_ = 0;
        if (!UringIoRuntime::cancel_io(IoTestThread::IO_0, recv_)) {
            return complete(recv_.wait.error == 0 ? EIO : recv_.wait.error);
        }
        state_ = State::Cancel;
        return pending();
    }

    af::TaskResult finish_cancel() {
        std::uint16_t ignored = 0;
        const af::IoStatus status = socket_.recv_from_multishot(
            *this,
            buffer_group,
            name_capacity,
            0,
            &ignored,
            recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.failed() || status.error != ECANCELED) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_ring() {
        if (registered_) {
            int unregister_error = 0;
            if (!UringIoRuntime::io_unregister_provided_buffer_ring(
                    IoTestThread::IO_0,
                    buffer_group,
                    &unregister_error)) {
                return complete(unregister_error == 0 ? EIO : unregister_error);
            }
            registered_ = false;
        }
        return complete(finish_error_);
    }

    af::TaskResult complete(int error) {
        if (registered_) {
            int unregister_error = 0;
            if (UringIoRuntime::io_unregister_provided_buffer_ring(
                    IoTestThread::IO_0,
                    buffer_group,
                    &unregister_error)) {
                registered_ = false;
            }
        }
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TaskResult stop_recv(int error) {
        finish_error_ = error == 0 ? EIO : error;
        if (!recv_.waiting) {
            state_ = State::Unregister;
            return again();
        }
        if (!UringIoRuntime::cancel_io(IoTestThread::IO_0, recv_)) {
            return complete(recv_.wait.error == 0 ? finish_error_ : recv_.wait.error);
        }
        state_ = State::Cancel;
        return pending();
    }

    static constexpr std::uint16_t buffer_group = 8;
    static constexpr unsigned buffer_count = 2;
    static constexpr int target_reads = 2;
    static constexpr socklen_t name_capacity = sizeof(sockaddr_storage);
    static constexpr std::size_t buffer_size =
        sizeof(af::detail::IoUringRecvmsgOut) + name_capacity + 16U;

    State state_{State::Register};
    af::UdpSocket<IoTestThread> socket_{};
    af::IoProvidedBufferRing ring_{};
    alignas(64) char buffers_[buffer_count][buffer_size]{};
    in_port_t expected_port_{0};
    int finish_error_{0};
    bool armed_once_{false};
    bool registered_{false};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* read_count_{nullptr};
    std::atomic<int>* packed_read_{nullptr};
    std::atomic<int>* peer_count_{nullptr};
    std::atomic<int>* error_{nullptr};
};
#endif
