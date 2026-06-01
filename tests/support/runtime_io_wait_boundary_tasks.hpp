#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_io_wait_boundary_tasks.hpp is a runtime_io_test_support implementation detail"
#endif

class ZeroByteIoTask final : public IoTaskBase {
public:
    explicit ZeroByteIoTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState read{};
        af::IoOpState write{};
        const af::IoStatus read_status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            0,
            read);
        const af::IoStatus write_status = af::io_write_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            0,
            write);
        if (!read_status.ready() || read_status.bytes != 0U ||
            !write_status.ready() || write_status.bytes != 0U) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
};

class VectoredBoundaryTask final : public IoTaskBase {
public:
    explicit VectoredBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoFile<IoTestThread> file(IoTestThread::IO_0, -1);
        af::TcpStream<IoTestThread> stream(IoTestThread::IO_0, -1);
        af::IoOpState readv{};
        af::IoOpState writev{};
        af::IoOpState readv_at{};
        af::IoOpState writev_at{};
        af::IoOpState recvv{};
        af::IoOpState sendv{};
        af::IoOpState sendv_zc{};
        af::IoOpState recv_from{};
        af::IoOpState send_to{};
        af::IoOpState send_zc_to{};
        af::IoOpState bad_file{};
        af::IoOpState bad_iov_state{};
        af::IoOpState bad_iov_zc_state{};
        af::IoOpState bad_count_state{};
        af::IoOpState bad_datagram_state{};
        af::IoOpState bad_datagram_zc_state{};

        char value = 'v';
        iovec valid_iov{&value, 1};
        iovec invalid_iov{nullptr, 1};
        af::UdpSocket<IoTestThread> datagram(IoTestThread::IO_0, -1);

        const af::IoStatus zero_readv = file.readv_some(*this, nullptr, 0, readv);
        const af::IoStatus zero_writev = file.writev_some(*this, nullptr, 0, writev);
        const af::IoStatus zero_readv_at = file.readv_at(*this, nullptr, 0, 0, readv_at);
        const af::IoStatus zero_writev_at = file.writev_at(*this, nullptr, 0, 0, writev_at);
        const af::IoStatus zero_recvv = stream.recvv_some(*this, nullptr, 0, recvv);
        const af::IoStatus zero_sendv = stream.sendv_some(*this, nullptr, 0, sendv);
        const af::IoStatus zero_sendv_zc =
            stream.sendv_zc_some(*this, nullptr, 0, sendv_zc);
        const af::IoStatus zero_recvv_from =
            datagram.recvv_from_some(*this, nullptr, 0, nullptr, nullptr, recv_from);
        const af::IoStatus zero_sendv_to =
            datagram.sendv_to_some(*this, nullptr, 0, nullptr, 0, send_to);
        const af::IoStatus zero_sendv_zc_to =
            datagram.sendv_zc_to_some(*this, nullptr, 0, nullptr, 0, send_zc_to);
        const af::IoStatus bad_file_status =
            file.writev_at(*this, &valid_iov, 1, 0, bad_file);
        const af::IoStatus bad_iov =
            stream.sendv_some(*this, &invalid_iov, 1, bad_iov_state);
        const af::IoStatus bad_iov_zc =
            stream.sendv_zc_some(*this, &invalid_iov, 1, bad_iov_zc_state);
        const af::IoStatus bad_count =
            stream.recvv_some(*this, &valid_iov, -1, bad_count_state);
        const af::IoStatus bad_datagram =
            datagram.sendv_to_some(*this, &invalid_iov, 1, nullptr, 0, bad_datagram_state);
        const af::IoStatus bad_datagram_zc =
            datagram.sendv_zc_to_some(*this, &invalid_iov, 1, nullptr, 0, bad_datagram_zc_state);

        if (!zero_readv.ready() || zero_readv.bytes != 0U ||
            !zero_writev.ready() || zero_writev.bytes != 0U ||
            !zero_readv_at.ready() || zero_readv_at.bytes != 0U ||
            !zero_writev_at.ready() || zero_writev_at.bytes != 0U ||
            !zero_recvv.ready() || zero_recvv.bytes != 0U ||
            !zero_sendv.ready() || zero_sendv.bytes != 0U ||
            !zero_sendv_zc.ready() || zero_sendv_zc.bytes != 0U ||
            !zero_recvv_from.ready() || zero_recvv_from.bytes != 0U ||
            !zero_sendv_to.ready() || zero_sendv_to.bytes != 0U ||
            !zero_sendv_zc_to.ready() || zero_sendv_zc_to.bytes != 0U ||
            !bad_file_status.failed() || bad_file_status.error != EBADF ||
            !bad_iov.failed() || bad_iov.error != EINVAL ||
            !bad_iov_zc.failed() || bad_iov_zc.error != EINVAL ||
            !bad_count.failed() || bad_count.error != EINVAL ||
            !bad_datagram.failed() || bad_datagram.error != EINVAL ||
            !bad_datagram_zc.failed() || bad_datagram_zc.error != EINVAL) {
            return failed();
        }

        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
};
