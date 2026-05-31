#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_wait_cancel_tasks_fragment.hpp is a runtime_io_test_support implementation fragment"
#endif

class SocketHangupTask final : public IoTaskBase {
public:
    explicit SocketHangupTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* armed, std::atomic<int>* closed) {
        fd_ = fd;
        armed_ = armed;
        closed_ = closed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.closed()) {
            return failed();
        }
        closed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int fd_{-1};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* closed_{nullptr};
};

class DuplicateWaitRejectedTask final : public IoTaskBase {
public:
    explicit DuplicateWaitRejectedTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* rejected, std::atomic<int>* error) {
        fd_ = fd;
        rejected_ = rejected;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoResult result{};
        if (wait_io(IoTestThread::IO_0, fd_, af::io_readable, &result)) {
            return failed();
        }
        error_->store(result.error, std::memory_order_release);
        rejected_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int fd_{-1};
    std::atomic<int>* rejected_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class BadFdReadTask final : public IoTaskBase {
public:
    explicit BadFdReadTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        char value = 0;
        af::IoOpState read{};
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            -1,
            &value,
            sizeof(value),
            read);
        if (!status.failed()) {
            return failed();
        }
        error_->store(status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class CancellableSocketReadTask final : public IoTaskBase {
public:
    explicit CancellableSocketReadTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<af::IoOpState*>* state,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* error) {
        fd_ = fd;
        state_ = state;
        armed_ = armed;
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_machine_) {
        case State::Arm:
            return arm_read();

        case State::Finish:
            return finish_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_machine_ = State::Finish;
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        if (!status.pending()) {
            return failed();
        }
        state_->store(&read_, std::memory_order_release);
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    af::TaskResult finish_read() {
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        if (!status.failed() || status.error != ECANCELED) {
            return failed();
        }
        error_->store(status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_machine_{State::Arm};
    int fd_{-1};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<af::IoOpState*>* state_{nullptr};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class CancelIoStateTask final : public IoTaskBase {
public:
    explicit CancelIoStateTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        std::atomic<af::IoOpState*>* state,
        bool cancel_twice,
        std::atomic<int>* completed,
        std::atomic<int>* first_result,
        std::atomic<int>* second_result,
        std::atomic<int>* error) {
        state_ = state;
        cancel_twice_ = cancel_twice;
        completed_ = completed;
        first_result_ = first_result;
        second_result_ = second_result;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState* state = state_->load(std::memory_order_acquire);
        if (state == nullptr) {
            return failed();
        }

        const bool first = IoRuntime::cancel_io(IoTestThread::IO_0, *state);
        first_result_->store(first ? 1 : 0, std::memory_order_release);
        error_->store(state->wait.error, std::memory_order_release);

        if (cancel_twice_) {
            const bool second = IoRuntime::cancel_io(IoTestThread::IO_0, *state);
            second_result_->store(second ? 1 : 0, std::memory_order_release);
        }

        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<af::IoOpState*>* state_{nullptr};
    bool cancel_twice_{false};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* first_result_{nullptr};
    std::atomic<int>* second_result_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class CancelIdleIoStateTask final : public IoTaskBase {
public:
    explicit CancelIdleIoStateTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* result, std::atomic<int>* error) {
        completed_ = completed;
        result_ = result;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState state{};
        const bool ok = IoRuntime::cancel_io(IoTestThread::IO_0, state);
        result_->store(ok ? 1 : 0, std::memory_order_release);
        error_->store(state.wait.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* result_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class TimeoutSocketReadTask final : public IoTaskBase {
public:
    explicit TimeoutSocketReadTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::chrono::nanoseconds timeout,
        std::atomic<af::IoOpState*>* state,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<char>* byte_read) {
        fd_ = fd;
        timeout_ = timeout;
        state_ = state;
        armed_ = armed;
        completed_ = completed;
        error_ = error;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Resume,
    };

    af::TaskResult run() override {
        switch (state_machine_) {
        case State::Arm:
            return arm_read();

        case State::Resume:
            return resume_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_machine_ = State::Resume;
        deadline_.set_after(timeout_);
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        if (!status.pending()) {
            return failed();
        }
        const af::IoStatus timeout = af::arm_io_timeout(
            *this,
            IoTestThread::IO_0,
            deadline_,
            read_);
        if (!timeout.pending()) {
            return failed();
        }
        state_->store(&read_, std::memory_order_release);
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    af::TaskResult resume_read() {
        const af::IoStatus timeout = af::arm_io_timeout(
            *this,
            IoTestThread::IO_0,
            deadline_,
            read_);
        if (timeout.pending()) {
            return pending();
        }
        if (timeout.failed()) {
            error_->store(timeout.error, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (!timeout.ready()) {
            return failed();
        }

        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        if (status.ready() && status.bytes == sizeof(value_)) {
            byte_read_->store(value_, std::memory_order_release);
            error_->store(0, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (status.failed()) {
            error_->store(status.error, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        return failed();
    }

    State state_machine_{State::Arm};
    int fd_{-1};
    std::chrono::nanoseconds timeout_{0};
    char value_{0};
    af::IoOpState read_{};
    af::IoDeadline deadline_{};
    std::atomic<af::IoOpState*>* state_{nullptr};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

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

