#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_uring_socket_stream_tasks_fragment.hpp is a runtime_io_uring_socket_tasks implementation fragment"
#endif

class UringStreamFallbackTask final : public UringIoTaskBase {
public:
    explicit UringStreamFallbackTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read) {
        stream_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.recv_some(*this, &value_, sizeof(value_), read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        byte_read_->store(value_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringCancellableSocketRecvTask final : public UringIoTaskBase {
public:
    explicit UringCancellableSocketRecvTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<af::IoOpState*>* state,
        std::atomic<int>* wait_kind,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<int>* bytes) {
        stream_.reset(IoTestThread::IO_0, fd);
        state_ = state;
        wait_kind_ = wait_kind;
        armed_ = armed;
        completed_ = completed;
        error_ = error;
        bytes_ = bytes;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.recv_some(*this, &value_, sizeof(value_), recv_);
        if (status.pending()) {
            state_->store(&recv_, std::memory_order_release);
            wait_kind_->store(static_cast<int>(recv_.wait_kind), std::memory_order_release);
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }

        if (status.failed()) {
            error_->store(status.error, std::memory_order_release);
        } else if (status.ready()) {
            error_->store(0, std::memory_order_release);
            bytes_->store(static_cast<int>(status.bytes), std::memory_order_release);
        } else if (status.closed()) {
            error_->store(0, std::memory_order_release);
            bytes_->store(0, std::memory_order_release);
        } else {
            return failed();
        }

        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{0};
    af::IoOpState recv_{};
    std::atomic<af::IoOpState*>* state_{nullptr};
    std::atomic<int>* wait_kind_{nullptr};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<int>* bytes_{nullptr};
};

class UringCancelIoStateTask final : public UringIoTaskBase {
public:
    explicit UringCancelIoStateTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(
        std::atomic<af::IoOpState*>* state,
        std::atomic<int>* completed,
        std::atomic<int>* result,
        std::atomic<int>* error) {
        state_ = state;
        completed_ = completed;
        result_ = result;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState* state = state_->load(std::memory_order_acquire);
        if (state == nullptr) {
            return failed();
        }

        const bool ok = UringIoRuntime::cancel_io(IoTestThread::IO_0, *state);
        result_->store(ok ? 1 : 0, std::memory_order_release);
        error_->store(state->wait.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<af::IoOpState*>* state_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* result_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class UringStreamSendTask final : public UringIoTaskBase {
public:
    explicit UringStreamSendTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* completed) {
        stream_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.send_some(*this, &value_, sizeof(value_), write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{'S'};
    af::IoOpState write_{};
    std::atomic<int>* completed_{nullptr};
};

class UringStreamSendZcTask final : public UringIoTaskBase {
public:
    explicit UringStreamSendZcTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* completed) {
        stream_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_value();
    }

    af::TaskResult send_value() {
        const af::IoStatus status = stream_.send_zc_some(*this, &value_, sizeof(value_), write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{'Z'};
    af::IoOpState write_{};
    std::atomic<int>* completed_{nullptr};
};

class UringStreamVectoredTask final : public UringIoTaskBase {
public:
    explicit UringStreamVectoredTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* request_seen) {
        stream_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        request_seen_ = request_seen;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        ReadRequest,
        SendResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::ReadRequest:
            return read_request();

        case State::SendResponse:
            return send_response();
        }
        return failed();
    }

    af::TaskResult read_request() {
        request_iov_[0] = iovec{&request_[0], 1};
        request_iov_[1] = iovec{&request_[1], 1};
        const af::IoStatus status = stream_.recvv_some(*this, request_iov_, 2, read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }
        const int combined =
            (static_cast<int>(request_[0]) << 8) | static_cast<unsigned char>(request_[1]);
        request_seen_->store(combined, std::memory_order_release);
        state_ = State::SendResponse;
        return again();
    }

    af::TaskResult send_response() {
        response_iov_[0] = iovec{&response_[0], 1};
        response_iov_[1] = iovec{&response_[1], 1};
        const af::IoStatus status = stream_.sendv_some(*this, response_iov_, 2, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::ReadRequest};
    af::TcpStream<IoTestThread> stream_{};
    char request_[2]{};
    char response_[2]{'U', 'V'};
    iovec request_iov_[2]{};
    iovec response_iov_[2]{};
    af::IoOpState read_{};
    af::IoOpState write_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* request_seen_{nullptr};
};

