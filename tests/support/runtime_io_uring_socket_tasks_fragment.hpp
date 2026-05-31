#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_uring_socket_tasks_fragment.hpp is a runtime_io_test_support implementation fragment"
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

class UringUdpRecvTask final : public UringIoTaskBase {
public:
    explicit UringUdpRecvTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read) {
        socket_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        peer_size_ = sizeof(peer_);
        const af::IoStatus status = socket_.recv_from_some(
            *this,
            &value_,
            sizeof(value_),
            reinterpret_cast<sockaddr*>(&peer_),
            &peer_size_,
            recv_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_) || peer_size_ == 0U) {
            return failed();
        }
        byte_read_->store(value_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::UdpSocket<IoTestThread> socket_{};
    char value_{0};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringUdpSendToTask final : public UringIoTaskBase {
public:
    explicit UringUdpSendToTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        sockaddr_in address,
        socklen_t address_size,
        char value,
        std::atomic<int>* completed,
        std::atomic<int>* bytes_sent) {
        socket_.reset(IoTestThread::IO_0, fd);
        address_ = address;
        address_size_ = address_size;
        value_ = value;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = socket_.send_to_some(
            *this,
            &value_,
            sizeof(value_),
            reinterpret_cast<const sockaddr*>(&address_),
            address_size_,
            send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        bytes_sent_->store(static_cast<int>(status.bytes), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::UdpSocket<IoTestThread> socket_{};
    sockaddr_in address_{};
    socklen_t address_size_{sizeof(address_)};
    char value_{0};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* bytes_sent_{nullptr};
};

class UringTcpAcceptTask final : public UringIoTaskBase {
public:
    explicit UringTcpAcceptTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* armed, std::atomic<int>* completed) {
        listener_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        peer_size_ = sizeof(peer_);
        const af::IoStatus status = listener_.accept_some(
            *this,
            reinterpret_cast<sockaddr*>(&peer_),
            &peer_size_,
            &accepted_fd_,
            accept_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || accepted_fd_ < 0 || peer_size_ == 0U) {
            return failed();
        }
        ::close(accepted_fd_);
        accepted_fd_ = -1;
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpListener<IoTestThread> listener_{};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    int accepted_fd_{-1};
    af::IoOpState accept_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
};

class UringTcpAcceptDirectTask final : public UringIoTaskBase {
public:
    explicit UringTcpAcceptDirectTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<int>* packed_read) {
        listener_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        error_ = error;
        packed_read_ = packed_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Accept,
        Recv,
        Send,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_sparse_slot();

        case State::Accept:
            return accept_direct();

        case State::Recv:
            return recv_request();

        case State::Send:
            return send_response();

        case State::Unregister:
            return complete(0);
        }
        return complete(EIO);
    }

    af::TaskResult register_sparse_slot() {
        const int sparse = -1;
        int error = 0;
        if (!UringIoRuntime::io_register_files(IoTestThread::IO_0, &sparse, 1, &error)) {
            return complete(error == 0 ? EIO : error);
        }
        registered_ = true;
        state_ = State::Accept;
        return again();
    }

    af::TaskResult accept_direct() {
        const af::IoStatus status = listener_.accept_direct(
            *this,
            nullptr,
            nullptr,
            0,
            &accepted_,
            accept_);
        if (status.pending()) {
            if (!armed_once_) {
                armed_once_ = true;
                armed_->fetch_add(1, std::memory_order_release);
            }
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        if (!accepted_.valid()) {
            return complete(EIO);
        }
        state_ = State::Recv;
        return again();
    }

    af::TaskResult recv_request() {
        request_iov_[0] = iovec{&request_[0], 1};
        request_iov_[1] = iovec{&request_[1], 1};
        const af::IoStatus status =
            accepted_.recvv_some(*this, request_iov_, 2, recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return complete(status.failed() ? status.error : EIO);
        }
        packed_read_->store(pack_request(), std::memory_order_release);
        state_ = State::Send;
        return again();
    }

    af::TaskResult send_response() {
        response_iov_[0] = iovec{&response_[0], 1};
        response_iov_[1] = iovec{&response_[1], 1};
        const af::IoStatus status =
            accepted_.sendv_some(*this, response_iov_, 2, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult complete(int error) {
        if (registered_) {
            int unregister_error = 0;
            if (!UringIoRuntime::io_unregister_files(IoTestThread::IO_0, &unregister_error) &&
                error == 0) {
                error = unregister_error == 0 ? EIO : unregister_error;
            }
            registered_ = false;
        }
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    [[nodiscard]] int pack_request() const noexcept {
        return (static_cast<int>(static_cast<unsigned char>(request_[0])) << 8) |
               static_cast<int>(static_cast<unsigned char>(request_[1]));
    }

    State state_{State::Register};
    af::TcpListener<IoTestThread> listener_{};
    af::IoFixedFile<IoTestThread> accepted_{};
    char request_[2]{};
    char response_[2]{'O', 'K'};
    iovec request_iov_[2]{};
    iovec response_iov_[2]{};
    bool registered_{false};
    bool armed_once_{false};
    af::IoOpState accept_{};
    af::IoOpState recv_{};
    af::IoOpState send_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<int>* packed_read_{nullptr};
};

class UringTcpAcceptMultishotTask final : public UringIoTaskBase {
public:
    explicit UringTcpAcceptMultishotTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        int target_accepts,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* accepted_count,
        std::atomic<int>* error) {
        listener_.reset(IoTestThread::IO_0, fd);
        target_accepts_ = target_accepts;
        armed_ = armed;
        completed_ = completed;
        accepted_count_ = accepted_count;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Accept,
        Cancel,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Accept:
            return accept_one();
        case State::Cancel:
            return finish_cancel();
        }
        return failed();
    }

    af::TaskResult accept_one() {
        const af::IoStatus status = listener_.accept_multishot(
            *this,
            nullptr,
            nullptr,
            &accepted_fd_,
            accept_);
        if (status.pending()) {
            if (!armed_once_) {
                armed_once_ = true;
                armed_->fetch_add(1, std::memory_order_release);
            }
            return pending();
        }
        if (status.failed()) {
            error_->store(status.error, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (!status.ready() || accepted_fd_ < 0) {
            return failed();
        }

        ::close(accepted_fd_);
        accepted_fd_ = -1;
        const int accepted = accepted_count_->fetch_add(1, std::memory_order_acq_rel) + 1;
        if (accepted < target_accepts_) {
            return pending();
        }

        if (!accept_.waiting) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (!UringIoRuntime::cancel_io(IoTestThread::IO_0, accept_)) {
            error_->store(accept_.wait.error == 0 ? EIO : accept_.wait.error, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        state_ = State::Cancel;
        return pending();
    }

    af::TaskResult finish_cancel() {
        int ignored = -1;
        const af::IoStatus status =
            listener_.accept_multishot(*this, nullptr, nullptr, &ignored, accept_);
        if (status.pending()) {
            return pending();
        }
        if (!status.failed() || status.error != ECANCELED) {
            return failed();
        }
        error_->store(0, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Accept};
    af::TcpListener<IoTestThread> listener_{};
    int accepted_fd_{-1};
    int target_accepts_{0};
    bool armed_once_{false};
    af::IoOpState accept_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* accepted_count_{nullptr};
    std::atomic<int>* error_{nullptr};
};

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
        return failed();
    }

    af::TaskResult register_ring() {
        int init_error = 0;
        if (!ring_.init(buffer_count, init_error)) {
            return complete(init_error == 0 ? EIO : init_error);
        }
        af::IoProvidedBuffer buffers[buffer_count]{};
        for (std::uint16_t i = 0; i < buffer_count; ++i) {
            buffers[i] = af::IoProvidedBuffer{&buffers_[i], sizeof(buffers_[i]), i};
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
        const af::IoStatus status = datagram_mode_
            ? datagram_.recv_multishot(*this, buffer_group, &buffer_id, recv_)
            : stream_.recv_multishot(*this, buffer_group, &buffer_id, recv_);
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
        if (!status.ready() || status.bytes != 1U || buffer_id >= buffer_count) {
            return complete(EIO);
        }

        const int previous = read_count_->fetch_add(1, std::memory_order_acq_rel);
        const int shifted = previous == 0 ? 8 : 0;
        packed_read_->fetch_or(
            static_cast<int>(static_cast<unsigned char>(buffers_[buffer_id])) << shifted,
            std::memory_order_acq_rel);

        const af::IoProvidedBuffer buffer{
            &buffers_[buffer_id],
            sizeof(buffers_[buffer_id]),
            buffer_id};
        int add_error = 0;
        if (!ring_.add(&buffer, 1, add_error)) {
            return stop_recv(add_error == 0 ? EIO : add_error);
        }

        if (previous + 1 < target_reads_) {
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
        const af::IoStatus status = datagram_mode_
            ? datagram_.recv_multishot(*this, buffer_group, &ignored, recv_)
            : stream_.recv_multishot(*this, buffer_group, &ignored, recv_);
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

class UringTcpConnectTask final : public UringIoTaskBase {
public:
    explicit UringTcpConnectTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, sockaddr_in address, socklen_t address_size, std::atomic<int>* completed) {
        stream_.reset(IoTestThread::IO_0, fd);
        address_ = address;
        address_size_ = address_size;
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.connect(
            *this,
            reinterpret_cast<const sockaddr*>(&address_),
            address_size_,
            connect_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    sockaddr_in address_{};
    socklen_t address_size_{sizeof(address_)};
    af::IoOpState connect_{};
    std::atomic<int>* completed_{nullptr};
};

