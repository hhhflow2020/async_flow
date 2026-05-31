#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_stream_tasks_fragment.hpp is a runtime_io_test_support implementation fragment"
#endif

class TcpConnectTask final : public IoTaskBase {
public:
    explicit TcpConnectTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

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

class StreamAdapterEchoTask final : public IoTaskBase {
public:
    explicit StreamAdapterEchoTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

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
        const af::IoStatus status = stream_.recv_some(
            *this,
            &request_,
            sizeof(request_),
            read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }
        byte_read_->store(request_, std::memory_order_release);
        state_ = State::SendResponse;
        return again();
    }

    af::TaskResult send_response() {
        const af::IoStatus status = stream_.send_some(
            *this,
            &response_,
            sizeof(response_),
            write_);
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
    char request_{0};
    char response_{'R'};
    af::IoOpState read_{};
    af::IoOpState write_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

template <typename BaseTask>
class BasicStreamShutdownTask final : public BaseTask {
public:
    explicit BasicStreamShutdownTask(typename BaseTask::FactoryToken token) : BaseTask(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<int>* error) {
        stream_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        error_ = error;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return shutdown_write();
    }

    af::TaskResult shutdown_write() {
        const af::IoStatus status = stream_.shutdown(*this, SHUT_WR, shutdown_);
        if (status.pending()) {
            return this->pending();
        }
        error_->store(status.ready() ? 0 : status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::TcpStream<IoTestThread> stream_{};
    af::IoOpState shutdown_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

using StreamShutdownTask = BasicStreamShutdownTask<IoTaskBase>;
using UringStreamShutdownTask = BasicStreamShutdownTask<UringIoTaskBase>;

class StreamVectoredEchoTask final : public IoTaskBase {
public:
    explicit StreamVectoredEchoTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

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
        iovec request_iov[2]{
            iovec{&request_[0], 1},
            iovec{&request_[1], 1},
        };
        const af::IoStatus status = stream_.recvv_some(*this, request_iov, 2, read_);
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
        iovec response_iov[2]{
            iovec{&response_[0], 1},
            iovec{&response_[1], 1},
        };
        const af::IoStatus status = stream_.sendv_some(*this, response_iov, 2, write_);
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
    char response_[2]{'X', 'Y'};
    af::IoOpState read_{};
    af::IoOpState write_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* request_seen_{nullptr};
};

class ZeroCopyBoundaryTask final : public IoTaskBase {
public:
    explicit ZeroCopyBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState sendfile_zero{};
        af::IoOpState sendfile_bad{};
        af::IoOpState send_zc_zero{};
        af::IoOpState send_zc_null{};
        af::IoOpState send_zc_bad{};
        af::IoOpState sendv_zc_zero{};
        af::IoOpState sendv_zc_null{};
        af::IoOpState sendv_zc_bad{};
        af::IoOpState send_zc_to_zero{};
        af::IoOpState send_zc_to_null{};
        af::IoOpState send_zc_to_bad{};
        af::IoOpState sendv_zc_to_zero{};
        af::IoOpState sendv_zc_to_bad{};
        af::IoOpState splice_zero{};
        af::IoOpState splice_bad{};
        af::IoOffset offset = 0;
        const char value = 'Z';
        iovec valid_iov{const_cast<char*>(&value), 1};

        const af::IoStatus sendfile_zero_status = af::io_sendfile_some(
            *this,
            IoTestThread::IO_0,
            -1,
            -1,
            nullptr,
            0,
            sendfile_zero);
        const af::IoStatus sendfile_bad_status = af::io_sendfile_some(
            *this,
            IoTestThread::IO_0,
            -1,
            -1,
            &offset,
            1,
            sendfile_bad);
        const af::IoStatus send_zc_zero_status = af::io_send_zc_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            0,
            send_zc_zero);
        const af::IoStatus send_zc_null_status = af::io_send_zc_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            1,
            send_zc_null);
        const af::IoStatus send_zc_bad_status = af::io_send_zc_some(
            *this,
            IoTestThread::IO_0,
            -1,
            &value,
            sizeof(value),
            send_zc_bad);
        const af::IoStatus sendv_zc_zero_status = af::io_sendv_zc_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            0,
            sendv_zc_zero);
        const af::IoStatus sendv_zc_null_status = af::io_sendv_zc_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            1,
            sendv_zc_null);
        const af::IoStatus sendv_zc_bad_status = af::io_sendv_zc_some(
            *this,
            IoTestThread::IO_0,
            -1,
            &valid_iov,
            1,
            sendv_zc_bad);
        const af::IoStatus send_zc_to_zero_status = af::io_send_zc_to_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            0,
            nullptr,
            0,
            send_zc_to_zero);
        const af::IoStatus send_zc_to_null_status = af::io_send_zc_to_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            1,
            nullptr,
            0,
            send_zc_to_null);
        const af::IoStatus send_zc_to_bad_status = af::io_send_zc_to_some(
            *this,
            IoTestThread::IO_0,
            -1,
            &value,
            sizeof(value),
            nullptr,
            0,
            send_zc_to_bad);
        const af::IoStatus sendv_zc_to_zero_status = af::io_sendv_zc_to_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            0,
            nullptr,
            0,
            sendv_zc_to_zero);
        const af::IoStatus sendv_zc_to_bad_status = af::io_sendv_zc_to_some(
            *this,
            IoTestThread::IO_0,
            -1,
            &valid_iov,
            1,
            nullptr,
            0,
            sendv_zc_to_bad);
        const af::IoStatus splice_zero_status = af::io_splice_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            -1,
            nullptr,
            0,
            0,
            splice_zero);
        const af::IoStatus splice_bad_status = af::io_splice_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            -1,
            nullptr,
            1,
            0,
            splice_bad);
        if (!sendfile_zero_status.ready() || sendfile_zero_status.bytes != 0U ||
            !send_zc_zero_status.ready() || send_zc_zero_status.bytes != 0U ||
            !send_zc_null_status.failed() || send_zc_null_status.error != EINVAL ||
            !send_zc_bad_status.failed() || send_zc_bad_status.error != EBADF ||
            !sendv_zc_zero_status.ready() || sendv_zc_zero_status.bytes != 0U ||
            !sendv_zc_null_status.failed() || sendv_zc_null_status.error != EINVAL ||
            !sendv_zc_bad_status.failed() || sendv_zc_bad_status.error != EBADF ||
            !send_zc_to_zero_status.ready() || send_zc_to_zero_status.bytes != 0U ||
            !send_zc_to_null_status.failed() || send_zc_to_null_status.error != EINVAL ||
            !send_zc_to_bad_status.failed() || send_zc_to_bad_status.error != EBADF ||
            !sendv_zc_to_zero_status.ready() || sendv_zc_to_zero_status.bytes != 0U ||
            !sendv_zc_to_bad_status.failed() || sendv_zc_to_bad_status.error != EBADF ||
            !splice_zero_status.ready() || splice_zero_status.bytes != 0U ||
            !sendfile_bad_status.failed() || sendfile_bad_status.error != EBADF ||
            !splice_bad_status.failed() || splice_bad_status.error != EBADF) {
            return failed();
        }
        error_->store(sendfile_bad_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class SendZcSocketTask final : public IoTaskBase {
public:
    explicit SendZcSocketTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        const char* payload,
        std::size_t total_size,
        std::size_t chunk_size,
        std::atomic<int>* completed,
        std::atomic<int>* calls,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        payload_ = payload;
        total_size_ = total_size;
        chunk_size_ = chunk_size == 0U ? total_size : chunk_size;
        completed_ = completed;
        calls_ = calls;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_next();
    }

    af::TaskResult send_next() {
        if (sent_ >= total_size_) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        const std::size_t remaining = total_size_ - sent_;
        const std::size_t count = remaining < chunk_size_ ? remaining : chunk_size_;
        const af::IoStatus status =
            stream_.send_zc_some(*this, payload_ + sent_, count, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > remaining) {
            return failed();
        }

        sent_ += status.bytes;
        calls_->fetch_add(1, std::memory_order_release);
        bytes_sent_->store(sent_, std::memory_order_release);
        return again();
    }

    af::TcpStream<IoTestThread> stream_{};
    const char* payload_{nullptr};
    std::size_t total_size_{0};
    std::size_t chunk_size_{0};
    std::size_t sent_{0};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* calls_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class SendvZcSocketTask final : public IoTaskBase {
public:
    explicit SendvZcSocketTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        const char* first,
        std::size_t first_size,
        const char* second,
        std::size_t second_size,
        std::atomic<int>* completed,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        first_ = first;
        first_size_ = first_size;
        second_ = second;
        second_size_ = second_size;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const std::size_t total_size = first_size_ + second_size_;
        if (sent_ >= total_size) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        int iov_count = 0;
        if (sent_ < first_size_) {
            iov_[iov_count++] = iovec{
                const_cast<char*>(first_ + sent_),
                first_size_ - sent_};
            iov_[iov_count++] = iovec{const_cast<char*>(second_), second_size_};
        } else {
            const std::size_t second_offset = sent_ - first_size_;
            iov_[iov_count++] = iovec{
                const_cast<char*>(second_ + second_offset),
                second_size_ - second_offset};
        }

        const af::IoStatus status = stream_.sendv_zc_some(*this, iov_, iov_count, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > total_size - sent_) {
            return failed();
        }

        sent_ += status.bytes;
        bytes_sent_->store(sent_, std::memory_order_release);
        return again();
    }

    af::TcpStream<IoTestThread> stream_{};
    const char* first_{nullptr};
    const char* second_{nullptr};
    std::size_t first_size_{0};
    std::size_t second_size_{0};
    std::size_t sent_{0};
    iovec iov_[2]{};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class SendfileSocketTask final : public IoTaskBase {
public:
    explicit SendfileSocketTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        int file_fd,
        std::size_t total_size,
        std::size_t chunk_size,
        bool use_null_offset,
        std::atomic<int>* completed,
        std::atomic<int>* calls,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        file_fd_ = file_fd;
        total_size_ = total_size;
        chunk_size_ = chunk_size == 0U ? total_size : chunk_size;
        use_null_offset_ = use_null_offset;
        completed_ = completed;
        calls_ = calls;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_next();
    }

    af::TaskResult send_next() {
        if (sent_ >= total_size_) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        const std::size_t remaining = total_size_ - sent_;
        const std::size_t count = remaining < chunk_size_ ? remaining : chunk_size_;
        af::IoOffset* offset = use_null_offset_ ? nullptr : &offset_;
        const af::IoStatus status = stream_.sendfile_some(*this, file_fd_, offset, count, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > remaining) {
            return failed();
        }
        sent_ += status.bytes;
        calls_->fetch_add(1, std::memory_order_release);
        bytes_sent_->store(sent_, std::memory_order_release);
        return again();
    }

    af::TcpStream<IoTestThread> stream_{};
    int file_fd_{-1};
    af::IoOffset offset_{0};
    std::size_t total_size_{0};
    std::size_t chunk_size_{0};
    std::size_t sent_{0};
    bool use_null_offset_{false};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* calls_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class PendingSendZcTask final : public IoTaskBase {
public:
    explicit PendingSendZcTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        std::atomic<int>* pending_seen,
        std::atomic<int>* completed,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        pending_seen_ = pending_seen;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_byte();
    }

    af::TaskResult send_byte() {
        const af::IoStatus status = stream_.send_zc_some(*this, &value_, sizeof(value_), send_);
        if (status.pending()) {
            pending_seen_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        bytes_sent_->store(status.bytes, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{'Z'};
    af::IoOpState send_{};
    std::atomic<int>* pending_seen_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class PendingSendfileTask final : public IoTaskBase {
public:
    explicit PendingSendfileTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        int file_fd,
        std::atomic<int>* pending_seen,
        std::atomic<int>* completed,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        file_fd_ = file_fd;
        pending_seen_ = pending_seen;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_byte();
    }

    af::TaskResult send_byte() {
        const af::IoStatus status = stream_.sendfile_some(*this, file_fd_, &offset_, 1, send_);
        if (status.pending()) {
            pending_seen_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != 1U || offset_ != 1) {
            return failed();
        }
        bytes_sent_->store(status.bytes, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    int file_fd_{-1};
    af::IoOffset offset_{0};
    af::IoOpState send_{};
    std::atomic<int>* pending_seen_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class UringPendingSendfilePollTask final : public UringIoTaskBase {
public:
    explicit UringPendingSendfilePollTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        int file_fd,
        std::atomic<af::IoOpState*>* state,
        std::atomic<int>* wait_kind,
        std::atomic<int>* pending_seen,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        file_fd_ = file_fd;
        state_ = state;
        wait_kind_ = wait_kind;
        pending_seen_ = pending_seen;
        completed_ = completed;
        error_ = error;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.sendfile_some(*this, file_fd_, &offset_, 1, send_);
        if (status.pending()) {
            state_->store(&send_, std::memory_order_release);
            wait_kind_->store(static_cast<int>(send_.wait_kind), std::memory_order_release);
            pending_seen_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (status.failed()) {
            error_->store(status.error, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (!status.ready() || status.bytes != 1U || offset_ != 1) {
            return failed();
        }
        bytes_sent_->store(status.bytes, std::memory_order_release);
        error_->store(0, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    int file_fd_{-1};
    af::IoOffset offset_{0};
    af::IoOpState send_{};
    std::atomic<af::IoOpState*>* state_{nullptr};
    std::atomic<int>* wait_kind_{nullptr};
    std::atomic<int>* pending_seen_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class SplicePipeTask final : public IoTaskBase {
public:
    explicit SplicePipeTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int input_fd,
        int output_fd,
        std::size_t total_size,
        std::atomic<int>* completed,
        std::atomic<std::size_t>* bytes_spliced) {
        input_fd_ = input_fd;
        output_fd_ = output_fd;
        total_size_ = total_size;
        completed_ = completed;
        bytes_spliced_ = bytes_spliced;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return splice_next();
    }

    af::TaskResult splice_next() {
        if (spliced_ >= total_size_) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        const af::IoStatus status = af::io_splice_some(
            *this,
            IoTestThread::IO_0,
            input_fd_,
            nullptr,
            output_fd_,
            nullptr,
            total_size_ - spliced_,
            SPLICE_F_NONBLOCK,
            splice_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > total_size_ - spliced_) {
            return failed();
        }
        spliced_ += status.bytes;
        bytes_spliced_->store(spliced_, std::memory_order_release);
        return again();
    }

    int input_fd_{-1};
    int output_fd_{-1};
    std::size_t total_size_{0};
    std::size_t spliced_{0};
    af::IoOpState splice_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::size_t>* bytes_spliced_{nullptr};
};

