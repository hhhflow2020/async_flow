#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_basic_socket_tasks_fragment.hpp is a runtime_io_test_support implementation fragment"
#endif

class SocketReadableTask final : public IoTaskBase {
public:
    explicit SocketReadableTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read) {
        fd_ = fd;
        armed_ = armed;
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Arm:
            return arm_read();

        case State::Read:
            return finish_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_ = State::Read;
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
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        byte_read_->store(value_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Arm};
    int fd_{-1};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class SocketRepeatedReadableTask final : public IoTaskBase {
public:
    explicit SocketRepeatedReadableTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* reads,
        char* output) {
        fd_ = fd;
        armed_ = armed;
        completed_ = completed;
        reads_ = reads;
        output_ = output;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Arm:
            return arm_read();

        case State::Read:
            return finish_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_ = State::Read;
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        return handle_status(status);
    }

    af::TaskResult finish_read() {
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        return handle_status(status);
    }

    af::TaskResult handle_status(af::IoStatus status) {
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }

        output_[read_count_] = value_;
        ++read_count_;
        reads_->store(static_cast<int>(read_count_), std::memory_order_release);
        if (read_count_ == expected_reads_) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        state_ = State::Arm;
        return arm_read();
    }

    static constexpr std::size_t expected_reads_{2};
    State state_{State::Arm};
    int fd_{-1};
    char value_{0};
    std::size_t read_count_{0};
    af::IoOpState read_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* reads_{nullptr};
    char* output_{nullptr};
};

class SocketWritableTask final : public IoTaskBase {
public:
    explicit SocketWritableTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* armed, std::atomic<int>* completed) {
        fd_ = fd;
        armed_ = armed;
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = af::io_write_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            write_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int fd_{-1};
    char value_{'w'};
    af::IoOpState write_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
};

class UdpRecvTask final : public IoTaskBase {
public:
    explicit UdpRecvTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read,
        std::size_t expected_bytes = 1U) {
        socket_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        byte_read_ = byte_read;
        expected_bytes_ = expected_bytes;
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
        if (!status.ready() || status.bytes != expected_bytes_) {
            return failed();
        }
        if (status.bytes != 0U) {
            byte_read_->store(value_, std::memory_order_release);
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::UdpSocket<IoTestThread> socket_{};
    char value_{0};
    std::size_t expected_bytes_{1U};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UdpSendToTask final : public IoTaskBase {
public:
    explicit UdpSendToTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

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

template <typename TaskBaseT>
class BasicUdpVectoredRecvTask final : public TaskBaseT {
public:
    explicit BasicUdpVectoredRecvTask(typename TaskBaseT::FactoryToken token) : TaskBaseT(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* payload_seen) {
        socket_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        payload_seen_ = payload_seen;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        peer_size_ = sizeof(peer_);
        iov_[0] = iovec{&payload_[0], 1};
        iov_[1] = iovec{&payload_[1], 1};
        const af::IoStatus status = socket_.recvv_from_some(
            *this,
            iov_,
            2,
            reinterpret_cast<sockaddr*>(&peer_),
            &peer_size_,
            recv_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return this->pending();
        }
        if (!status.ready() || status.bytes != sizeof(payload_) || peer_size_ == 0U) {
            return this->failed();
        }

        const int combined =
            (static_cast<unsigned char>(payload_[0]) << 8) |
            static_cast<unsigned char>(payload_[1]);
        payload_seen_->store(combined, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::UdpSocket<IoTestThread> socket_{};
    char payload_[2]{};
    iovec iov_[2]{};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* payload_seen_{nullptr};
};

template <typename TaskBaseT, bool ZeroCopy = false>
class BasicUdpVectoredSendToTask final : public TaskBaseT {
public:
    explicit BasicUdpVectoredSendToTask(typename TaskBaseT::FactoryToken token) : TaskBaseT(token) {}

    bool do_it(
        int fd,
        sockaddr_in address,
        socklen_t address_size,
        char first,
        char second,
        std::atomic<int>* completed,
        std::atomic<int>* bytes_sent) {
        socket_.reset(IoTestThread::IO_0, fd);
        address_ = address;
        address_size_ = address_size;
        payload_[0] = first;
        payload_[1] = second;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        iov_[0] = iovec{&payload_[0], 1};
        iov_[1] = iovec{&payload_[1], 1};
        const af::IoStatus status = [&]() {
            if constexpr (ZeroCopy) {
                return socket_.sendv_zc_to_some(
                    *this,
                    iov_,
                    2,
                    reinterpret_cast<const sockaddr*>(&address_),
                    address_size_,
                    send_);
            } else {
                return socket_.sendv_to_some(
                    *this,
                    iov_,
                    2,
                    reinterpret_cast<const sockaddr*>(&address_),
                    address_size_,
                    send_);
            }
        }();
        if (status.pending()) {
            return this->pending();
        }
        if (!status.ready() || status.bytes != sizeof(payload_)) {
            return this->failed();
        }

        bytes_sent_->store(static_cast<int>(status.bytes), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::UdpSocket<IoTestThread> socket_{};
    sockaddr_in address_{};
    socklen_t address_size_{sizeof(address_)};
    char payload_[2]{};
    iovec iov_[2]{};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* bytes_sent_{nullptr};
};

using UdpVectoredRecvTask = BasicUdpVectoredRecvTask<IoTaskBase>;
using UdpVectoredSendToTask = BasicUdpVectoredSendToTask<IoTaskBase>;
using UdpVectoredZcSendToTask = BasicUdpVectoredSendToTask<IoTaskBase, true>;
using UringUdpVectoredRecvTask = BasicUdpVectoredRecvTask<UringIoTaskBase>;
using UringUdpVectoredSendToTask = BasicUdpVectoredSendToTask<UringIoTaskBase>;
using UringUdpVectoredZcSendToTask = BasicUdpVectoredSendToTask<UringIoTaskBase, true>;

