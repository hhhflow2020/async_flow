#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_file_rw_tasks_fragment.hpp is a runtime_io_file_tasks implementation fragment"
#endif

class UringFileReadWriteTask final : public UringIoTaskBase {
public:
    explicit UringFileReadWriteTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<char>* byte_read) {
        file_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Write,
        Fsync,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();
        }
        return failed();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_value() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_at(*this, &read_, sizeof(read_), 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_)) {
            return failed();
        }
        byte_read_->store(read_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Write};
    af::IoFile<IoTestThread> file_{};
    char value_{'F'};
    char read_{0};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringFileVectoredReadWriteTask final : public UringIoTaskBase {
public:
    explicit UringFileVectoredReadWriteTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<int>* bytes_read) {
        file_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        bytes_read_ = bytes_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Write,
        Fsync,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();
        }
        return failed();
    }

    af::TaskResult write_value() {
        write_iov_[0] = iovec{&first_, 1};
        write_iov_[1] = iovec{&second_, 1};
        const af::IoStatus status = file_.writev_at(*this, write_iov_, 2, 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 2U) {
            return failed();
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_value() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        read_iov_[0] = iovec{&read_[0], 1};
        read_iov_[1] = iovec{&read_[1], 1};
        const af::IoStatus status = file_.readv_at(*this, read_iov_, 2, 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 2U || read_[0] != first_ || read_[1] != second_) {
            return failed();
        }
        bytes_read_->store(static_cast<int>(status.bytes), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Write};
    af::IoFile<IoTestThread> file_{};
    char first_{'V'};
    char second_{'W'};
    char read_[2]{};
    iovec write_iov_[2]{};
    iovec read_iov_[2]{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* bytes_read_{nullptr};
};

class UringFileCurrentOffsetTask final : public UringIoTaskBase {
public:
    explicit UringFileCurrentOffsetTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* completed,
        std::atomic<int>* packed_read,
        std::atomic<int>* pending_submits) {
        file_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        packed_read_ = packed_read;
        pending_submits_ = pending_submits;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        WriteOne,
        WriteVector,
        Fsync,
        SeekStart,
        ReadOne,
        ReadVector,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::WriteOne:
            return write_one();

        case State::WriteVector:
            return write_vector();

        case State::Fsync:
            return fsync_file();

        case State::SeekStart:
            return seek_start();

        case State::ReadOne:
            return read_one();

        case State::ReadVector:
            return read_vector();
        }
        return failed();
    }

    af::TaskResult write_one() {
        const af::IoStatus status = file_.write_some(*this, &first_, sizeof(first_), write_one_);
        if (status.pending()) {
            pending_submits_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(first_)) {
            return failed();
        }
        state_ = State::WriteVector;
        return again();
    }

    af::TaskResult write_vector() {
        write_iov_[0] = iovec{&second_, 1};
        write_iov_[1] = iovec{&third_, 1};
        const af::IoStatus status = file_.writev_some(*this, write_iov_, 2, write_vector_);
        if (status.pending()) {
            pending_submits_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != 2U) {
            return failed();
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_file() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            pending_submits_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::SeekStart;
        return again();
    }

    af::TaskResult seek_start() {
#if defined(__linux__)
        if (::lseek(file_.fd(), 0, SEEK_SET) < 0) {
            return failed();
        }
        state_ = State::ReadOne;
        return again();
#else
        return failed();
#endif
    }

    af::TaskResult read_one() {
        const af::IoStatus status = file_.read_some(*this, &read_first_, sizeof(read_first_), read_one_);
        if (status.pending()) {
            pending_submits_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_first_) || read_first_ != first_) {
            return failed();
        }
        state_ = State::ReadVector;
        return again();
    }

    af::TaskResult read_vector() {
        read_iov_[0] = iovec{&read_rest_[0], 1};
        read_iov_[1] = iovec{&read_rest_[1], 1};
        const af::IoStatus status = file_.readv_some(*this, read_iov_, 2, read_vector_);
        if (status.pending()) {
            pending_submits_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_rest_) ||
            read_rest_[0] != second_ || read_rest_[1] != third_) {
            return failed();
        }
        const int packed =
            (static_cast<int>(static_cast<unsigned char>(read_first_)) << 16) |
            (static_cast<int>(static_cast<unsigned char>(read_rest_[0])) << 8) |
            static_cast<int>(static_cast<unsigned char>(read_rest_[1]));
        packed_read_->store(packed, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::WriteOne};
    af::IoFile<IoTestThread> file_{};
    char first_{'A'};
    char second_{'B'};
    char third_{'C'};
    char read_first_{0};
    char read_rest_[2]{};
    iovec write_iov_[2]{};
    iovec read_iov_[2]{};
    af::IoOpState write_one_{};
    af::IoOpState write_vector_{};
    af::IoOpState fsync_{};
    af::IoOpState read_one_{};
    af::IoOpState read_vector_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* packed_read_{nullptr};
    std::atomic<int>* pending_submits_{nullptr};
};

