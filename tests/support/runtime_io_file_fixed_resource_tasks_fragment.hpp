#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_file_fixed_resource_tasks_fragment.hpp is a runtime_io_file_tasks implementation fragment"
#endif

class UringFixedBufferFileTask final : public UringIoTaskBase {
public:
    explicit UringFixedBufferFileTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read) {
        file_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Write,
        Fsync,
        Read,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_buffer();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::Unregister:
            return unregister_buffer();
        }
        return failed();
    }

    af::TaskResult register_buffer() {
        const af::IoStatus no_buffer = file_.write_fixed_at(
            *this,
            buffer_,
            1,
            0,
            0,
            no_buffer_);
        if (!no_buffer.failed() || no_buffer.error != ENOBUFS) {
            return failed();
        }

        iovec iov{buffer_, sizeof(buffer_)};
        int error = 0;
        if (!UringIoRuntime::io_register_buffers(IoTestThread::IO_0, &iov, 1, &error)) {
            return failed();
        }

        const af::IoStatus bad_index = file_.write_fixed_at(
            *this,
            buffer_,
            1,
            0,
            1,
            bad_index_);
        if (!bad_index.failed() || bad_index.error != EINVAL) {
            return failed();
        }

        buffer_[0] = value_;
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_fixed_at(
            *this,
            af::IoFixedBuffer{buffer_, 1, 0},
            0,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U) {
            return failed();
        }
        buffer_[0] = 0;
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
        const af::IoStatus status = file_.read_fixed_at(
            *this,
            af::IoFixedBuffer{buffer_, 1, 0},
            0,
            read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U || buffer_[0] != value_) {
            return failed();
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_buffer() {
        int error = 0;
        if (!UringIoRuntime::io_unregister_buffers(IoTestThread::IO_0, &error)) {
            return failed();
        }
        byte_read_->store(buffer_[0], std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Register};
    af::IoFile<IoTestThread> file_{};
    alignas(64) char buffer_[64]{};
    char value_{'B'};
    af::IoOpState no_buffer_{};
    af::IoOpState bad_index_{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringFixedFileTask final : public UringIoTaskBase {
public:
    explicit UringFixedFileTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<char>* byte_read) {
        fd_ = fd;
        file_.reset(IoTestThread::IO_0, 0);
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Write,
        WriteVectored,
        Fsync,
        Read,
        ReadVectored,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_file();

        case State::Write:
            return write_value();

        case State::WriteVectored:
            return write_vectored();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::ReadVectored:
            return read_vectored();

        case State::Unregister:
            return unregister_file();
        }
        return failed();
    }

    af::TaskResult register_file() {
        const af::IoStatus no_table = file_.write_at(
            *this,
            &value_,
            sizeof(value_),
            0,
            no_table_);
        if (!no_table.failed() || no_table.error != ENXIO) {
            return failed();
        }

        int error = 0;
        if (!UringIoRuntime::io_register_files(IoTestThread::IO_0, &fd_, 1, &error)) {
            return failed();
        }

        int duplicate_error = 0;
        if (UringIoRuntime::io_register_files(
                IoTestThread::IO_0,
                &fd_,
                1,
                &duplicate_error) ||
            duplicate_error != EALREADY) {
            return failed();
        }

        af::IoFixedFile<IoTestThread> bad_file(IoTestThread::IO_0, 1);
        const af::IoStatus bad_index = bad_file.write_at(
            *this,
            &value_,
            sizeof(value_),
            0,
            bad_index_);
        if (!bad_index.failed() || bad_index.error != EINVAL) {
            return failed();
        }

        const af::IoStatus no_buffer = file_.write_fixed_at(
            *this,
            buffer_,
            1,
            0,
            0,
            no_buffer_);
        if (!no_buffer.failed() || no_buffer.error != ENOBUFS) {
            return failed();
        }

        iovec iov{buffer_, sizeof(buffer_)};
        int buffer_error = 0;
        if (!UringIoRuntime::io_register_buffers(IoTestThread::IO_0, &iov, 1, &buffer_error)) {
            return failed();
        }

        buffer_[0] = value_;
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_fixed_at(
            *this,
            af::IoFixedBuffer{buffer_, 1, 0},
            0,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U) {
            return failed();
        }
        buffer_[0] = 0;
        state_ = State::WriteVectored;
        return again();
    }

    af::TaskResult write_vectored() {
        write_iov_[0] = iovec{&vector_write_[0], 1};
        write_iov_[1] = iovec{&vector_write_[1], 1};
        const af::IoStatus status = file_.writev_at(
            *this,
            write_iov_,
            2,
            1,
            writev_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(vector_write_)) {
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
        const af::IoStatus status = file_.read_fixed_at(
            *this,
            af::IoFixedBuffer{buffer_, 1, 0},
            0,
            read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U || buffer_[0] != value_) {
            return failed();
        }
        state_ = State::ReadVectored;
        return again();
    }

    af::TaskResult read_vectored() {
        read_iov_[0] = iovec{&vector_read_[0], 1};
        read_iov_[1] = iovec{&vector_read_[1], 1};
        const af::IoStatus status = file_.readv_at(
            *this,
            read_iov_,
            2,
            1,
            readv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() ||
            status.bytes != sizeof(vector_read_) ||
            vector_read_[0] != vector_write_[0] ||
            vector_read_[1] != vector_write_[1]) {
            return failed();
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_file() {
        int error = 0;
        if (!UringIoRuntime::io_unregister_buffers(IoTestThread::IO_0, &error)) {
            return failed();
        }
        if (!UringIoRuntime::io_unregister_files(IoTestThread::IO_0, &error)) {
            return failed();
        }
        byte_read_->store(buffer_[0], std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Register};
    int fd_{-1};
    af::IoFixedFile<IoTestThread> file_{};
    alignas(64) char buffer_[64]{};
    char value_{'F'};
    char vector_write_[2]{'I', 'O'};
    char vector_read_[2]{};
    iovec write_iov_[2]{};
    iovec read_iov_[2]{};
    af::IoOpState no_table_{};
    af::IoOpState bad_index_{};
    af::IoOpState no_buffer_{};
    af::IoOpState write_{};
    af::IoOpState writev_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    af::IoOpState readv_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringFixedFileUpdateTask final : public UringIoTaskBase {
public:
    explicit UringFixedFileUpdateTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int first_fd,
        int second_fd,
        std::atomic<int>* completed,
        std::atomic<int>* packed_read) {
        first_fd_ = first_fd;
        second_fd_ = second_fd;
        completed_ = completed;
        packed_read_ = packed_read;
        file_.reset(IoTestThread::IO_0, 0);
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        ReadFirst,
        Update,
        ReadSecond,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_file();

        case State::ReadFirst:
            return read_first();

        case State::Update:
            return update_file();

        case State::ReadSecond:
            return read_second();

        case State::Unregister:
            return unregister_file();
        }
        return failed();
    }

    af::TaskResult register_file() {
        int error = 0;
        if (UringIoRuntime::io_update_registered_files(
                IoTestThread::IO_0,
                0,
                &second_fd_,
                1,
                &error) ||
            error != ENOENT) {
            return failed();
        }
        if (!UringIoRuntime::io_register_files(IoTestThread::IO_0, &first_fd_, 1, &error)) {
            return failed();
        }
        if (UringIoRuntime::io_update_registered_files(
                IoTestThread::IO_0,
                1,
                &second_fd_,
                1,
                &error) ||
            error != EINVAL) {
            return failed();
        }
        state_ = State::ReadFirst;
        return again();
    }

    af::TaskResult read_first() {
        const af::IoStatus status = file_.read_at(
            *this,
            &first_read_,
            sizeof(first_read_),
            0,
            first_read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(first_read_) || first_read_ != first_value_) {
            return failed();
        }
        state_ = State::Update;
        return again();
    }

    af::TaskResult update_file() {
        int error = 0;
        if (!UringIoRuntime::io_update_registered_files(
                IoTestThread::IO_0,
                0,
                &second_fd_,
                1,
                &error)) {
            return failed();
        }
        state_ = State::ReadSecond;
        return again();
    }

    af::TaskResult read_second() {
        const af::IoStatus status = file_.read_at(
            *this,
            &second_read_,
            sizeof(second_read_),
            0,
            second_read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(second_read_) || second_read_ != second_value_) {
            return failed();
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_file() {
        int error = 0;
        if (!UringIoRuntime::io_unregister_files(IoTestThread::IO_0, &error)) {
            return failed();
        }
        const int packed =
            (static_cast<int>(static_cast<unsigned char>(first_read_)) << 8) |
            static_cast<int>(static_cast<unsigned char>(second_read_));
        packed_read_->store(packed, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Register};
    int first_fd_{-1};
    int second_fd_{-1};
    af::IoFixedFile<IoTestThread> file_{};
    char first_value_{'1'};
    char second_value_{'2'};
    char first_read_{0};
    char second_read_{0};
    af::IoOpState first_read_state_{};
    af::IoOpState second_read_state_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* packed_read_{nullptr};
};

class UringOpenAtDirectFileTask final : public UringIoTaskBase {
public:
    explicit UringOpenAtDirectFileTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        const char* path,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<char>* byte_read) {
        path_ = path;
        completed_ = completed;
        error_ = error;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Open,
        Write,
        Fsync,
        Read,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_sparse_slot();

        case State::Open:
            return open_direct();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

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
        state_ = State::Open;
        return again();
    }

    af::TaskResult open_direct() {
        const af::IoStatus status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            path_,
            O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC,
            0600U,
            0,
            &file_,
            open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || !file_.valid()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return complete(status.failed() ? status.error : EIO);
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
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_at(*this, &read_, sizeof(read_), 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_) || read_ != value_) {
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
        byte_read_->store(read_, std::memory_order_release);
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Register};
    const char* path_{nullptr};
    af::IoFixedFile<IoTestThread> file_{};
    char value_{'D'};
    char read_{0};
    bool registered_{false};
    af::IoOpState open_{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

