#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_file_tasks_fragment.hpp is a runtime_io_test_support implementation fragment"
#endif

class FileAdapterBoundaryTask final : public IoTaskBase {
public:
    explicit FileAdapterBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoFile<IoTestThread> file(IoTestThread::IO_0, -1);
        af::IoOpState read{};
        af::IoOpState write{};
        char value = 0;

        const af::IoStatus zero_read = file.read_some(*this, nullptr, 0, read);
        const af::IoStatus zero_write = file.write_some(*this, nullptr, 0, write);
        const af::IoStatus bad_read = file.read_some(*this, &value, sizeof(value), read);
        if (!zero_read.ready() || zero_read.bytes != 0U ||
            !zero_write.ready() || zero_write.bytes != 0U ||
            !bad_read.failed()) {
            return failed();
        }

        error_->store(bad_read.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class FixedBufferBoundaryTask final : public IoTaskBase {
public:
    explicit FixedBufferBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoFile<IoTestThread> file(IoTestThread::IO_0, -1);
        af::IoOpState zero{};
        af::IoOpState bad{};
        char value = 0;
        iovec buffer{&value, sizeof(value)};

        int register_error = 0;
        const bool registered =
            IoRuntime::io_register_buffers(IoTestThread::IO_0, &buffer, 1, &register_error);
        if (registered || register_error != ENOSYS) {
            return failed();
        }

        const af::IoStatus zero_read =
            file.read_fixed_at(*this, nullptr, 0, 0, 0, zero);
        const af::IoStatus bad_read =
            file.read_fixed_at(*this, &value, sizeof(value), 0, 0, bad);
        if (!zero_read.ready() || zero_read.bytes != 0U || !bad_read.failed()) {
            return failed();
        }

        error_->store(bad_read.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class FixedFileBoundaryTask final : public IoTaskBase {
public:
    explicit FixedFileBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        std::atomic<int>* completed,
        std::atomic<int>* register_error,
        std::atomic<int>* unavailable_error,
        std::atomic<int>* invalid_error,
        std::atomic<int>* null_error) {
        completed_ = completed;
        register_error_ = register_error;
        unavailable_error_ = unavailable_error;
        invalid_error_ = invalid_error;
        null_error_ = null_error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const int fd = -1;
        int register_error = 0;
        const bool registered =
            IoRuntime::io_register_files(IoTestThread::IO_0, &fd, 1, &register_error);
        if (registered || register_error != ENOSYS) {
            return failed();
        }
        int update_error = 0;
        const bool updated =
            IoRuntime::io_update_registered_files(IoTestThread::IO_0, 0, &fd, 1, &update_error);
        if (updated || update_error != ENOSYS) {
            return failed();
        }
        int null_update_error = 0;
        const bool null_update = IoRuntime::io_update_registered_files(
            IoTestThread::IO_0,
            0,
            nullptr,
            1,
            &null_update_error);
        if (null_update || null_update_error != EINVAL) {
            return failed();
        }

        af::IoFixedFile<IoTestThread> missing(IoTestThread::IO_0, 0);
        af::IoFixedFile<IoTestThread> invalid(IoTestThread::IO_0, -1);
        af::IoOpState unavailable{};
        af::IoOpState zero{};
        af::IoOpState bad{};
        af::IoOpState null_data{};
        af::IoOpState fixed_unavailable{};
        af::IoOpState fixed_bad{};
        af::IoOpState fixed_null{};
        af::IoOpState direct_null_path{};
        af::IoOpState direct_null_output{};
        af::IoOpState direct_bad_index{};
        af::IoOpState direct_unavailable{};
        af::IoOpState accept_bad_fd{};
        af::IoOpState accept_null_output{};
        af::IoOpState accept_bad_address{};
        af::IoOpState accept_bad_index{};
        af::IoOpState accept_unavailable{};
        af::IoOpState fixed_recv_unavailable{};
        af::IoOpState fixed_recv_zero{};
        af::IoOpState fixed_recv_bad{};
        af::IoOpState fixed_recv_null{};
        af::IoOpState fixed_send_unavailable{};
        af::IoOpState fixed_send_zero{};
        af::IoOpState fixed_send_bad{};
        af::IoOpState fixed_send_null{};
        af::IoOpState fixed_readv_unavailable{};
        af::IoOpState fixed_readv_zero{};
        af::IoOpState fixed_readv_bad{};
        af::IoOpState fixed_readv_null{};
        af::IoOpState fixed_writev_unavailable{};
        af::IoOpState fixed_writev_zero{};
        af::IoOpState fixed_writev_bad{};
        af::IoOpState fixed_writev_null{};
        af::IoOpState fixed_recvv_unavailable{};
        af::IoOpState fixed_recvv_zero{};
        af::IoOpState fixed_recvv_bad{};
        af::IoOpState fixed_recvv_null{};
        af::IoOpState fixed_sendv_unavailable{};
        af::IoOpState fixed_sendv_zero{};
        af::IoOpState fixed_sendv_bad{};
        af::IoOpState fixed_sendv_null{};
        char value = 0;
        af::IoFixedBuffer buffer{&value, sizeof(value), 0};

        const af::IoStatus unavailable_read =
            missing.read_at(*this, &value, sizeof(value), 0, unavailable);
        const af::IoStatus zero_read = invalid.read_at(*this, nullptr, 0, 0, zero);
        const af::IoStatus bad_read = invalid.read_at(*this, &value, sizeof(value), 0, bad);
        const af::IoStatus null_read =
            missing.read_at(*this, nullptr, sizeof(value), 0, null_data);
        const af::IoStatus fixed_unavailable_read =
            missing.read_fixed_at(*this, buffer, 0, fixed_unavailable);
        const af::IoStatus fixed_bad_read =
            invalid.read_fixed_at(*this, buffer, 0, fixed_bad);
        const af::IoStatus fixed_null_read = missing.read_fixed_at(
            *this,
            af::IoFixedBuffer{nullptr, sizeof(value), 0},
            0,
            fixed_null);
        af::IoFixedFile<IoTestThread> direct_file{};
        const af::IoStatus direct_null_path_status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            O_RDONLY | O_CLOEXEC,
            0,
            0,
            &direct_file,
            direct_null_path);
        const af::IoStatus direct_null_output_status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-direct-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            0,
            nullptr,
            direct_null_output);
        const af::IoStatus direct_bad_index_status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-direct-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            -1,
            &direct_file,
            direct_bad_index);
        const af::IoStatus direct_unavailable_status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-direct-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            0,
            &direct_file,
            direct_unavailable);
        af::IoFixedFile<IoTestThread> accepted_direct{};
        sockaddr_storage peer{};
        socklen_t peer_size = sizeof(peer);
        const int placeholder_fd = STDIN_FILENO;
        const af::IoStatus accept_bad_fd_status = af::io_accept_direct(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &accepted_direct,
            accept_bad_fd);
        const af::IoStatus accept_null_output_status = af::io_accept_direct(
            *this,
            IoTestThread::IO_0,
            placeholder_fd,
            nullptr,
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            nullptr,
            accept_null_output);
        const af::IoStatus accept_bad_address_status = af::io_accept_direct(
            *this,
            IoTestThread::IO_0,
            placeholder_fd,
            reinterpret_cast<sockaddr*>(&peer),
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &accepted_direct,
            accept_bad_address);
        const af::IoStatus accept_bad_index_status = af::io_accept_direct(
            *this,
            IoTestThread::IO_0,
            placeholder_fd,
            nullptr,
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC,
            -1,
            &accepted_direct,
            accept_bad_index);
        const af::IoStatus accept_unavailable_status = af::io_accept_direct(
            *this,
            IoTestThread::IO_0,
            placeholder_fd,
            nullptr,
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &accepted_direct,
            accept_unavailable);
        const af::IoStatus fixed_recv_unavailable_status =
            missing.recv_some(*this, &value, sizeof(value), fixed_recv_unavailable);
        const af::IoStatus fixed_recv_zero_status =
            invalid.recv_some(*this, nullptr, 0, fixed_recv_zero);
        const af::IoStatus fixed_recv_bad_status =
            invalid.recv_some(*this, &value, sizeof(value), fixed_recv_bad);
        const af::IoStatus fixed_recv_null_status =
            missing.recv_some(*this, nullptr, sizeof(value), fixed_recv_null);
        const af::IoStatus fixed_send_unavailable_status =
            missing.send_some(*this, &value, sizeof(value), fixed_send_unavailable);
        const af::IoStatus fixed_send_zero_status =
            invalid.send_some(*this, nullptr, 0, fixed_send_zero);
        const af::IoStatus fixed_send_bad_status =
            invalid.send_some(*this, &value, sizeof(value), fixed_send_bad);
        const af::IoStatus fixed_send_null_status =
            missing.send_some(*this, nullptr, sizeof(value), fixed_send_null);
        iovec valid_iov{&value, sizeof(value)};
        iovec invalid_iov{nullptr, sizeof(value)};
        const af::IoStatus fixed_readv_unavailable_status =
            missing.readv_at(*this, &valid_iov, 1, 0, fixed_readv_unavailable);
        const af::IoStatus fixed_readv_zero_status =
            invalid.readv_at(*this, nullptr, 0, 0, fixed_readv_zero);
        const af::IoStatus fixed_readv_bad_status =
            invalid.readv_at(*this, &valid_iov, 1, 0, fixed_readv_bad);
        const af::IoStatus fixed_readv_null_status =
            missing.readv_at(*this, &invalid_iov, 1, 0, fixed_readv_null);
        const af::IoStatus fixed_writev_unavailable_status =
            missing.writev_at(*this, &valid_iov, 1, 0, fixed_writev_unavailable);
        const af::IoStatus fixed_writev_zero_status =
            invalid.writev_at(*this, nullptr, 0, 0, fixed_writev_zero);
        const af::IoStatus fixed_writev_bad_status =
            invalid.writev_at(*this, &valid_iov, 1, 0, fixed_writev_bad);
        const af::IoStatus fixed_writev_null_status =
            missing.writev_at(*this, &invalid_iov, 1, 0, fixed_writev_null);
        const af::IoStatus fixed_recvv_unavailable_status =
            missing.recvv_some(*this, &valid_iov, 1, fixed_recvv_unavailable);
        const af::IoStatus fixed_recvv_zero_status =
            invalid.recvv_some(*this, nullptr, 0, fixed_recvv_zero);
        const af::IoStatus fixed_recvv_bad_status =
            invalid.recvv_some(*this, &valid_iov, 1, fixed_recvv_bad);
        const af::IoStatus fixed_recvv_null_status =
            missing.recvv_some(*this, &invalid_iov, 1, fixed_recvv_null);
        const af::IoStatus fixed_sendv_unavailable_status =
            missing.sendv_some(*this, &valid_iov, 1, fixed_sendv_unavailable);
        const af::IoStatus fixed_sendv_zero_status =
            invalid.sendv_some(*this, nullptr, 0, fixed_sendv_zero);
        const af::IoStatus fixed_sendv_bad_status =
            invalid.sendv_some(*this, &valid_iov, 1, fixed_sendv_bad);
        const af::IoStatus fixed_sendv_null_status =
            missing.sendv_some(*this, &invalid_iov, 1, fixed_sendv_null);
        if (!unavailable_read.failed() || unavailable_read.error != ENOSYS ||
            !zero_read.ready() || zero_read.bytes != 0U ||
            !bad_read.failed() || bad_read.error != EBADF ||
            !null_read.failed() || null_read.error != EINVAL ||
            !fixed_unavailable_read.failed() || fixed_unavailable_read.error != ENOSYS ||
            !fixed_bad_read.failed() || fixed_bad_read.error != EBADF ||
            !fixed_null_read.failed() || fixed_null_read.error != EINVAL ||
            !direct_null_path_status.failed() || direct_null_path_status.error != EINVAL ||
            !direct_null_output_status.failed() || direct_null_output_status.error != EINVAL ||
            !direct_bad_index_status.failed() || direct_bad_index_status.error != EBADF ||
            !direct_unavailable_status.failed() || direct_unavailable_status.error != ENOSYS ||
            !accept_bad_fd_status.failed() || accept_bad_fd_status.error != EBADF ||
            !accept_null_output_status.failed() || accept_null_output_status.error != EINVAL ||
            !accept_bad_address_status.failed() || accept_bad_address_status.error != EINVAL ||
            !accept_bad_index_status.failed() || accept_bad_index_status.error != EBADF ||
            !accept_unavailable_status.failed() || accept_unavailable_status.error != ENOSYS ||
            !fixed_recv_unavailable_status.failed() || fixed_recv_unavailable_status.error != ENOSYS ||
            !fixed_recv_zero_status.ready() || fixed_recv_zero_status.bytes != 0U ||
            !fixed_recv_bad_status.failed() || fixed_recv_bad_status.error != EBADF ||
            !fixed_recv_null_status.failed() || fixed_recv_null_status.error != EINVAL ||
            !fixed_send_unavailable_status.failed() || fixed_send_unavailable_status.error != ENOSYS ||
            !fixed_send_zero_status.ready() || fixed_send_zero_status.bytes != 0U ||
            !fixed_send_bad_status.failed() || fixed_send_bad_status.error != EBADF ||
            !fixed_send_null_status.failed() || fixed_send_null_status.error != EINVAL ||
            !fixed_readv_unavailable_status.failed() || fixed_readv_unavailable_status.error != ENOSYS ||
            !fixed_readv_zero_status.ready() || fixed_readv_zero_status.bytes != 0U ||
            !fixed_readv_bad_status.failed() || fixed_readv_bad_status.error != EBADF ||
            !fixed_readv_null_status.failed() || fixed_readv_null_status.error != EINVAL ||
            !fixed_writev_unavailable_status.failed() || fixed_writev_unavailable_status.error != ENOSYS ||
            !fixed_writev_zero_status.ready() || fixed_writev_zero_status.bytes != 0U ||
            !fixed_writev_bad_status.failed() || fixed_writev_bad_status.error != EBADF ||
            !fixed_writev_null_status.failed() || fixed_writev_null_status.error != EINVAL ||
            !fixed_recvv_unavailable_status.failed() || fixed_recvv_unavailable_status.error != ENOSYS ||
            !fixed_recvv_zero_status.ready() || fixed_recvv_zero_status.bytes != 0U ||
            !fixed_recvv_bad_status.failed() || fixed_recvv_bad_status.error != EBADF ||
            !fixed_recvv_null_status.failed() || fixed_recvv_null_status.error != EINVAL ||
            !fixed_sendv_unavailable_status.failed() || fixed_sendv_unavailable_status.error != ENOSYS ||
            !fixed_sendv_zero_status.ready() || fixed_sendv_zero_status.bytes != 0U ||
            !fixed_sendv_bad_status.failed() || fixed_sendv_bad_status.error != EBADF ||
            !fixed_sendv_null_status.failed() || fixed_sendv_null_status.error != EINVAL ||
            direct_file.valid() || accepted_direct.valid()) {
            return failed();
        }

        register_error_->store(register_error, std::memory_order_release);
        unavailable_error_->store(unavailable_read.error, std::memory_order_release);
        invalid_error_->store(bad_read.error, std::memory_order_release);
        null_error_->store(null_read.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* register_error_{nullptr};
    std::atomic<int>* unavailable_error_{nullptr};
    std::atomic<int>* invalid_error_{nullptr};
    std::atomic<int>* null_error_{nullptr};
};

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

class UringBatchedFileWriteTask final : public UringIoTaskBase {
public:
    explicit UringBatchedFileWriteTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::uint64_t offset,
        char value,
        std::atomic<int>* completed) {
        file_.reset(IoTestThread::IO_0, fd);
        offset_ = offset;
        value_ = value;
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), offset_, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::IoFile<IoTestThread> file_{};
    std::uint64_t offset_{0};
    char value_{0};
    af::IoOpState write_{};
    std::atomic<int>* completed_{nullptr};
};

class UringOpenAtFileTask final : public UringIoTaskBase {
public:
    explicit UringOpenAtFileTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(
        const char* path,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read) {
        path_ = path;
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Open,
        Write,
        Fsync,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Open:
            return open_file();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();
        }
        return failed();
    }

    af::TaskResult open_file() {
        int fd = -1;
        const af::IoStatus status = af::io_openat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            path_,
            O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC,
            0600U,
            &fd,
            open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return failed();
        }
        owned_.reset(fd);
        file_.reset(IoTestThread::IO_0, owned_.get());
        state_ = State::Write;
        return again();
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
        if (!status.ready() || status.bytes != sizeof(read_) || read_ != value_) {
            return failed();
        }
        byte_read_->store(read_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Open};
    const char* path_{nullptr};
    af::UniqueFd owned_{};
    af::IoFile<IoTestThread> file_{};
    char value_{'O'};
    char read_{0};
    af::IoOpState open_{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringFileLifecycleTask final : public UringIoTaskBase {
public:
    explicit UringFileLifecycleTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        const char* path,
        const char* renamed_path,
        std::atomic<int>* completed,
        std::atomic<int>* close_released,
        std::atomic<std::uint64_t>* observed_size) {
        path_ = path;
        renamed_path_ = renamed_path;
        completed_ = completed;
        close_released_ = close_released;
        observed_size_ = observed_size;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Open,
        Fallocate,
        Write,
        Fsync,
        Read,
        Statx,
        Rename,
        Unlink,
        Close,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Open:
            return open_file();

        case State::Fallocate:
            return fallocate_file();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::Statx:
            return stat_file();

        case State::Rename:
            return rename_file();

        case State::Unlink:
            return unlink_file();

        case State::Close:
            return close_file();
        }
        return failed();
    }

    af::TaskResult open_file() {
        int fd = -1;
        const af::IoStatus status = af::io_openat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            path_,
            O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC,
            0600U,
            &fd,
            open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return failed();
        }
        owned_.reset(fd);
        file_.reset(IoTestThread::IO_0, owned_.get());
        state_ = State::Fallocate;
        return again();
    }

    af::TaskResult fallocate_file() {
        const af::IoStatus status = af::io_fallocate(
            *this,
            IoTestThread::IO_0,
            owned_.get(),
            FALLOC_FL_KEEP_SIZE,
            0,
            4096,
            fallocate_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
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
        if (!status.ready() || status.bytes != sizeof(read_) || read_ != value_) {
            return failed();
        }
        state_ = State::Statx;
        return again();
    }

    af::TaskResult stat_file() {
        const af::IoStatus status = af::io_statx(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            path_,
            0,
            STATX_SIZE,
            &stat_,
            stat_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || stat_.stx_size != sizeof(value_)) {
            return failed();
        }
        observed_size_->store(stat_.stx_size, std::memory_order_release);
        state_ = State::Rename;
        return again();
    }

    af::TaskResult rename_file() {
        const af::IoStatus status = af::io_renameat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            path_,
            AT_FDCWD,
            renamed_path_,
            0,
            rename_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Unlink;
        return again();
    }

    af::TaskResult unlink_file() {
        const af::IoStatus status = af::io_unlinkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            renamed_path_,
            0,
            unlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Close;
        return again();
    }

    af::TaskResult close_file() {
        const af::IoStatus status = af::io_close(*this, IoTestThread::IO_0, owned_, close_);
        if (status.pending()) {
            if (owned_.get() != -1) {
                return failed();
            }
            return pending();
        }
        if (!status.ready() || owned_.get() != -1) {
            return failed();
        }
        close_released_->store(1, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Open};
    const char* path_{nullptr};
    const char* renamed_path_{nullptr};
    af::UniqueFd owned_{};
    af::IoFile<IoTestThread> file_{};
    char value_{'L'};
    char read_{0};
    struct statx stat_{};
    af::IoOpState open_{};
    af::IoOpState fallocate_{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    af::IoOpState stat_state_{};
    af::IoOpState rename_{};
    af::IoOpState unlink_{};
    af::IoOpState close_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* close_released_{nullptr};
    std::atomic<std::uint64_t>* observed_size_{nullptr};
};

class UringFilesystemOpsTask final : public UringIoTaskBase {
public:
    explicit UringFilesystemOpsTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        const char* dir_path,
        const char* file_path,
        const char* hardlink_path,
        const char* symlink_path,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<std::uint64_t>* observed_size) {
        dir_path_ = dir_path;
        file_path_ = file_path;
        hardlink_path_ = hardlink_path;
        symlink_path_ = symlink_path;
        completed_ = completed;
        error_ = error;
        observed_size_ = observed_size;
        how_.flags = O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC;
        how_.mode = 0600U;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Mkdir,
        OpenAt2,
        Write,
        Ftruncate,
        Fsync,
        Statx,
        Close,
        Link,
        Symlink,
        UnlinkFile,
        UnlinkHardlink,
        UnlinkSymlink,
        Rmdir,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Mkdir:
            return mkdir_dir();

        case State::OpenAt2:
            return open_file();

        case State::Write:
            return write_payload();

        case State::Ftruncate:
            return truncate_file();

        case State::Fsync:
            return fsync_file();

        case State::Statx:
            return stat_file();

        case State::Close:
            return close_file();

        case State::Link:
            return link_file();

        case State::Symlink:
            return symlink_file();

        case State::UnlinkFile:
            return unlink_file(file_path_, State::UnlinkHardlink, 0);

        case State::UnlinkHardlink:
            return unlink_file(hardlink_path_, State::UnlinkSymlink, 0);

        case State::UnlinkSymlink:
            return unlink_file(symlink_path_, State::Rmdir, 0);

        case State::Rmdir:
            return unlink_file(dir_path_, State::Rmdir, AT_REMOVEDIR, true);
        }
        return complete(EIO);
    }

    af::TaskResult mkdir_dir() {
        const af::IoStatus status = af::io_mkdirat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            dir_path_,
            0700U,
            mkdir_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::OpenAt2;
        return again();
    }

    af::TaskResult open_file() {
        int fd = -1;
        const af::IoStatus status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            file_path_,
            &how_,
            &fd,
            open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return complete(status.failed() ? status.error : EIO);
        }
        owned_.reset(fd);
        file_.reset(IoTestThread::IO_0, owned_.get());
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_payload() {
        const af::IoStatus status = file_.write_at(*this, payload_, sizeof(payload_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(payload_)) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Ftruncate;
        return again();
    }

    af::TaskResult truncate_file() {
        const af::IoStatus status =
            af::io_ftruncate(*this, IoTestThread::IO_0, owned_.get(), 1, truncate_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_file() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Statx;
        return again();
    }

    af::TaskResult stat_file() {
        const af::IoStatus status = af::io_statx(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            file_path_,
            0,
            STATX_SIZE,
            &stat_,
            stat_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || stat_.stx_size != 1U) {
            return complete(status.failed() ? status.error : EIO);
        }
        observed_size_->store(stat_.stx_size, std::memory_order_release);
        state_ = State::Close;
        return again();
    }

    af::TaskResult close_file() {
        const af::IoStatus status = af::io_close(*this, IoTestThread::IO_0, owned_, close_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || owned_.get() != -1) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Link;
        return again();
    }

    af::TaskResult link_file() {
        const af::IoStatus status = af::io_linkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            file_path_,
            AT_FDCWD,
            hardlink_path_,
            0,
            link_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Symlink;
        return again();
    }

    af::TaskResult symlink_file() {
        const af::IoStatus status = af::io_symlinkat(
            *this,
            IoTestThread::IO_0,
            file_path_,
            AT_FDCWD,
            symlink_path_,
            symlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::UnlinkFile;
        return again();
    }

    af::TaskResult unlink_file(
        const char* path,
        State next_state,
        int flags,
        bool final_state = false) {
        const af::IoStatus status = af::io_unlinkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            path,
            flags,
            unlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        if (final_state) {
            return complete(0);
        }
        state_ = next_state;
        unlink_.reset();
        return again();
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Mkdir};
    const char* dir_path_{nullptr};
    const char* file_path_{nullptr};
    const char* hardlink_path_{nullptr};
    const char* symlink_path_{nullptr};
    struct open_how how_{};
    af::UniqueFd owned_{};
    af::IoFile<IoTestThread> file_{};
    char payload_[2]{'F', 'S'};
    struct statx stat_{};
    af::IoOpState mkdir_{};
    af::IoOpState open_{};
    af::IoOpState write_{};
    af::IoOpState truncate_{};
    af::IoOpState fsync_{};
    af::IoOpState stat_state_{};
    af::IoOpState close_{};
    af::IoOpState link_{};
    af::IoOpState symlink_{};
    af::IoOpState unlink_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<std::uint64_t>* observed_size_{nullptr};
};

