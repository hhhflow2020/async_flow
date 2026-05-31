#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_file_boundary_tasks_fragment.hpp is a runtime_io_file_tasks implementation fragment"
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

