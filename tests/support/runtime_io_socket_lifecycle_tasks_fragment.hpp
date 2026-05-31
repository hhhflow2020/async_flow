#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_socket_lifecycle_tasks_fragment.hpp is a runtime_io_test_support implementation fragment"
#endif

class SocketLifecycleSetupTask final : public IoTaskBase {
public:
    explicit SocketLifecycleSetupTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<int>* reuse_value,
        std::atomic<int>* local_port,
        std::atomic<std::uint16_t>* ran_on) {
        completed_ = completed;
        error_ = error;
        reuse_value_ = reuse_value;
        local_port_ = local_port;
        ran_on_ = ran_on;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        CreateSocket,
        FinishSocket,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::CreateSocket:
            return create_socket();
        case State::FinishSocket:
            return finish_socket();
        }
        return failed();
    }

    af::TaskResult create_socket() {
        state_ = State::FinishSocket;
        return consume_socket_status(af::io_socket(
            *this,
            IoTestThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &opened_fd_,
            socket_));
    }

    af::TaskResult finish_socket() {
        return consume_socket_status(af::io_socket(
            *this,
            IoTestThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &opened_fd_,
            socket_));
    }

    af::TaskResult consume_socket_status(const af::IoStatus status) {
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || opened_fd_ < 0) {
            return complete(status.failed() ? status.error : EIO);
        }
        owned_.reset(opened_fd_);
        opened_fd_ = -1;
        listener_.reset(IoTestThread::IO_0, owned_.get());
        return configure_listener();
    }

    af::TaskResult configure_listener() {
        ran_on_->store(IoRuntime::current_thread_index(), std::memory_order_release);

        const int one = 1;
        const af::IoStatus set_status = listener_.setsockopt(
            *this,
            SOL_SOCKET,
            SO_REUSEADDR,
            &one,
            sizeof(one));
        if (!set_status.ready()) {
            return complete(set_status.error);
        }

        int reuse = 0;
        socklen_t reuse_size = sizeof(reuse);
        const af::IoStatus get_status = listener_.getsockopt(
            *this,
            SOL_SOCKET,
            SO_REUSEADDR,
            &reuse,
            &reuse_size);
        if (!get_status.ready()) {
            return complete(get_status.error);
        }
        reuse_value_->store(reuse, std::memory_order_release);

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        const af::IoStatus bind_status = listener_.bind(
            *this,
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address));
        if (!bind_status.ready()) {
            return complete(bind_status.error);
        }

        const af::IoStatus listen_status = listener_.listen(*this, 16);
        if (!listen_status.ready()) {
            return complete(listen_status.error);
        }

        sockaddr_in local{};
        socklen_t local_size = sizeof(local);
        const af::IoStatus name_status = listener_.getsockname(
            *this,
            reinterpret_cast<sockaddr*>(&local),
            &local_size);
        if (!name_status.ready()) {
            return complete(name_status.error);
        }
        if (local.sin_family != AF_INET || local.sin_port == 0 || local_size == 0U) {
            return complete(EIO);
        }
        local_port_->store(static_cast<int>(ntohs(local.sin_port)), std::memory_order_release);
        return complete(0);
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::CreateSocket};
    af::IoOpState socket_{};
    int opened_fd_{-1};
    af::UniqueFd owned_{};
    af::TcpListener<IoTestThread> listener_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<int>* reuse_value_{nullptr};
    std::atomic<int>* local_port_{nullptr};
    std::atomic<std::uint16_t>* ran_on_{nullptr};
};

class SocketLifecycleBoundaryTask final : public IoTaskBase {
public:
    explicit SocketLifecycleBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState null_socket{};
        int one = 1;
        int opened = -1;
        int value = 0;
        socklen_t value_size = sizeof(value);
        sockaddr_storage name{};
        socklen_t name_size = sizeof(name);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;

        const af::IoStatus null_socket_status = af::io_socket(
            *this,
            IoTestThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            nullptr,
            null_socket);
        const af::IoStatus bad_setsockopt_status = af::io_setsockopt(
            *this,
            IoTestThread::IO_0,
            -1,
            SOL_SOCKET,
            SO_REUSEADDR,
            &one,
            sizeof(one));
        const af::IoStatus null_setsockopt_status = af::io_setsockopt(
            *this,
            IoTestThread::IO_0,
            0,
            SOL_SOCKET,
            SO_REUSEADDR,
            nullptr,
            sizeof(one));
        const af::IoStatus bad_getsockopt_status = af::io_getsockopt(
            *this,
            IoTestThread::IO_0,
            -1,
            SOL_SOCKET,
            SO_REUSEADDR,
            &value,
            &value_size);
        const af::IoStatus null_getsockopt_status = af::io_getsockopt(
            *this,
            IoTestThread::IO_0,
            0,
            SOL_SOCKET,
            SO_REUSEADDR,
            nullptr,
            &value_size);
        const af::IoStatus bad_getsockname_status = af::io_getsockname(
            *this,
            IoTestThread::IO_0,
            -1,
            reinterpret_cast<sockaddr*>(&name),
            &name_size);
        const af::IoStatus null_getsockname_status = af::io_getsockname(
            *this,
            IoTestThread::IO_0,
            0,
            nullptr,
            &name_size);
        const af::IoStatus bad_getpeername_status = af::io_getpeername(
            *this,
            IoTestThread::IO_0,
            -1,
            reinterpret_cast<sockaddr*>(&name),
            &name_size);
        const af::IoStatus null_getpeername_status = af::io_getpeername(
            *this,
            IoTestThread::IO_0,
            0,
            reinterpret_cast<sockaddr*>(&name),
            nullptr);
        const af::IoStatus bad_bind_status = af::io_bind(
            *this,
            IoTestThread::IO_0,
            -1,
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address));
        const af::IoStatus null_bind_status = af::io_bind(
            *this,
            IoTestThread::IO_0,
            0,
            nullptr,
            sizeof(address));
        const af::IoStatus bad_listen_status =
            af::io_listen(*this, IoTestThread::IO_0, -1, 16);

        af::UniqueFd temp(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        if (!temp) {
            return complete(EIO);
        }
        const af::IoStatus wrong_thread_status =
            af::io_listen(*this, IoTestThread::Logic_0, temp.get(), 16);
        const af::IoStatus wrong_name_thread_status = af::io_getsockname(
            *this,
            IoTestThread::Logic_0,
            temp.get(),
            reinterpret_cast<sockaddr*>(&name),
            &name_size);

        const bool ok =
            null_socket_status.failed() && null_socket_status.error == EINVAL &&
            bad_setsockopt_status.failed() && bad_setsockopt_status.error == EBADF &&
            null_setsockopt_status.failed() && null_setsockopt_status.error == EINVAL &&
            bad_getsockopt_status.failed() && bad_getsockopt_status.error == EBADF &&
            null_getsockopt_status.failed() && null_getsockopt_status.error == EINVAL &&
            bad_getsockname_status.failed() && bad_getsockname_status.error == EBADF &&
            null_getsockname_status.failed() && null_getsockname_status.error == EINVAL &&
            bad_getpeername_status.failed() && bad_getpeername_status.error == EBADF &&
            null_getpeername_status.failed() && null_getpeername_status.error == EINVAL &&
            bad_bind_status.failed() && bad_bind_status.error == EBADF &&
            null_bind_status.failed() && null_bind_status.error == EINVAL &&
            bad_listen_status.failed() && bad_listen_status.error == EBADF &&
            wrong_thread_status.failed() && wrong_thread_status.error == EINVAL &&
            wrong_name_thread_status.failed() && wrong_name_thread_status.error == EINVAL &&
            opened == -1;
        return complete(ok ? 0 : EIO);
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class UringSocketCreateTask final : public UringIoTaskBase {
public:
    explicit UringSocketCreateTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        CreateSocket,
        FinishSocket,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::CreateSocket:
            return create_socket();
        case State::FinishSocket:
            return finish_socket();
        }
        return failed();
    }

    af::TaskResult create_socket() {
        state_ = State::FinishSocket;
        return consume_socket_status(af::io_socket(
            *this,
            IoTestThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &opened_fd_,
            socket_));
    }

    af::TaskResult finish_socket() {
        return consume_socket_status(af::io_socket(
            *this,
            IoTestThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &opened_fd_,
            socket_));
    }

    af::TaskResult consume_socket_status(const af::IoStatus status) {
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || opened_fd_ < 0) {
            return complete(status.failed() ? status.error : EIO);
        }
        af::UniqueFd fd(opened_fd_);
        opened_fd_ = -1;
        return complete(0);
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::CreateSocket};
    af::IoOpState socket_{};
    int opened_fd_{-1};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class PendingSocketWaitTask final : public FastIoTaskBase {
public:
    explicit PendingSocketWaitTask(FastIoTaskBase::FactoryToken token) : FastIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* armed) {
        fd_ = fd;
        armed_ = armed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        if (!wait_io(IoTestThread::IO_0, fd_, af::io_readable, &result_)) {
            return failed();
        }
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    int fd_{-1};
    af::IoResult result_{};
    std::atomic<int>* armed_{nullptr};
};

class FastIoDoneTask final : public FastIoTaskBase {
public:
    explicit FastIoDoneTask(FastIoTaskBase::FactoryToken token) : FastIoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
};

