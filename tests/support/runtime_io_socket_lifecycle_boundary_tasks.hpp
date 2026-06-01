#pragma once

class SocketLifecycleBoundaryTask final : public IoTaskBase {
public:
    explicit SocketLifecycleBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *error) {
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

        const af::IoStatus null_socket_status =
            af::io_socket(*this, IoTestThread::IO_0, AF_INET,
                          SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, nullptr, null_socket);
        const af::IoStatus bad_setsockopt_status = af::io_setsockopt(
            *this, IoTestThread::IO_0, -1, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        const af::IoStatus null_setsockopt_status = af::io_setsockopt(
            *this, IoTestThread::IO_0, 0, SOL_SOCKET, SO_REUSEADDR, nullptr, sizeof(one));
        const af::IoStatus bad_getsockopt_status = af::io_getsockopt(
            *this, IoTestThread::IO_0, -1, SOL_SOCKET, SO_REUSEADDR, &value, &value_size);
        const af::IoStatus null_getsockopt_status = af::io_getsockopt(
            *this, IoTestThread::IO_0, 0, SOL_SOCKET, SO_REUSEADDR, nullptr, &value_size);
        const af::IoStatus bad_getsockname_status = af::io_getsockname(
            *this, IoTestThread::IO_0, -1, reinterpret_cast<sockaddr *>(&name), &name_size);
        const af::IoStatus null_getsockname_status =
            af::io_getsockname(*this, IoTestThread::IO_0, 0, nullptr, &name_size);
        const af::IoStatus bad_getpeername_status = af::io_getpeername(
            *this, IoTestThread::IO_0, -1, reinterpret_cast<sockaddr *>(&name), &name_size);
        const af::IoStatus null_getpeername_status = af::io_getpeername(
            *this, IoTestThread::IO_0, 0, reinterpret_cast<sockaddr *>(&name), nullptr);
        const af::IoStatus bad_bind_status =
            af::io_bind(*this, IoTestThread::IO_0, -1, reinterpret_cast<const sockaddr *>(&address),
                        sizeof(address));
        const af::IoStatus null_bind_status =
            af::io_bind(*this, IoTestThread::IO_0, 0, nullptr, sizeof(address));
        const af::IoStatus bad_listen_status = af::io_listen(*this, IoTestThread::IO_0, -1, 16);

        af::UniqueFd temp(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        if (!temp) {
            return complete(EIO);
        }
        const af::IoStatus wrong_thread_status =
            af::io_listen(*this, IoTestThread::Logic_0, temp.get(), 16);
        const af::IoStatus wrong_name_thread_status =
            af::io_getsockname(*this, IoTestThread::Logic_0, temp.get(),
                               reinterpret_cast<sockaddr *>(&name), &name_size);

        const bool ok = null_socket_status.failed() && null_socket_status.error == EINVAL &&
                        bad_setsockopt_status.failed() && bad_setsockopt_status.error == EBADF &&
                        null_setsockopt_status.failed() && null_setsockopt_status.error == EINVAL &&
                        bad_getsockopt_status.failed() && bad_getsockopt_status.error == EBADF &&
                        null_getsockopt_status.failed() && null_getsockopt_status.error == EINVAL &&
                        bad_getsockname_status.failed() && bad_getsockname_status.error == EBADF &&
                        null_getsockname_status.failed() &&
                        null_getsockname_status.error == EINVAL &&
                        bad_getpeername_status.failed() && bad_getpeername_status.error == EBADF &&
                        null_getpeername_status.failed() &&
                        null_getpeername_status.error == EINVAL && bad_bind_status.failed() &&
                        bad_bind_status.error == EBADF && null_bind_status.failed() &&
                        null_bind_status.error == EINVAL && bad_listen_status.failed() &&
                        bad_listen_status.error == EBADF && wrong_thread_status.failed() &&
                        wrong_thread_status.error == EINVAL && wrong_name_thread_status.failed() &&
                        wrong_name_thread_status.error == EINVAL && opened == -1;
        return complete(ok ? 0 : EIO);
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *error_{nullptr};
};
