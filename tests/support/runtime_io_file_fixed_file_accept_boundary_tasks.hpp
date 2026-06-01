#pragma once

class FixedFileAcceptDirectBoundaryTask final : public IoTaskBase {
public:
    explicit FixedFileAcceptDirectBoundaryTask(IoTaskBase::FactoryToken token)
        : IoTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *bad_fd_error,
               std::atomic<int> *null_output_error, std::atomic<int> *bad_address_error,
               std::atomic<int> *bad_index_error, std::atomic<int> *unavailable_error) {
        completed_ = completed;
        bad_fd_error_ = bad_fd_error;
        null_output_error_ = null_output_error;
        bad_address_error_ = bad_address_error;
        bad_index_error_ = bad_index_error;
        unavailable_error_ = unavailable_error;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoFixedFile<IoTestThread> accepted_direct{};
        sockaddr_storage peer{};
        const int placeholder_fd = STDIN_FILENO;
        af::IoOpState bad_fd{};
        af::IoOpState null_output{};
        af::IoOpState bad_address{};
        af::IoOpState bad_index{};
        af::IoOpState unavailable{};

        const af::IoStatus bad_fd_status =
            af::io_accept_direct(*this, IoTestThreads::IO_0, -1, nullptr, nullptr,
                                 SOCK_NONBLOCK | SOCK_CLOEXEC, 0, &accepted_direct, bad_fd);
        const af::IoStatus null_output_status =
            af::io_accept_direct(*this, IoTestThreads::IO_0, placeholder_fd, nullptr, nullptr,
                                 SOCK_NONBLOCK | SOCK_CLOEXEC, 0, nullptr, null_output);
        const af::IoStatus bad_address_status = af::io_accept_direct(
            *this, IoTestThreads::IO_0, placeholder_fd, reinterpret_cast<sockaddr *>(&peer),
            nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC, 0, &accepted_direct, bad_address);
        const af::IoStatus bad_index_status =
            af::io_accept_direct(*this, IoTestThreads::IO_0, placeholder_fd, nullptr, nullptr,
                                 SOCK_NONBLOCK | SOCK_CLOEXEC, -1, &accepted_direct, bad_index);
        const af::IoStatus unavailable_status =
            af::io_accept_direct(*this, IoTestThreads::IO_0, placeholder_fd, nullptr, nullptr,
                                 SOCK_NONBLOCK | SOCK_CLOEXEC, 0, &accepted_direct, unavailable);

        if (!bad_fd_status.failed() || bad_fd_status.error != EBADF ||
            !null_output_status.failed() || null_output_status.error != EINVAL ||
            !bad_address_status.failed() || bad_address_status.error != EINVAL ||
            !bad_index_status.failed() || bad_index_status.error != EBADF ||
            !unavailable_status.failed() || unavailable_status.error != ENOSYS ||
            accepted_direct.valid()) {
            return failed();
        }

        bad_fd_error_->store(bad_fd_status.error, std::memory_order_release);
        null_output_error_->store(null_output_status.error, std::memory_order_release);
        bad_address_error_->store(bad_address_status.error, std::memory_order_release);
        bad_index_error_->store(bad_index_status.error, std::memory_order_release);
        unavailable_error_->store(unavailable_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *bad_fd_error_{nullptr};
    std::atomic<int> *null_output_error_{nullptr};
    std::atomic<int> *bad_address_error_{nullptr};
    std::atomic<int> *bad_index_error_{nullptr};
    std::atomic<int> *unavailable_error_{nullptr};
};
