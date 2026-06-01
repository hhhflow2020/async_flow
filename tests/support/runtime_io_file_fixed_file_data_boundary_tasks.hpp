#pragma once

class FixedFileDataBoundaryTask final : public IoTaskBase {
public:
    explicit FixedFileDataBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *unavailable_error,
               std::atomic<int> *invalid_error, std::atomic<int> *null_error) {
        completed_ = completed;
        unavailable_error_ = unavailable_error;
        invalid_error_ = invalid_error;
        null_error_ = null_error;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoFixedFile<IoTestThread> missing(IoTestThreads::IO_0, 0);
        af::IoFixedFile<IoTestThread> invalid(IoTestThreads::IO_0, -1);
        af::IoOpState recv_unavailable{};
        af::IoOpState recv_zero{};
        af::IoOpState recv_bad{};
        af::IoOpState recv_null{};
        af::IoOpState send_unavailable{};
        af::IoOpState send_zero{};
        af::IoOpState send_bad{};
        af::IoOpState send_null{};
        af::IoOpState readv_unavailable{};
        af::IoOpState readv_zero{};
        af::IoOpState readv_bad{};
        af::IoOpState readv_null{};
        af::IoOpState writev_unavailable{};
        af::IoOpState writev_zero{};
        af::IoOpState writev_bad{};
        af::IoOpState writev_null{};
        af::IoOpState recvv_unavailable{};
        af::IoOpState recvv_zero{};
        af::IoOpState recvv_bad{};
        af::IoOpState recvv_null{};
        af::IoOpState sendv_unavailable{};
        af::IoOpState sendv_zero{};
        af::IoOpState sendv_bad{};
        af::IoOpState sendv_null{};
        char value = 0;

        const af::IoStatus recv_unavailable_status =
            missing.recv_some(*this, &value, sizeof(value), recv_unavailable);
        const af::IoStatus recv_zero_status = invalid.recv_some(*this, nullptr, 0, recv_zero);
        const af::IoStatus recv_bad_status =
            invalid.recv_some(*this, &value, sizeof(value), recv_bad);
        const af::IoStatus recv_null_status =
            missing.recv_some(*this, nullptr, sizeof(value), recv_null);
        const af::IoStatus send_unavailable_status =
            missing.send_some(*this, &value, sizeof(value), send_unavailable);
        const af::IoStatus send_zero_status = invalid.send_some(*this, nullptr, 0, send_zero);
        const af::IoStatus send_bad_status =
            invalid.send_some(*this, &value, sizeof(value), send_bad);
        const af::IoStatus send_null_status =
            missing.send_some(*this, nullptr, sizeof(value), send_null);

        iovec valid_iov{&value, sizeof(value)};
        iovec invalid_iov{nullptr, sizeof(value)};
        const af::IoStatus readv_unavailable_status =
            missing.readv_at(*this, &valid_iov, 1, 0, readv_unavailable);
        const af::IoStatus readv_zero_status = invalid.readv_at(*this, nullptr, 0, 0, readv_zero);
        const af::IoStatus readv_bad_status = invalid.readv_at(*this, &valid_iov, 1, 0, readv_bad);
        const af::IoStatus readv_null_status =
            missing.readv_at(*this, &invalid_iov, 1, 0, readv_null);
        const af::IoStatus writev_unavailable_status =
            missing.writev_at(*this, &valid_iov, 1, 0, writev_unavailable);
        const af::IoStatus writev_zero_status =
            invalid.writev_at(*this, nullptr, 0, 0, writev_zero);
        const af::IoStatus writev_bad_status =
            invalid.writev_at(*this, &valid_iov, 1, 0, writev_bad);
        const af::IoStatus writev_null_status =
            missing.writev_at(*this, &invalid_iov, 1, 0, writev_null);
        const af::IoStatus recvv_unavailable_status =
            missing.recvv_some(*this, &valid_iov, 1, recvv_unavailable);
        const af::IoStatus recvv_zero_status = invalid.recvv_some(*this, nullptr, 0, recvv_zero);
        const af::IoStatus recvv_bad_status = invalid.recvv_some(*this, &valid_iov, 1, recvv_bad);
        const af::IoStatus recvv_null_status =
            missing.recvv_some(*this, &invalid_iov, 1, recvv_null);
        const af::IoStatus sendv_unavailable_status =
            missing.sendv_some(*this, &valid_iov, 1, sendv_unavailable);
        const af::IoStatus sendv_zero_status = invalid.sendv_some(*this, nullptr, 0, sendv_zero);
        const af::IoStatus sendv_bad_status = invalid.sendv_some(*this, &valid_iov, 1, sendv_bad);
        const af::IoStatus sendv_null_status =
            missing.sendv_some(*this, &invalid_iov, 1, sendv_null);

        if (!recv_unavailable_status.failed() || recv_unavailable_status.error != ENOSYS ||
            !recv_zero_status.ready() || recv_zero_status.bytes != 0U ||
            !recv_bad_status.failed() || recv_bad_status.error != EBADF ||
            !recv_null_status.failed() || recv_null_status.error != EINVAL ||
            !send_unavailable_status.failed() || send_unavailable_status.error != ENOSYS ||
            !send_zero_status.ready() || send_zero_status.bytes != 0U ||
            !send_bad_status.failed() || send_bad_status.error != EBADF ||
            !send_null_status.failed() || send_null_status.error != EINVAL ||
            !readv_unavailable_status.failed() || readv_unavailable_status.error != ENOSYS ||
            !readv_zero_status.ready() || readv_zero_status.bytes != 0U ||
            !readv_bad_status.failed() || readv_bad_status.error != EBADF ||
            !readv_null_status.failed() || readv_null_status.error != EINVAL ||
            !writev_unavailable_status.failed() || writev_unavailable_status.error != ENOSYS ||
            !writev_zero_status.ready() || writev_zero_status.bytes != 0U ||
            !writev_bad_status.failed() || writev_bad_status.error != EBADF ||
            !writev_null_status.failed() || writev_null_status.error != EINVAL ||
            !recvv_unavailable_status.failed() || recvv_unavailable_status.error != ENOSYS ||
            !recvv_zero_status.ready() || recvv_zero_status.bytes != 0U ||
            !recvv_bad_status.failed() || recvv_bad_status.error != EBADF ||
            !recvv_null_status.failed() || recvv_null_status.error != EINVAL ||
            !sendv_unavailable_status.failed() || sendv_unavailable_status.error != ENOSYS ||
            !sendv_zero_status.ready() || sendv_zero_status.bytes != 0U ||
            !sendv_bad_status.failed() || sendv_bad_status.error != EBADF ||
            !sendv_null_status.failed() || sendv_null_status.error != EINVAL) {
            return failed();
        }

        unavailable_error_->store(recv_unavailable_status.error, std::memory_order_release);
        invalid_error_->store(recv_bad_status.error, std::memory_order_release);
        null_error_->store(recv_null_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *unavailable_error_{nullptr};
    std::atomic<int> *invalid_error_{nullptr};
    std::atomic<int> *null_error_{nullptr};
};
