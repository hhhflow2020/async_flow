#pragma once

class ZeroCopyBoundaryTask final : public IoTaskBase {
public:
    explicit ZeroCopyBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThreads::IO_0);
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
        iovec valid_iov{const_cast<char *>(&value), 1};

        const af::IoStatus sendfile_zero_status =
            af::io_sendfile_some(*this, IoTestThreads::IO_0, -1, -1, nullptr, 0, sendfile_zero);
        const af::IoStatus sendfile_bad_status =
            af::io_sendfile_some(*this, IoTestThreads::IO_0, -1, -1, &offset, 1, sendfile_bad);
        const af::IoStatus send_zc_zero_status =
            af::io_send_zc_some(*this, IoTestThreads::IO_0, -1, nullptr, 0, send_zc_zero);
        const af::IoStatus send_zc_null_status =
            af::io_send_zc_some(*this, IoTestThreads::IO_0, -1, nullptr, 1, send_zc_null);
        const af::IoStatus send_zc_bad_status =
            af::io_send_zc_some(*this, IoTestThreads::IO_0, -1, &value, sizeof(value), send_zc_bad);
        const af::IoStatus sendv_zc_zero_status =
            af::io_sendv_zc_some(*this, IoTestThreads::IO_0, -1, nullptr, 0, sendv_zc_zero);
        const af::IoStatus sendv_zc_null_status =
            af::io_sendv_zc_some(*this, IoTestThreads::IO_0, -1, nullptr, 1, sendv_zc_null);
        const af::IoStatus sendv_zc_bad_status =
            af::io_sendv_zc_some(*this, IoTestThreads::IO_0, -1, &valid_iov, 1, sendv_zc_bad);
        const af::IoStatus send_zc_to_zero_status = af::io_send_zc_to_some(
            *this, IoTestThreads::IO_0, -1, nullptr, 0, nullptr, 0, send_zc_to_zero);
        const af::IoStatus send_zc_to_null_status = af::io_send_zc_to_some(
            *this, IoTestThreads::IO_0, -1, nullptr, 1, nullptr, 0, send_zc_to_null);
        const af::IoStatus send_zc_to_bad_status = af::io_send_zc_to_some(
            *this, IoTestThreads::IO_0, -1, &value, sizeof(value), nullptr, 0, send_zc_to_bad);
        const af::IoStatus sendv_zc_to_zero_status = af::io_sendv_zc_to_some(
            *this, IoTestThreads::IO_0, -1, nullptr, 0, nullptr, 0, sendv_zc_to_zero);
        const af::IoStatus sendv_zc_to_bad_status = af::io_sendv_zc_to_some(
            *this, IoTestThreads::IO_0, -1, &valid_iov, 1, nullptr, 0, sendv_zc_to_bad);
        const af::IoStatus splice_zero_status = af::io_splice_some(
            *this, IoTestThreads::IO_0, -1, nullptr, -1, nullptr, 0, 0, splice_zero);
        const af::IoStatus splice_bad_status = af::io_splice_some(
            *this, IoTestThreads::IO_0, -1, nullptr, -1, nullptr, 1, 0, splice_bad);
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

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *error_{nullptr};
};
