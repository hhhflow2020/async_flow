#pragma once

class OpenAtBoundaryTask final : public IoTaskBase {
public:
    explicit OpenAtBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState null_path{};
        af::IoOpState null_output{};
        af::IoOpState no_uring{};
        af::IoOpState close_no_uring{};
        int opened = -1;
        const af::IoStatus null_path_status =
            af::io_openat(*this, IoTestThread::IO_0, AT_FDCWD, nullptr, O_RDONLY | O_CLOEXEC, 0,
                          &opened, null_path);
        const af::IoStatus null_output_status =
            af::io_openat(*this, IoTestThread::IO_0, AT_FDCWD, "/tmp/asyncflow-openat-boundary",
                          O_RDONLY | O_CLOEXEC, 0, nullptr, null_output);
        const af::IoStatus no_uring_status =
            af::io_openat(*this, IoTestThread::IO_0, AT_FDCWD, "/tmp/asyncflow-openat-boundary",
                          O_RDONLY | O_CLOEXEC, 0, &opened, no_uring);
        af::UniqueFd event = af::make_eventfd();
        if (!event) {
            return failed();
        }
        const af::IoStatus close_no_uring_status =
            af::io_close(*this, IoTestThread::IO_0, event, close_no_uring);
        if (!null_path_status.failed() || null_path_status.error != EINVAL ||
            !null_output_status.failed() || null_output_status.error != EINVAL ||
            !no_uring_status.failed() || no_uring_status.error != ENOSYS ||
            !close_no_uring_status.failed() || close_no_uring_status.error != ENOSYS ||
            event.get() < 0 || opened != -1) {
            return failed();
        }
        error_->store(no_uring_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *error_{nullptr};
};
