#pragma once

class FilesystemMetadataBoundaryTask final : public IoTaskBase {
public:
    explicit FilesystemMetadataBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        af::UniqueFd event = af::make_eventfd();
        if (!event) {
            return failed();
        }

        af::IoOpState stat_no_uring{};
        af::IoOpState fallocate_no_uring{};
        af::IoOpState ftruncate_no_uring{};
        af::IoOpState stat_null_path{};
        af::IoOpState stat_null_output{};
        af::IoOpState fallocate_bad_fd{};
        af::IoOpState ftruncate_bad_fd{};
        struct statx stat{};

        const af::IoStatus stat_no_uring_status =
            af::io_statx(*this, IoTestThreads::IO_0, AT_FDCWD, "/tmp/asyncflow-openat-boundary", 0,
                         STATX_SIZE, &stat, stat_no_uring);
        const af::IoStatus fallocate_no_uring_status =
            af::io_fallocate(*this, IoTestThreads::IO_0, event.get(), FALLOC_FL_KEEP_SIZE, 0, 4096,
                             fallocate_no_uring);
        const af::IoStatus ftruncate_no_uring_status =
            af::io_ftruncate(*this, IoTestThreads::IO_0, event.get(), 0, ftruncate_no_uring);
        const af::IoStatus stat_null_path_status = af::io_statx(
            *this, IoTestThreads::IO_0, AT_FDCWD, nullptr, 0, STATX_SIZE, &stat, stat_null_path);
        const af::IoStatus stat_null_output_status =
            af::io_statx(*this, IoTestThreads::IO_0, AT_FDCWD, "/tmp/asyncflow-openat-boundary", 0,
                         STATX_SIZE, nullptr, stat_null_output);
        const af::IoStatus fallocate_bad_fd_status = af::io_fallocate(
            *this, IoTestThreads::IO_0, -1, FALLOC_FL_KEEP_SIZE, 0, 4096, fallocate_bad_fd);
        const af::IoStatus ftruncate_bad_fd_status =
            af::io_ftruncate(*this, IoTestThreads::IO_0, -1, 0, ftruncate_bad_fd);

        if (!stat_no_uring_status.failed() || stat_no_uring_status.error != ENOSYS ||
            !fallocate_no_uring_status.failed() || fallocate_no_uring_status.error != ENOSYS ||
            !ftruncate_no_uring_status.failed() || ftruncate_no_uring_status.error != ENOSYS ||
            !stat_null_path_status.failed() || stat_null_path_status.error != EINVAL ||
            !stat_null_output_status.failed() || stat_null_output_status.error != EINVAL ||
            !fallocate_bad_fd_status.failed() || fallocate_bad_fd_status.error != EBADF ||
            !ftruncate_bad_fd_status.failed() || ftruncate_bad_fd_status.error != EBADF) {
            return failed();
        }
        error_->store(stat_no_uring_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *error_{nullptr};
};
