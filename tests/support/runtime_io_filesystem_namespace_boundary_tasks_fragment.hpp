#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_filesystem_namespace_boundary_tasks_fragment.hpp is a runtime_io_filesystem_boundary_tasks implementation fragment"
#endif

class FilesystemNamespaceBoundaryTask final : public IoTaskBase {
public:
    explicit FilesystemNamespaceBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState rename_no_uring{};
        af::IoOpState unlink_no_uring{};
        af::IoOpState openat2_no_uring{};
        af::IoOpState mkdir_no_uring{};
        af::IoOpState symlink_no_uring{};
        af::IoOpState link_no_uring{};
        af::IoOpState rename_null_old{};
        af::IoOpState rename_null_new{};
        af::IoOpState unlink_null_path{};
        af::IoOpState openat2_null_path{};
        af::IoOpState openat2_null_how{};
        af::IoOpState openat2_null_output{};
        af::IoOpState mkdir_null_path{};
        af::IoOpState symlink_null_target{};
        af::IoOpState symlink_null_path{};
        af::IoOpState link_null_old{};
        af::IoOpState link_null_new{};
        struct open_how how{};
        how.flags = O_RDONLY | O_CLOEXEC;
        int opened = -1;

        const af::IoStatus rename_no_uring_status = af::io_renameat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary-old",
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary-new",
            0,
            rename_no_uring);
        const af::IoStatus unlink_no_uring_status = af::io_unlinkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary",
            0,
            unlink_no_uring);
        const af::IoStatus openat2_no_uring_status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat2-boundary",
            &how,
            &opened,
            openat2_no_uring);
        const af::IoStatus mkdir_no_uring_status = af::io_mkdirat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-mkdirat-boundary",
            0700U,
            mkdir_no_uring);
        const af::IoStatus symlink_no_uring_status = af::io_symlinkat(
            *this,
            IoTestThread::IO_0,
            "/tmp/asyncflow-symlinkat-target",
            AT_FDCWD,
            "/tmp/asyncflow-symlinkat-boundary",
            symlink_no_uring);
        const af::IoStatus link_no_uring_status = af::io_linkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-linkat-old",
            AT_FDCWD,
            "/tmp/asyncflow-linkat-new",
            0,
            link_no_uring);
        const af::IoStatus rename_null_old_status = af::io_renameat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary-new",
            0,
            rename_null_old);
        const af::IoStatus rename_null_new_status = af::io_renameat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary-old",
            AT_FDCWD,
            nullptr,
            0,
            rename_null_new);
        const af::IoStatus unlink_null_path_status = af::io_unlinkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            0,
            unlink_null_path);
        const af::IoStatus openat2_null_path_status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            &how,
            &opened,
            openat2_null_path);
        const af::IoStatus openat2_null_how_status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat2-boundary",
            nullptr,
            &opened,
            openat2_null_how);
        const af::IoStatus openat2_null_output_status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat2-boundary",
            &how,
            nullptr,
            openat2_null_output);
        const af::IoStatus mkdir_null_path_status = af::io_mkdirat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            0700U,
            mkdir_null_path);
        const af::IoStatus symlink_null_target_status = af::io_symlinkat(
            *this,
            IoTestThread::IO_0,
            nullptr,
            AT_FDCWD,
            "/tmp/asyncflow-symlinkat-boundary",
            symlink_null_target);
        const af::IoStatus symlink_null_path_status = af::io_symlinkat(
            *this,
            IoTestThread::IO_0,
            "/tmp/asyncflow-symlinkat-target",
            AT_FDCWD,
            nullptr,
            symlink_null_path);
        const af::IoStatus link_null_old_status = af::io_linkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            AT_FDCWD,
            "/tmp/asyncflow-linkat-new",
            0,
            link_null_old);
        const af::IoStatus link_null_new_status = af::io_linkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-linkat-old",
            AT_FDCWD,
            nullptr,
            0,
            link_null_new);
        if (!rename_no_uring_status.failed() || rename_no_uring_status.error != ENOSYS ||
            !unlink_no_uring_status.failed() || unlink_no_uring_status.error != ENOSYS ||
            !openat2_no_uring_status.failed() || openat2_no_uring_status.error != ENOSYS ||
            !mkdir_no_uring_status.failed() || mkdir_no_uring_status.error != ENOSYS ||
            !symlink_no_uring_status.failed() || symlink_no_uring_status.error != ENOSYS ||
            !link_no_uring_status.failed() || link_no_uring_status.error != ENOSYS ||
            !rename_null_old_status.failed() || rename_null_old_status.error != EINVAL ||
            !rename_null_new_status.failed() || rename_null_new_status.error != EINVAL ||
            !unlink_null_path_status.failed() || unlink_null_path_status.error != EINVAL ||
            !openat2_null_path_status.failed() || openat2_null_path_status.error != EINVAL ||
            !openat2_null_how_status.failed() || openat2_null_how_status.error != EINVAL ||
            !openat2_null_output_status.failed() || openat2_null_output_status.error != EINVAL ||
            !mkdir_null_path_status.failed() || mkdir_null_path_status.error != EINVAL ||
            !symlink_null_target_status.failed() || symlink_null_target_status.error != EINVAL ||
            !symlink_null_path_status.failed() || symlink_null_path_status.error != EINVAL ||
            !link_null_old_status.failed() || link_null_old_status.error != EINVAL ||
            !link_null_new_status.failed() || link_null_new_status.error != EINVAL ||
            opened != -1) {
            return failed();
        }
        error_->store(rename_no_uring_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};
