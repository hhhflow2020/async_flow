#if !defined(AF_EXAMPLE_IO_URING_FILE_LIFECYCLE_TASK_FRAGMENT_INCLUDE)
#error "io_uring_file_lifecycle_task_namespace_ops.hpp is a FileLifecycleTask implementation fragment"
#endif

af::TaskResult stat_file() {
    const af::IoStatus status = af::io_statx(
        *this,
        LifecycleThread::IO_0,
        AT_FDCWD,
        path_.data(),
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
    *observed_size_ = stat_.stx_size;
    state_ = State::Rename;
    return again();
}

af::TaskResult rename_file() {
    const af::IoStatus status = af::io_renameat(
        *this,
        LifecycleThread::IO_0,
        AT_FDCWD,
        path_.data(),
        AT_FDCWD,
        renamed_path_.data(),
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
        LifecycleThread::IO_0,
        AT_FDCWD,
        renamed_path_.data(),
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
