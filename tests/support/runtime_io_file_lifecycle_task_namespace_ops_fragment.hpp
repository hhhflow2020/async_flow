#if !defined(AF_RUNTIME_IO_FILE_LIFECYCLE_TASK_FRAGMENT_INCLUDE)
#error "runtime_io_file_lifecycle_task_namespace_ops_fragment.hpp is a UringFileLifecycleTask implementation fragment"
#endif

af::TaskResult stat_file() {
    const af::IoStatus status = af::io_statx(
        *this,
        IoTestThread::IO_0,
        AT_FDCWD,
        path_,
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
    observed_size_->store(stat_.stx_size, std::memory_order_release);
    state_ = State::Rename;
    return again();
}

af::TaskResult rename_file() {
    const af::IoStatus status = af::io_renameat(
        *this,
        IoTestThread::IO_0,
        AT_FDCWD,
        path_,
        AT_FDCWD,
        renamed_path_,
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
        IoTestThread::IO_0,
        AT_FDCWD,
        renamed_path_,
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
