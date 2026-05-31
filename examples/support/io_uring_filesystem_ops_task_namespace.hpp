#if !defined(AF_EXAMPLE_IO_URING_FILESYSTEM_OPS_TASK_FRAGMENT_INCLUDE)
#error "io_uring_filesystem_ops_task_namespace.hpp is a FilesystemOpsTask implementation fragment"
#endif

af::TaskResult link_file() {
    const af::IoStatus status = af::io_linkat(
        *this,
        FsThread::IO_0,
        AT_FDCWD,
        file_path_,
        AT_FDCWD,
        hardlink_path_,
        0,
        link_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready()) {
        return finish(status.failed() ? status.error : EIO);
    }
    state_ = State::Symlink;
    return again();
}

af::TaskResult symlink_file() {
    const af::IoStatus status = af::io_symlinkat(
        *this,
        FsThread::IO_0,
        file_path_,
        AT_FDCWD,
        symlink_path_,
        symlink_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready()) {
        return finish(status.failed() ? status.error : EIO);
    }
    state_ = State::UnlinkFile;
    return again();
}

af::TaskResult unlink_path(
    const char* path,
    State next_state,
    int flags,
    bool final_state = false) {
    const af::IoStatus status =
        af::io_unlinkat(*this, FsThread::IO_0, AT_FDCWD, path, flags, unlink_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready()) {
        return finish(status.failed() ? status.error : EIO);
    }
    if (final_state) {
        return finish(0);
    }
    state_ = next_state;
    unlink_.reset();
    return again();
}
