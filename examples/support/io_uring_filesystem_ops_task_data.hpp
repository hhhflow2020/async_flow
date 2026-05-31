#if !defined(AF_EXAMPLE_IO_URING_FILESYSTEM_OPS_TASK_FRAGMENT_INCLUDE)
#error "io_uring_filesystem_ops_task_data.hpp is a FilesystemOpsTask implementation fragment"
#endif

af::TaskResult mkdir_dir() {
    const af::IoStatus status =
        af::io_mkdirat(*this, FsThread::IO_0, AT_FDCWD, dir_path_, 0700U, mkdir_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready()) {
        return finish(status.failed() ? status.error : EIO);
    }
    state_ = State::OpenAt2;
    return again();
}

af::TaskResult open_file() {
    int fd = -1;
    const af::IoStatus status = af::io_openat2(
        *this,
        FsThread::IO_0,
        AT_FDCWD,
        file_path_,
        &how_,
        &fd,
        open_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || fd < 0) {
        return finish(status.failed() ? status.error : EIO);
    }
    owned_.reset(fd);
    file_.reset(FsThread::IO_0, owned_.get());
    state_ = State::Write;
    return again();
}

af::TaskResult write_payload() {
    const af::IoStatus status = file_.write_at(*this, payload_, sizeof(payload_), 0, write_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || status.bytes != sizeof(payload_)) {
        return finish(status.failed() ? status.error : EIO);
    }
    state_ = State::Ftruncate;
    return again();
}

af::TaskResult truncate_file() {
    const af::IoStatus status =
        af::io_ftruncate(*this, FsThread::IO_0, owned_.get(), 1, truncate_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready()) {
        return finish(status.failed() ? status.error : EIO);
    }
    state_ = State::Fsync;
    return again();
}

af::TaskResult fsync_file() {
    const af::IoStatus status = file_.fsync(*this, fsync_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready()) {
        return finish(status.failed() ? status.error : EIO);
    }
    state_ = State::Statx;
    return again();
}

af::TaskResult stat_file() {
    const af::IoStatus status = af::io_statx(
        *this,
        FsThread::IO_0,
        AT_FDCWD,
        file_path_,
        0,
        STATX_SIZE,
        &stat_,
        stat_state_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || stat_.stx_size != 1U) {
        return finish(status.failed() ? status.error : EIO);
    }
    result_->observed_size = stat_.stx_size;
    state_ = State::Close;
    return again();
}

af::TaskResult close_file() {
    const af::IoStatus status = af::io_close(*this, FsThread::IO_0, owned_, close_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || owned_.get() != -1) {
        return finish(status.failed() ? status.error : EIO);
    }
    state_ = State::Link;
    return again();
}
