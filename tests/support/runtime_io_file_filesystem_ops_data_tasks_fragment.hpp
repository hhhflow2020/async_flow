#if !defined(AF_RUNTIME_IO_FILESYSTEM_OPS_TASK_FRAGMENT_INCLUDE)
#error "runtime_io_file_filesystem_ops_data_tasks_fragment.hpp is a UringFilesystemOpsTask implementation fragment"
#endif

af::TaskResult mkdir_dir() {
    const af::IoStatus status = af::io_mkdirat(
        *this,
        IoTestThread::IO_0,
        AT_FDCWD,
        dir_path_,
        0700U,
        mkdir_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready()) {
        return complete(status.failed() ? status.error : EIO);
    }
    state_ = State::OpenAt2;
    return again();
}

af::TaskResult open_file() {
    int fd = -1;
    const af::IoStatus status = af::io_openat2(
        *this,
        IoTestThread::IO_0,
        AT_FDCWD,
        file_path_,
        &how_,
        &fd,
        open_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || fd < 0) {
        return complete(status.failed() ? status.error : EIO);
    }
    owned_.reset(fd);
    file_.reset(IoTestThread::IO_0, owned_.get());
    state_ = State::Write;
    return again();
}

af::TaskResult write_payload() {
    const af::IoStatus status = file_.write_at(*this, payload_, sizeof(payload_), 0, write_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || status.bytes != sizeof(payload_)) {
        return complete(status.failed() ? status.error : EIO);
    }
    state_ = State::Ftruncate;
    return again();
}

af::TaskResult truncate_file() {
    const af::IoStatus status =
        af::io_ftruncate(*this, IoTestThread::IO_0, owned_.get(), 1, truncate_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready()) {
        return complete(status.failed() ? status.error : EIO);
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
        return complete(status.failed() ? status.error : EIO);
    }
    state_ = State::Statx;
    return again();
}

af::TaskResult stat_file() {
    const af::IoStatus status = af::io_statx(
        *this,
        IoTestThread::IO_0,
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
        return complete(status.failed() ? status.error : EIO);
    }
    observed_size_->store(stat_.stx_size, std::memory_order_release);
    state_ = State::Close;
    return again();
}

af::TaskResult close_file() {
    const af::IoStatus status = af::io_close(*this, IoTestThread::IO_0, owned_, close_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || owned_.get() != -1) {
        return complete(status.failed() ? status.error : EIO);
    }
    state_ = State::Link;
    return again();
}
