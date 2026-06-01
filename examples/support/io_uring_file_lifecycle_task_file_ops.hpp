#if !defined(AF_EXAMPLE_IO_URING_FILE_LIFECYCLE_TASK_FRAGMENT_INCLUDE)
#error "io_uring_file_lifecycle_task_file_ops.hpp is a FileLifecycleTask implementation fragment"
#endif

af::TaskResult open_file() {
    int fd = -1;
    const af::IoStatus status = af::io_openat(
        *this,
        LifecycleThread::IO_0,
        AT_FDCWD,
        path_.data(),
        O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC,
        0600U,
        &fd,
        open_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || fd < 0) {
        return failed();
    }
    owned_.reset(fd);
    file_.reset(LifecycleThread::IO_0, owned_.get());
    state_ = State::Fallocate;
    return again();
}

af::TaskResult fallocate_file() {
    const af::IoStatus status = af::io_fallocate(
        *this,
        LifecycleThread::IO_0,
        owned_.get(),
        FALLOC_FL_KEEP_SIZE,
        0,
        4096,
        fallocate_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready()) {
        return failed();
    }
    state_ = State::Write;
    return again();
}

af::TaskResult write_value() {
    const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), 0, write_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || status.bytes != sizeof(value_)) {
        return failed();
    }
    state_ = State::Fsync;
    return again();
}

af::TaskResult fsync_value() {
    const af::IoStatus status = file_.fsync(*this, fsync_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready()) {
        return failed();
    }
    state_ = State::Read;
    return again();
}

af::TaskResult read_value() {
    const af::IoStatus status = file_.read_at(*this, &read_, sizeof(read_), 0, read_state_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || status.bytes != sizeof(read_) || read_ != value_) {
        return failed();
    }
    state_ = State::Statx;
    return again();
}

af::TaskResult close_file() {
    const af::IoStatus status = af::io_close(*this, LifecycleThread::IO_0, owned_, close_);
    if (status.pending()) {
        return owned_.get() == -1 ? pending() : failed();
    }
    if (!status.ready() || owned_.get() != -1) {
        return failed();
    }
    return done();
}
