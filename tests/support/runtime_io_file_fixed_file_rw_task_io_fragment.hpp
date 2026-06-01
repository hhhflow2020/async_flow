#if !defined(AF_RUNTIME_IO_FILE_FIXED_FILE_RW_TASK_FRAGMENT_INCLUDE)
#error "runtime_io_file_fixed_file_rw_task_io_fragment.hpp is a UringFixedFileTask implementation fragment"
#endif

af::TaskResult write_value() {
    const af::IoStatus status = file_.write_fixed_at(
        *this,
        af::IoFixedBuffer{buffer_, 1, 0},
        0,
        write_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || status.bytes != 1U) {
        return failed();
    }
    buffer_[0] = 0;
    state_ = State::WriteVectored;
    return again();
}

af::TaskResult write_vectored() {
    write_iov_[0] = iovec{&vector_write_[0], 1};
    write_iov_[1] = iovec{&vector_write_[1], 1};
    const af::IoStatus status = file_.writev_at(
        *this,
        write_iov_,
        2,
        1,
        writev_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || status.bytes != sizeof(vector_write_)) {
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
    const af::IoStatus status = file_.read_fixed_at(
        *this,
        af::IoFixedBuffer{buffer_, 1, 0},
        0,
        read_state_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || status.bytes != 1U || buffer_[0] != value_) {
        return failed();
    }
    state_ = State::ReadVectored;
    return again();
}

af::TaskResult read_vectored() {
    read_iov_[0] = iovec{&vector_read_[0], 1};
    read_iov_[1] = iovec{&vector_read_[1], 1};
    const af::IoStatus status = file_.readv_at(
        *this,
        read_iov_,
        2,
        1,
        readv_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() ||
        status.bytes != sizeof(vector_read_) ||
        vector_read_[0] != vector_write_[0] ||
        vector_read_[1] != vector_write_[1]) {
        return failed();
    }
    state_ = State::Unregister;
    return again();
}
