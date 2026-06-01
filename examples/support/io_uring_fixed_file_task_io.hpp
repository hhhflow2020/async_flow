#if !defined(AF_EXAMPLE_IO_URING_FIXED_FILE_TASK_FRAGMENT_INCLUDE)
#error "io_uring_fixed_file_task_io.hpp is a FixedFileRoundTripTask implementation fragment"
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
    if (!status.ready()) {
        return complete(status.failed() ? status.error : EIO);
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
        return complete(status.failed() ? status.error : EIO);
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
        return complete(status.failed() ? status.error : EIO);
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
    if (!status.ready() || status.bytes != 1U) {
        return complete(status.failed() ? status.error : EIO);
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
        return complete(status.failed() ? status.error : EIO);
    }
    *vectored_read_ = pack_vectored_read();
    state_ = State::Update;
    return again();
}

af::TaskResult read_updated_value() {
    const af::IoStatus status = file_.read_at(
        *this,
        &updated_read_,
        sizeof(updated_read_),
        0,
        updated_read_state_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || status.bytes != sizeof(updated_read_)) {
        return complete(status.failed() ? status.error : EIO);
    }
    state_ = State::Unregister;
    return again();
}

[[nodiscard]] int pack_vectored_read() const noexcept {
    return (static_cast<int>(static_cast<unsigned char>(vector_read_[0])) << 8) |
           static_cast<int>(static_cast<unsigned char>(vector_read_[1]));
}
