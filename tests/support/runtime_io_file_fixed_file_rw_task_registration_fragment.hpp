#if !defined(AF_RUNTIME_IO_FILE_FIXED_FILE_RW_TASK_FRAGMENT_INCLUDE)
#error "runtime_io_file_fixed_file_rw_task_registration_fragment.hpp is a UringFixedFileTask implementation fragment"
#endif

af::TaskResult register_file() {
    const af::IoStatus no_table = file_.write_at(
        *this,
        &value_,
        sizeof(value_),
        0,
        no_table_);
    if (!no_table.failed() || no_table.error != ENXIO) {
        return failed();
    }

    int error = 0;
    if (!UringIoRuntime::io_register_files(IoTestThread::IO_0, &fd_, 1, &error)) {
        return failed();
    }

    int duplicate_error = 0;
    if (UringIoRuntime::io_register_files(
            IoTestThread::IO_0,
            &fd_,
            1,
            &duplicate_error) ||
        duplicate_error != EALREADY) {
        return failed();
    }

    af::IoFixedFile<IoTestThread> bad_file(IoTestThread::IO_0, 1);
    const af::IoStatus bad_index = bad_file.write_at(
        *this,
        &value_,
        sizeof(value_),
        0,
        bad_index_);
    if (!bad_index.failed() || bad_index.error != EINVAL) {
        return failed();
    }

    const af::IoStatus no_buffer = file_.write_fixed_at(
        *this,
        buffer_,
        1,
        0,
        0,
        no_buffer_);
    if (!no_buffer.failed() || no_buffer.error != ENOBUFS) {
        return failed();
    }

    iovec iov{buffer_, sizeof(buffer_)};
    int buffer_error = 0;
    if (!UringIoRuntime::io_register_buffers(IoTestThread::IO_0, &iov, 1, &buffer_error)) {
        return failed();
    }

    buffer_[0] = value_;
    state_ = State::Write;
    return again();
}

af::TaskResult unregister_file() {
    int error = 0;
    if (!UringIoRuntime::io_unregister_buffers(IoTestThread::IO_0, &error)) {
        return failed();
    }
    if (!UringIoRuntime::io_unregister_files(IoTestThread::IO_0, &error)) {
        return failed();
    }
    byte_read_->store(buffer_[0], std::memory_order_release);
    completed_->fetch_add(1, std::memory_order_release);
    return done();
}
