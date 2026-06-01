#if !defined(AF_EXAMPLE_IO_URING_FIXED_FILE_TASK_FRAGMENT_INCLUDE)
#error "io_uring_fixed_file_task_registration.hpp is a FixedFileRoundTripTask implementation fragment"
#endif

af::TaskResult register_file() {
    int error = 0;
    if (!fixed_file_async::io_register_files(FixedFileThread::IO_0, &fd_, 1, &error)) {
        return complete(error == 0 ? EIO : error);
    }
    iovec iov{buffer_, sizeof(buffer_)};
    if (!fixed_file_async::io_register_buffers(FixedFileThread::IO_0, &iov, 1, &error)) {
        return complete(error == 0 ? EIO : error);
    }
    buffer_[0] = value_;
    state_ = State::Write;
    return again();
}

af::TaskResult update_file() {
    int error = 0;
    if (!fixed_file_async::io_update_registered_files(
            FixedFileThread::IO_0,
            0,
            &updated_fd_,
            1,
            &error)) {
        return complete(error == 0 ? EIO : error);
    }
    state_ = State::ReadUpdated;
    return again();
}

af::TaskResult unregister_file() {
    int error = 0;
    if (!fixed_file_async::io_unregister_buffers(FixedFileThread::IO_0, &error)) {
        return complete(error == 0 ? EIO : error);
    }
    if (!fixed_file_async::io_unregister_files(FixedFileThread::IO_0, &error)) {
        return complete(error == 0 ? EIO : error);
    }
    *byte_read_ = buffer_[0];
    *updated_byte_read_ = updated_read_;
    return complete(0);
}
