#if !defined(AF_EXAMPLE_IO_URING_FIXED_FILE_TASK_FRAGMENT_INCLUDE)
#error "io_uring_fixed_file_task_flow.hpp is a FixedFileRoundTripTask implementation fragment"
#endif

af::TaskResult run() override {
    switch (state_) {
    case State::Register:
        return register_file();

    case State::Write:
        return write_value();

    case State::WriteVectored:
        return write_vectored();

    case State::Fsync:
        return fsync_value();

    case State::Read:
        return read_value();

    case State::ReadVectored:
        return read_vectored();

    case State::Update:
        return update_file();

    case State::ReadUpdated:
        return read_updated_value();

    case State::Unregister:
        return unregister_file();
    }
    return complete(EIO);
}

af::TaskResult complete(int error) {
    *error_ = error;
    return done();
}
