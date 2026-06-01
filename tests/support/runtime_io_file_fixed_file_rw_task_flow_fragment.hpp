#if !defined(AF_RUNTIME_IO_FILE_FIXED_FILE_RW_TASK_FRAGMENT_INCLUDE)
#error "runtime_io_file_fixed_file_rw_task_flow_fragment.hpp is a UringFixedFileTask implementation fragment"
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

    case State::Unregister:
        return unregister_file();
    }
    return failed();
}
