#if !defined(AF_RUNTIME_IO_FILE_LIFECYCLE_TASK_FRAGMENT_INCLUDE)
#error "runtime_io_file_lifecycle_task_flow_fragment.hpp is a UringFileLifecycleTask implementation fragment"
#endif

af::TaskResult run() override {
    switch (state_) {
    case State::Open:
        return open_file();

    case State::Fallocate:
        return fallocate_file();

    case State::Write:
        return write_value();

    case State::Fsync:
        return fsync_value();

    case State::Read:
        return read_value();

    case State::Statx:
        return stat_file();

    case State::Rename:
        return rename_file();

    case State::Unlink:
        return unlink_file();

    case State::Close:
        return close_file();
    }
    return failed();
}
