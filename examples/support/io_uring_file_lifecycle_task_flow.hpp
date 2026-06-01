#if !defined(AF_EXAMPLE_IO_URING_FILE_LIFECYCLE_TASK_FRAGMENT_INCLUDE)
#error "io_uring_file_lifecycle_task_flow.hpp is a FileLifecycleTask implementation fragment"
#endif

static bool copy_path(char* output, std::size_t output_size, const char* input) {
    const int written = std::snprintf(output, output_size, "%s", input);
    return written >= 0 && static_cast<std::size_t>(written) < output_size;
}

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
