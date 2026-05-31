#if !defined(AF_RUNTIME_IO_FILESYSTEM_OPS_TASK_FRAGMENT_INCLUDE)
#error "runtime_io_file_filesystem_ops_flow_tasks_fragment.hpp is a UringFilesystemOpsTask implementation fragment"
#endif

af::TaskResult run() override {
    switch (state_) {
    case State::Mkdir:
        return mkdir_dir();

    case State::OpenAt2:
        return open_file();

    case State::Write:
        return write_payload();

    case State::Ftruncate:
        return truncate_file();

    case State::Fsync:
        return fsync_file();

    case State::Statx:
        return stat_file();

    case State::Close:
        return close_file();

    case State::Link:
        return link_file();

    case State::Symlink:
        return symlink_file();

    case State::UnlinkFile:
        return unlink_path(file_path_, State::UnlinkHardlink, 0);

    case State::UnlinkHardlink:
        return unlink_path(hardlink_path_, State::UnlinkSymlink, 0);

    case State::UnlinkSymlink:
        return unlink_path(symlink_path_, State::Rmdir, 0);

    case State::Rmdir:
        return unlink_path(dir_path_, State::Rmdir, AT_REMOVEDIR, true);
    }
    return complete(EIO);
}

af::TaskResult complete(int error) {
    error_->store(error, std::memory_order_release);
    completed_->fetch_add(1, std::memory_order_release);
    return done();
}
