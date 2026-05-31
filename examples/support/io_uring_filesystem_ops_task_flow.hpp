#if !defined(AF_EXAMPLE_IO_URING_FILESYSTEM_OPS_TASK_FRAGMENT_INCLUDE)
#error "io_uring_filesystem_ops_task_flow.hpp is a FilesystemOpsTask implementation fragment"
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
    return finish(EIO);
}

af::TaskResult finish(int error) {
    result_->error = error;
    return done();
}
