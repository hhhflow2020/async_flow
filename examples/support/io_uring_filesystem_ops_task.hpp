#pragma once

#include <cerrno>
#include <cstdint>

#include "io_uring_filesystem_ops_runtime.hpp"

#if defined(__linux__)
#include <fcntl.h>
#include <linux/openat2.h>
#include <sys/stat.h>
#include <unistd.h>

namespace io_uring_filesystem_ops_example {

class FilesystemOpsTask final : public FsTaskBase {
public:
    explicit FilesystemOpsTask(FsTaskBase::FactoryToken token) : FsTaskBase(token) {}

    bool do_it(const char *dir_path, const char *file_path, const char *hardlink_path,
               const char *symlink_path, FsResult *result) {
        dir_path_ = dir_path;
        file_path_ = file_path;
        hardlink_path_ = hardlink_path;
        symlink_path_ = symlink_path;
        result_ = result;
        how_.flags = O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC;
        how_.mode = 0600U;
        return schedule(FsThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Mkdir,
        OpenAt2,
        Write,
        Ftruncate,
        Fsync,
        Statx,
        Close,
        Link,
        Symlink,
        UnlinkFile,
        UnlinkHardlink,
        UnlinkSymlink,
        Rmdir,
    };

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

    af::TaskResult mkdir_dir() {
        const af::IoStatus status =
            af::io_mkdirat(*this, FsThread::IO_0, AT_FDCWD, dir_path_, 0700U, mkdir_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::OpenAt2;
        return again();
    }

    af::TaskResult open_file() {
        int fd = -1;
        const af::IoStatus status =
            af::io_openat2(*this, FsThread::IO_0, AT_FDCWD, file_path_, &how_, &fd, open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return finish(status.failed() ? status.error : EIO);
        }
        owned_.reset(fd);
        file_.reset(FsThread::IO_0, owned_.get());
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_payload() {
        const af::IoStatus status = file_.write_at(*this, payload_, sizeof(payload_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(payload_)) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::Ftruncate;
        return again();
    }

    af::TaskResult truncate_file() {
        const af::IoStatus status =
            af::io_ftruncate(*this, FsThread::IO_0, owned_.get(), 1, truncate_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_file() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::Statx;
        return again();
    }

    af::TaskResult stat_file() {
        const af::IoStatus status = af::io_statx(*this, FsThread::IO_0, AT_FDCWD, file_path_, 0,
                                                 STATX_SIZE, &stat_, stat_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || stat_.stx_size != 1U) {
            return finish(status.failed() ? status.error : EIO);
        }
        result_->observed_size = stat_.stx_size;
        state_ = State::Close;
        return again();
    }

    af::TaskResult close_file() {
        const af::IoStatus status = af::io_close(*this, FsThread::IO_0, owned_, close_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || owned_.get() != -1) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::Link;
        return again();
    }

    af::TaskResult link_file() {
        const af::IoStatus status = af::io_linkat(*this, FsThread::IO_0, AT_FDCWD, file_path_,
                                                  AT_FDCWD, hardlink_path_, 0, link_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::Symlink;
        return again();
    }

    af::TaskResult symlink_file() {
        const af::IoStatus status =
            af::io_symlinkat(*this, FsThread::IO_0, file_path_, AT_FDCWD, symlink_path_, symlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::UnlinkFile;
        return again();
    }

    af::TaskResult unlink_path(const char *path, State next_state, int flags,
                               bool final_state = false) {
        const af::IoStatus status =
            af::io_unlinkat(*this, FsThread::IO_0, AT_FDCWD, path, flags, unlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        if (final_state) {
            return finish(0);
        }
        state_ = next_state;
        unlink_.reset();
        return again();
    }

    State state_{State::Mkdir};
    const char *dir_path_{nullptr};
    const char *file_path_{nullptr};
    const char *hardlink_path_{nullptr};
    const char *symlink_path_{nullptr};
    FsResult *result_{nullptr};
    struct open_how how_{};
    af::UniqueFd owned_{};
    af::IoFile<FsThread> file_{};
    char payload_[2]{'F', 'S'};
    struct statx stat_{};
    af::IoOpState mkdir_{};
    af::IoOpState open_{};
    af::IoOpState write_{};
    af::IoOpState truncate_{};
    af::IoOpState fsync_{};
    af::IoOpState stat_state_{};
    af::IoOpState close_{};
    af::IoOpState link_{};
    af::IoOpState symlink_{};
    af::IoOpState unlink_{};
};

} // namespace io_uring_filesystem_ops_example

#endif
