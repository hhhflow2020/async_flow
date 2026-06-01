#pragma once

#include <array>
#include <cstdio>
#include <cstdint>

#include "io_uring_file_lifecycle_runtime.hpp"

#if defined(__linux__)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace io_uring_file_lifecycle_example {

class FileLifecycleTask final : public LifecycleTaskBase {
public:
    explicit FileLifecycleTask(LifecycleTaskBase::FactoryToken token) : LifecycleTaskBase(token) {}

    bool do_it(const char *path, const char *renamed_path, std::uint64_t *observed_size) {
        if (!copy_path(path_.data(), path_.size(), path) ||
            !copy_path(renamed_path_.data(), renamed_path_.size(), renamed_path)) {
            return false;
        }
        observed_size_ = observed_size;
        return schedule(LifecycleThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Open,
        Fallocate,
        Write,
        Fsync,
        Read,
        Statx,
        Rename,
        Unlink,
        Close,
    };

    static bool copy_path(char *output, std::size_t output_size, const char *input) {
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

    af::TaskResult open_file() {
        int fd = -1;
        const af::IoStatus status =
            af::io_openat(*this, LifecycleThreads::IO_0, AT_FDCWD, path_.data(),
                          O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC, 0600U, &fd, open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return failed();
        }
        owned_.reset(fd);
        file_.reset(LifecycleThreads::IO_0, owned_.get());
        state_ = State::Fallocate;
        return again();
    }

    af::TaskResult fallocate_file() {
        const af::IoStatus status = af::io_fallocate(*this, LifecycleThreads::IO_0, owned_.get(),
                                                     FALLOC_FL_KEEP_SIZE, 0, 4096, fallocate_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
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
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_at(*this, &read_, sizeof(read_), 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_) || read_ != value_) {
            return failed();
        }
        state_ = State::Statx;
        return again();
    }

    af::TaskResult close_file() {
        const af::IoStatus status = af::io_close(*this, LifecycleThreads::IO_0, owned_, close_);
        if (status.pending()) {
            return owned_.get() == -1 ? pending() : failed();
        }
        if (!status.ready() || owned_.get() != -1) {
            return failed();
        }
        return done();
    }

    af::TaskResult stat_file() {
        const af::IoStatus status = af::io_statx(*this, LifecycleThreads::IO_0, AT_FDCWD,
                                                 path_.data(), 0, STATX_SIZE, &stat_, stat_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || stat_.stx_size != sizeof(value_)) {
            return failed();
        }
        *observed_size_ = stat_.stx_size;
        state_ = State::Rename;
        return again();
    }

    af::TaskResult rename_file() {
        const af::IoStatus status =
            af::io_renameat(*this, LifecycleThreads::IO_0, AT_FDCWD, path_.data(), AT_FDCWD,
                            renamed_path_.data(), 0, rename_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Unlink;
        return again();
    }

    af::TaskResult unlink_file() {
        const af::IoStatus status = af::io_unlinkat(*this, LifecycleThreads::IO_0, AT_FDCWD,
                                                    renamed_path_.data(), 0, unlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Close;
        return again();
    }

    State state_{State::Open};
    std::array<char, 160> path_{};
    std::array<char, 176> renamed_path_{};
    af::UniqueFd owned_{};
    af::IoFile<LifecycleThread> file_{};
    char value_{'L'};
    char read_{0};
    struct statx stat_{};
    af::IoOpState open_{};
    af::IoOpState fallocate_{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    af::IoOpState stat_state_{};
    af::IoOpState rename_{};
    af::IoOpState unlink_{};
    af::IoOpState close_{};
    std::uint64_t *observed_size_{nullptr};
};

} // namespace io_uring_file_lifecycle_example

#endif
