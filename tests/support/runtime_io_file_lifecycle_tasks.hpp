#pragma once

class UringFileLifecycleTask final : public UringIoTaskBase {
public:
    explicit UringFileLifecycleTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(const char *path, const char *renamed_path, std::atomic<int> *completed,
               std::atomic<int> *close_released, std::atomic<std::uint64_t> *observed_size) {
        path_ = path;
        renamed_path_ = renamed_path;
        completed_ = completed;
        close_released_ = close_released;
        observed_size_ = observed_size;
        return schedule(IoTestThreads::IO_0);
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
            af::io_openat(*this, IoTestThreads::IO_0, AT_FDCWD, path_,
                          O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC, 0600U, &fd, open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return failed();
        }
        owned_.reset(fd);
        file_.reset(IoTestThreads::IO_0, owned_.get());
        state_ = State::Fallocate;
        return again();
    }

    af::TaskResult fallocate_file() {
        const af::IoStatus status = af::io_fallocate(*this, IoTestThreads::IO_0, owned_.get(),
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

    af::TaskResult stat_file() {
        const af::IoStatus status = af::io_statx(*this, IoTestThreads::IO_0, AT_FDCWD, path_, 0,
                                                 STATX_SIZE, &stat_, stat_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || stat_.stx_size != sizeof(value_)) {
            return failed();
        }
        observed_size_->store(stat_.stx_size, std::memory_order_release);
        state_ = State::Rename;
        return again();
    }

    af::TaskResult rename_file() {
        const af::IoStatus status = af::io_renameat(*this, IoTestThreads::IO_0, AT_FDCWD, path_,
                                                    AT_FDCWD, renamed_path_, 0, rename_);
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
        const af::IoStatus status =
            af::io_unlinkat(*this, IoTestThreads::IO_0, AT_FDCWD, renamed_path_, 0, unlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Close;
        return again();
    }

    af::TaskResult close_file() {
        const af::IoStatus status = af::io_close(*this, IoTestThreads::IO_0, owned_, close_);
        if (status.pending()) {
            if (owned_.get() != -1) {
                return failed();
            }
            return pending();
        }
        if (!status.ready() || owned_.get() != -1) {
            return failed();
        }
        close_released_->store(1, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Open};
    const char *path_{nullptr};
    const char *renamed_path_{nullptr};
    af::UniqueFd owned_{};
    af::IoFile<IoTestThread> file_{};
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
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *close_released_{nullptr};
    std::atomic<std::uint64_t> *observed_size_{nullptr};
};
