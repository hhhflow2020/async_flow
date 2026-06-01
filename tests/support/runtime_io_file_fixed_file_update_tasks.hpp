#pragma once

class UringFixedFileUpdateTask final : public UringIoTaskBase {
public:
    explicit UringFixedFileUpdateTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(int first_fd, int second_fd, std::atomic<int> *completed,
               std::atomic<int> *packed_read) {
        first_fd_ = first_fd;
        second_fd_ = second_fd;
        completed_ = completed;
        packed_read_ = packed_read;
        file_.reset(IoTestThreads::IO_0, 0);
        return schedule(IoTestThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        ReadFirst,
        Update,
        ReadSecond,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_file();

        case State::ReadFirst:
            return read_first();

        case State::Update:
            return update_file();

        case State::ReadSecond:
            return read_second();

        case State::Unregister:
            return unregister_file();
        }
        return failed();
    }

    af::TaskResult register_file() {
        int error = 0;
        if (UringIoRuntime::io_update_registered_files(IoTestThreads::IO_0, 0, &second_fd_, 1,
                                                       &error) ||
            error != ENOENT) {
            return failed();
        }
        if (!UringIoRuntime::io_register_files(IoTestThreads::IO_0, &first_fd_, 1, &error)) {
            return failed();
        }
        if (UringIoRuntime::io_update_registered_files(IoTestThreads::IO_0, 1, &second_fd_, 1,
                                                       &error) ||
            error != EINVAL) {
            return failed();
        }
        state_ = State::ReadFirst;
        return again();
    }

    af::TaskResult read_first() {
        const af::IoStatus status =
            file_.read_at(*this, &first_read_, sizeof(first_read_), 0, first_read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(first_read_) || first_read_ != first_value_) {
            return failed();
        }
        state_ = State::Update;
        return again();
    }

    af::TaskResult update_file() {
        int error = 0;
        if (!UringIoRuntime::io_update_registered_files(IoTestThreads::IO_0, 0, &second_fd_, 1,
                                                        &error)) {
            return failed();
        }
        state_ = State::ReadSecond;
        return again();
    }

    af::TaskResult read_second() {
        const af::IoStatus status =
            file_.read_at(*this, &second_read_, sizeof(second_read_), 0, second_read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(second_read_) ||
            second_read_ != second_value_) {
            return failed();
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_file() {
        int error = 0;
        if (!UringIoRuntime::io_unregister_files(IoTestThreads::IO_0, &error)) {
            return failed();
        }
        const int packed = (static_cast<int>(static_cast<unsigned char>(first_read_)) << 8) |
                           static_cast<int>(static_cast<unsigned char>(second_read_));
        packed_read_->store(packed, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Register};
    int first_fd_{-1};
    int second_fd_{-1};
    af::IoFixedFile<IoTestThread> file_{};
    char first_value_{'1'};
    char second_value_{'2'};
    char first_read_{0};
    char second_read_{0};
    af::IoOpState first_read_state_{};
    af::IoOpState second_read_state_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *packed_read_{nullptr};
};
