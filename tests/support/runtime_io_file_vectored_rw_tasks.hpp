#pragma once

class UringFileVectoredReadWriteTask final : public UringIoTaskBase {
public:
    explicit UringFileVectoredReadWriteTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *completed, std::atomic<int> *bytes_read) {
        file_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        bytes_read_ = bytes_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Write,
        Fsync,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();
        }
        return failed();
    }

    af::TaskResult write_value() {
        write_iov_[0] = iovec{&first_, 1};
        write_iov_[1] = iovec{&second_, 1};
        const af::IoStatus status = file_.writev_at(*this, write_iov_, 2, 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 2U) {
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
        read_iov_[0] = iovec{&read_[0], 1};
        read_iov_[1] = iovec{&read_[1], 1};
        const af::IoStatus status = file_.readv_at(*this, read_iov_, 2, 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 2U || read_[0] != first_ || read_[1] != second_) {
            return failed();
        }
        bytes_read_->store(static_cast<int>(status.bytes), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Write};
    af::IoFile<IoTestThread> file_{};
    char first_{'V'};
    char second_{'W'};
    char read_[2]{};
    iovec write_iov_[2]{};
    iovec read_iov_[2]{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *bytes_read_{nullptr};
};
