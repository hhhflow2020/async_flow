#pragma once

class UringFileReadWriteTask final : public UringIoTaskBase {
public:
    explicit UringFileReadWriteTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *completed, std::atomic<char> *byte_read) {
        file_.reset(IoTestThreads::IO_0, fd);
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThreads::IO_0);
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
        if (!status.ready() || status.bytes != sizeof(read_)) {
            return failed();
        }
        byte_read_->store(read_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Write};
    af::IoFile<IoTestThread> file_{};
    char value_{'F'};
    char read_{0};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<char> *byte_read_{nullptr};
};

class UringOversizedReadRejectTask final : public UringIoTaskBase {
public:
    explicit UringOversizedReadRejectTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *completed, std::atomic<int> *error,
               std::atomic<std::int64_t> *result_value, std::atomic<int> *token_cleared) {
        fd_ = fd;
        completed_ = completed;
        error_ = error;
        result_value_ = result_value;
        token_cleared_ = token_cleared;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        int token = 0;
        char buffer = 0;
        af::IoResult result{fd_, af::io_readable, 0, 64, &token};
        const auto oversized = static_cast<std::size_t>(std::numeric_limits<unsigned>::max()) + 1U;
        const bool accepted = UringIoRuntime::io_submit_read_at(IoTestThreads::IO_0, fd_, &buffer,
                                                                oversized, 0, this, &result);
        if (accepted) {
            return failed();
        }

        error_->store(result.error, std::memory_order_release);
        result_value_->store(result.result, std::memory_order_release);
        token_cleared_->store(result.completion_token == nullptr ? 1 : 0,
                              std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int fd_{-1};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *error_{nullptr};
    std::atomic<std::int64_t> *result_value_{nullptr};
    std::atomic<int> *token_cleared_{nullptr};
};
