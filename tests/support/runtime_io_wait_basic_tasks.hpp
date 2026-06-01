#pragma once

class SocketHangupTask final : public IoTaskBase {
public:
    explicit SocketHangupTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *armed, std::atomic<int> *closed) {
        fd_ = fd;
        armed_ = armed;
        closed_ = closed;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status =
            af::io_read_some(*this, IoTestThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.closed()) {
            return failed();
        }
        closed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int fd_{-1};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<int> *armed_{nullptr};
    std::atomic<int> *closed_{nullptr};
};

class DuplicateWaitRejectedTask final : public IoTaskBase {
public:
    explicit DuplicateWaitRejectedTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *rejected, std::atomic<int> *error) {
        fd_ = fd;
        rejected_ = rejected;
        error_ = error;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoResult result{};
        if (wait_io(IoTestThreads::IO_0, fd_, af::io_readable, &result)) {
            return failed();
        }
        error_->store(result.error, std::memory_order_release);
        rejected_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int fd_{-1};
    std::atomic<int> *rejected_{nullptr};
    std::atomic<int> *error_{nullptr};
};

class BadFdReadTask final : public IoTaskBase {
public:
    explicit BadFdReadTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        char value = 0;
        af::IoOpState read{};
        const af::IoStatus status =
            af::io_read_some(*this, IoTestThreads::IO_0, -1, &value, sizeof(value), read);
        if (!status.failed()) {
            return failed();
        }
        error_->store(status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *error_{nullptr};
};
