#pragma once

class SocketReadableTask final : public IoTaskBase {
public:
    explicit SocketReadableTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *armed, std::atomic<int> *completed,
               std::atomic<char> *byte_read) {
        fd_ = fd;
        armed_ = armed;
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Arm:
            return arm_read();

        case State::Read:
            return finish_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_ = State::Read;
        const af::IoStatus status =
            af::io_read_some(*this, IoTestThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (!status.pending()) {
            return failed();
        }
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    af::TaskResult finish_read() {
        const af::IoStatus status =
            af::io_read_some(*this, IoTestThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        byte_read_->store(value_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Arm};
    int fd_{-1};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<int> *armed_{nullptr};
    std::atomic<int> *completed_{nullptr};
    std::atomic<char> *byte_read_{nullptr};
};

class SocketRepeatedReadableTask final : public IoTaskBase {
public:
    explicit SocketRepeatedReadableTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *armed, std::atomic<int> *completed,
               std::atomic<int> *reads, char *output) {
        fd_ = fd;
        armed_ = armed;
        completed_ = completed;
        reads_ = reads;
        output_ = output;
        return schedule(IoTestThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Arm:
            return arm_read();

        case State::Read:
            return finish_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_ = State::Read;
        const af::IoStatus status =
            af::io_read_some(*this, IoTestThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        return handle_status(status);
    }

    af::TaskResult finish_read() {
        const af::IoStatus status =
            af::io_read_some(*this, IoTestThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        return handle_status(status);
    }

    af::TaskResult handle_status(af::IoStatus status) {
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }

        output_[read_count_] = value_;
        ++read_count_;
        reads_->store(static_cast<int>(read_count_), std::memory_order_release);
        if (read_count_ == expected_reads_) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        state_ = State::Arm;
        return arm_read();
    }

    static constexpr std::size_t expected_reads_{2};
    State state_{State::Arm};
    int fd_{-1};
    char value_{0};
    std::size_t read_count_{0};
    af::IoOpState read_{};
    std::atomic<int> *armed_{nullptr};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *reads_{nullptr};
    char *output_{nullptr};
};

class SocketWritableTask final : public IoTaskBase {
public:
    explicit SocketWritableTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *armed, std::atomic<int> *completed) {
        fd_ = fd;
        armed_ = armed;
        completed_ = completed;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status =
            af::io_write_some(*this, IoTestThreads::IO_0, fd_, &value_, sizeof(value_), write_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int fd_{-1};
    char value_{'w'};
    af::IoOpState write_{};
    std::atomic<int> *armed_{nullptr};
    std::atomic<int> *completed_{nullptr};
};
