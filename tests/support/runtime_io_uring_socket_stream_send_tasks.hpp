#pragma once

class UringStreamSendTask final : public UringIoTaskBase {
public:
    explicit UringStreamSendTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *completed) {
        stream_.reset(IoTestThreads::IO_0, fd);
        completed_ = completed;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.send_some(*this, &value_, sizeof(value_), write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{'S'};
    af::IoOpState write_{};
    std::atomic<int> *completed_{nullptr};
};

class UringStreamSendZcTask final : public UringIoTaskBase {
public:
    explicit UringStreamSendZcTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *completed) {
        stream_.reset(IoTestThreads::IO_0, fd);
        completed_ = completed;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_value();
    }

    af::TaskResult send_value() {
        const af::IoStatus status = stream_.send_zc_some(*this, &value_, sizeof(value_), write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{'Z'};
    af::IoOpState write_{};
    std::atomic<int> *completed_{nullptr};
};
