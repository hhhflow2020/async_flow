#pragma once

class PendingSendZcTask final : public IoTaskBase {
public:
    explicit PendingSendZcTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int socket_fd, std::atomic<int> *pending_seen, std::atomic<int> *completed,
               std::atomic<std::size_t> *bytes_sent) {
        stream_.reset(IoTestThreads::IO_0, socket_fd);
        pending_seen_ = pending_seen;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_byte();
    }

    af::TaskResult send_byte() {
        const af::IoStatus status = stream_.send_zc_some(*this, &value_, sizeof(value_), send_);
        if (status.pending()) {
            pending_seen_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        bytes_sent_->store(status.bytes, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{'Z'};
    af::IoOpState send_{};
    std::atomic<int> *pending_seen_{nullptr};
    std::atomic<int> *completed_{nullptr};
    std::atomic<std::size_t> *bytes_sent_{nullptr};
};
