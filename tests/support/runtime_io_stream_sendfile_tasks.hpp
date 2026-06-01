#pragma once

class SendfileSocketTask final : public IoTaskBase {
public:
    explicit SendfileSocketTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        int file_fd,
        std::size_t total_size,
        std::size_t chunk_size,
        bool use_null_offset,
        std::atomic<int>* completed,
        std::atomic<int>* calls,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        file_fd_ = file_fd;
        total_size_ = total_size;
        chunk_size_ = chunk_size == 0U ? total_size : chunk_size;
        use_null_offset_ = use_null_offset;
        completed_ = completed;
        calls_ = calls;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_next();
    }

    af::TaskResult send_next() {
        if (sent_ >= total_size_) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        const std::size_t remaining = total_size_ - sent_;
        const std::size_t count = remaining < chunk_size_ ? remaining : chunk_size_;
        af::IoOffset* offset = use_null_offset_ ? nullptr : &offset_;
        const af::IoStatus status = stream_.sendfile_some(*this, file_fd_, offset, count, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > remaining) {
            return failed();
        }
        sent_ += status.bytes;
        calls_->fetch_add(1, std::memory_order_release);
        bytes_sent_->store(sent_, std::memory_order_release);
        return again();
    }

    af::TcpStream<IoTestThread> stream_{};
    int file_fd_{-1};
    af::IoOffset offset_{0};
    std::size_t total_size_{0};
    std::size_t chunk_size_{0};
    std::size_t sent_{0};
    bool use_null_offset_{false};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* calls_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class PendingSendfileTask final : public IoTaskBase {
public:
    explicit PendingSendfileTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        int file_fd,
        std::atomic<int>* pending_seen,
        std::atomic<int>* completed,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        file_fd_ = file_fd;
        pending_seen_ = pending_seen;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_byte();
    }

    af::TaskResult send_byte() {
        const af::IoStatus status = stream_.sendfile_some(*this, file_fd_, &offset_, 1, send_);
        if (status.pending()) {
            pending_seen_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != 1U || offset_ != 1) {
            return failed();
        }
        bytes_sent_->store(status.bytes, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    int file_fd_{-1};
    af::IoOffset offset_{0};
    af::IoOpState send_{};
    std::atomic<int>* pending_seen_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class UringPendingSendfilePollTask final : public UringIoTaskBase {
public:
    explicit UringPendingSendfilePollTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        int file_fd,
        std::atomic<af::IoOpState*>* state,
        std::atomic<int>* wait_kind,
        std::atomic<int>* pending_seen,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        file_fd_ = file_fd;
        state_ = state;
        wait_kind_ = wait_kind;
        pending_seen_ = pending_seen;
        completed_ = completed;
        error_ = error;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.sendfile_some(*this, file_fd_, &offset_, 1, send_);
        if (status.pending()) {
            state_->store(&send_, std::memory_order_release);
            wait_kind_->store(static_cast<int>(send_.wait_kind), std::memory_order_release);
            pending_seen_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (status.failed()) {
            error_->store(status.error, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (!status.ready() || status.bytes != 1U || offset_ != 1) {
            return failed();
        }
        bytes_sent_->store(status.bytes, std::memory_order_release);
        error_->store(0, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    int file_fd_{-1};
    af::IoOffset offset_{0};
    af::IoOpState send_{};
    std::atomic<af::IoOpState*>* state_{nullptr};
    std::atomic<int>* wait_kind_{nullptr};
    std::atomic<int>* pending_seen_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};
