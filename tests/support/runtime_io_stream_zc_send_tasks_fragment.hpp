#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_stream_zc_send_tasks_fragment.hpp is a runtime_io_test_support implementation fragment"
#endif

class SendZcSocketTask final : public IoTaskBase {
public:
    explicit SendZcSocketTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        const char* payload,
        std::size_t total_size,
        std::size_t chunk_size,
        std::atomic<int>* completed,
        std::atomic<int>* calls,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        payload_ = payload;
        total_size_ = total_size;
        chunk_size_ = chunk_size == 0U ? total_size : chunk_size;
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
        const af::IoStatus status =
            stream_.send_zc_some(*this, payload_ + sent_, count, send_);
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
    const char* payload_{nullptr};
    std::size_t total_size_{0};
    std::size_t chunk_size_{0};
    std::size_t sent_{0};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* calls_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class SendvZcSocketTask final : public IoTaskBase {
public:
    explicit SendvZcSocketTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        const char* first,
        std::size_t first_size,
        const char* second,
        std::size_t second_size,
        std::atomic<int>* completed,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        first_ = first;
        first_size_ = first_size;
        second_ = second;
        second_size_ = second_size;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const std::size_t total_size = first_size_ + second_size_;
        if (sent_ >= total_size) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        int iov_count = 0;
        if (sent_ < first_size_) {
            iov_[iov_count++] = iovec{
                const_cast<char*>(first_ + sent_),
                first_size_ - sent_};
            iov_[iov_count++] = iovec{const_cast<char*>(second_), second_size_};
        } else {
            const std::size_t second_offset = sent_ - first_size_;
            iov_[iov_count++] = iovec{
                const_cast<char*>(second_ + second_offset),
                second_size_ - second_offset};
        }

        const af::IoStatus status = stream_.sendv_zc_some(*this, iov_, iov_count, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > total_size - sent_) {
            return failed();
        }

        sent_ += status.bytes;
        bytes_sent_->store(sent_, std::memory_order_release);
        return again();
    }

    af::TcpStream<IoTestThread> stream_{};
    const char* first_{nullptr};
    const char* second_{nullptr};
    std::size_t first_size_{0};
    std::size_t second_size_{0};
    std::size_t sent_{0};
    iovec iov_[2]{};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};
