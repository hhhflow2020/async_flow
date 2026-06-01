#pragma once

class SplicePipeTask final : public IoTaskBase {
public:
    explicit SplicePipeTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int input_fd, int output_fd, std::size_t total_size, std::atomic<int> *completed,
               std::atomic<std::size_t> *bytes_spliced) {
        input_fd_ = input_fd;
        output_fd_ = output_fd;
        total_size_ = total_size;
        completed_ = completed;
        bytes_spliced_ = bytes_spliced;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return splice_next();
    }

    af::TaskResult splice_next() {
        if (spliced_ >= total_size_) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        const af::IoStatus status =
            af::io_splice_some(*this, IoTestThread::IO_0, input_fd_, nullptr, output_fd_, nullptr,
                               total_size_ - spliced_, SPLICE_F_NONBLOCK, splice_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > total_size_ - spliced_) {
            return failed();
        }
        spliced_ += status.bytes;
        bytes_spliced_->store(spliced_, std::memory_order_release);
        return again();
    }

    int input_fd_{-1};
    int output_fd_{-1};
    std::size_t total_size_{0};
    std::size_t spliced_{0};
    af::IoOpState splice_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<std::size_t> *bytes_spliced_{nullptr};
};
