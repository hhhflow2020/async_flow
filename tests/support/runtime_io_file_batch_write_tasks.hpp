#pragma once

class UringBatchedFileWriteTask final : public UringIoTaskBase {
public:
    explicit UringBatchedFileWriteTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(int fd, std::uint64_t offset, char value, std::atomic<int> *completed) {
        file_.reset(IoTestThread::IO_0, fd);
        offset_ = offset;
        value_ = value;
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), offset_, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::IoFile<IoTestThread> file_{};
    std::uint64_t offset_{0};
    char value_{0};
    af::IoOpState write_{};
    std::atomic<int> *completed_{nullptr};
};
