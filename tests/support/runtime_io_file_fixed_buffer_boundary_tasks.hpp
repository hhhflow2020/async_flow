#pragma once

class FixedBufferBoundaryTask final : public IoTaskBase {
public:
    explicit FixedBufferBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoFile<IoTestThread> file(IoTestThreads::IO_0, -1);
        af::IoOpState zero{};
        af::IoOpState bad{};
        char value = 0;
        iovec buffer{&value, sizeof(value)};

        int register_error = 0;
        const bool registered =
            IoRuntime::io_register_buffers(IoTestThreads::IO_0, &buffer, 1, &register_error);
        if (registered || register_error != ENOSYS) {
            return failed();
        }

        const af::IoStatus zero_read = file.read_fixed_at(*this, nullptr, 0, 0, 0, zero);
        const af::IoStatus bad_read = file.read_fixed_at(*this, &value, sizeof(value), 0, 0, bad);
        if (!zero_read.ready() || zero_read.bytes != 0U || !bad_read.failed()) {
            return failed();
        }

        error_->store(bad_read.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *error_{nullptr};
};
