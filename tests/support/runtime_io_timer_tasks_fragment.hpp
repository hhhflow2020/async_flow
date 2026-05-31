#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_timer_tasks_fragment.hpp is a runtime_io_test_support implementation fragment"
#endif

template <typename TaskBaseT>
class BasicTimerFdTask final : public TaskBaseT {
public:
    explicit BasicTimerFdTask(typename TaskBaseT::FactoryToken token) : TaskBaseT(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<std::uint64_t>* expirations) {
        timer_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        expirations_ = expirations;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        std::uint64_t count = 0;
        const af::IoStatus status = timer_.wait(*this, &count, wait_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return this->pending();
        }
        if (!status.ready() || status.bytes != sizeof(count) || count == 0U) {
            return this->failed();
        }
        expirations_->store(count, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::IoTimer<IoTestThread> timer_{};
    af::IoOpState wait_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint64_t>* expirations_{nullptr};
};

using TimerFdTask = BasicTimerFdTask<IoTaskBase>;
using UringTimerFdTask = BasicTimerFdTask<UringIoTaskBase>;

template <typename TaskBaseT>
class BasicUringTimeoutTask final : public TaskBaseT {
public:
    explicit BasicUringTimeoutTask(typename TaskBaseT::FactoryToken token) : TaskBaseT(token) {}

    bool do_it(
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* error) {
        armed_ = armed;
        completed_ = completed;
        error_ = error;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = af::io_wait_timeout(
            *this,
            IoTestThread::IO_0,
            std::chrono::milliseconds(1),
            wait_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return this->pending();
        }
        error_->store(status.failed() ? status.error : 0, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::IoOpState wait_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

using UringTimeoutTask = BasicUringTimeoutTask<UringIoTaskBase>;
