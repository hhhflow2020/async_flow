#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_timer_event_boundary_tasks_fragment.hpp is a runtime_io_test_support implementation fragment"
#endif

class TimerBoundaryTask final : public IoTaskBase {
public:
    explicit TimerBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoTimer<IoTestThread> timer(IoTestThread::IO_0, -1);
        af::IoOpState null_state{};
        af::IoOpState bad_fd_state{};
        std::uint64_t expirations = 0;
        const af::IoStatus null_status = timer.wait(*this, nullptr, null_state);
        const af::IoStatus bad_fd_status = timer.wait(*this, &expirations, bad_fd_state);
        if (!null_status.failed() || null_status.error != EINVAL ||
            !bad_fd_status.failed() || bad_fd_status.error != EBADF) {
            return failed();
        }
        error_->store(bad_fd_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class TimeoutBoundaryTask final : public IoTaskBase {
public:
    explicit TimeoutBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState invalid_delay{};
        af::IoOpState no_uring{};
        const af::IoStatus invalid_status = af::io_wait_timeout(
            *this,
            IoTestThread::IO_0,
            std::chrono::nanoseconds{0},
            invalid_delay);
        const af::IoStatus no_uring_status = af::io_wait_timeout(
            *this,
            IoTestThread::IO_0,
            std::chrono::milliseconds(1),
            no_uring);
        if (!invalid_status.failed() || invalid_status.error != EINVAL ||
            !no_uring_status.failed() || no_uring_status.error != ENOSYS) {
            return failed();
        }
        error_->store(no_uring_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class EventBoundaryTask final : public IoTaskBase {
public:
    explicit EventBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoEvent<IoTestThread> event(IoTestThread::IO_0, -1);
        af::IoOpState null_state{};
        af::IoOpState bad_fd_state{};
        std::uint64_t value = 0;
        const af::IoStatus null_status = event.wait(*this, nullptr, null_state);
        const af::IoStatus bad_fd_status = event.wait(*this, &value, bad_fd_state);
        if (!null_status.failed() || null_status.error != EINVAL ||
            !bad_fd_status.failed() || bad_fd_status.error != EBADF) {
            return failed();
        }
        error_->store(bad_fd_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};
