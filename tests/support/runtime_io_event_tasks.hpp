#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_io_event_tasks.hpp is a runtime_io_test_support implementation detail"
#endif

template <typename TaskBaseT>
class BasicEventFdTask final : public TaskBaseT {
public:
    explicit BasicEventFdTask(typename TaskBaseT::FactoryToken token) : TaskBaseT(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<std::uint64_t>* value) {
        event_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        value_ = value;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        std::uint64_t counter = 0;
        const af::IoStatus status = event_.wait(*this, &counter, wait_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return this->pending();
        }
        if (!status.ready() || status.bytes != sizeof(counter) || counter == 0U) {
            return this->failed();
        }
        value_->store(counter, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::IoEvent<IoTestThread> event_{};
    af::IoOpState wait_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint64_t>* value_{nullptr};
};

using EventFdTask = BasicEventFdTask<IoTaskBase>;
using UringEventFdTask = BasicEventFdTask<UringIoTaskBase>;
