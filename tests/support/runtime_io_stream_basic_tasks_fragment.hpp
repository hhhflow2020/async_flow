#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_stream_basic_tasks_fragment.hpp is a runtime_io_test_support implementation fragment"
#endif

class StreamAdapterEchoTask final : public IoTaskBase {
public:
    explicit StreamAdapterEchoTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read) {
        stream_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        ReadRequest,
        SendResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::ReadRequest:
            return read_request();

        case State::SendResponse:
            return send_response();
        }
        return failed();
    }

    af::TaskResult read_request() {
        const af::IoStatus status = stream_.recv_some(
            *this,
            &request_,
            sizeof(request_),
            read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }
        byte_read_->store(request_, std::memory_order_release);
        state_ = State::SendResponse;
        return again();
    }

    af::TaskResult send_response() {
        const af::IoStatus status = stream_.send_some(
            *this,
            &response_,
            sizeof(response_),
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::ReadRequest};
    af::TcpStream<IoTestThread> stream_{};
    char request_{0};
    char response_{'R'};
    af::IoOpState read_{};
    af::IoOpState write_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

template <typename BaseTask>
class BasicStreamShutdownTask final : public BaseTask {
public:
    explicit BasicStreamShutdownTask(typename BaseTask::FactoryToken token) : BaseTask(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<int>* error) {
        stream_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        error_ = error;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return shutdown_write();
    }

    af::TaskResult shutdown_write() {
        const af::IoStatus status = stream_.shutdown(*this, SHUT_WR, shutdown_);
        if (status.pending()) {
            return this->pending();
        }
        error_->store(status.ready() ? 0 : status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::TcpStream<IoTestThread> stream_{};
    af::IoOpState shutdown_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

using StreamShutdownTask = BasicStreamShutdownTask<IoTaskBase>;
using UringStreamShutdownTask = BasicStreamShutdownTask<UringIoTaskBase>;
