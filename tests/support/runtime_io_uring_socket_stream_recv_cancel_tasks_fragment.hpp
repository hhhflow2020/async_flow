#if !defined(AF_RUNTIME_IO_URING_SOCKET_STREAM_TASKS_FRAGMENT_INCLUDE)
#error "runtime_io_uring_socket_stream_recv_cancel_tasks_fragment.hpp is a runtime_io_uring_socket_stream_tasks implementation fragment"
#endif

class UringSelfCancelRecvCompletionTask final : public UringIoTaskBase {
public:
    explicit UringSelfCancelRecvCompletionTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* wait_kind,
        std::atomic<int>* cancel_result,
        std::atomic<int>* immediate_pending,
        std::atomic<int>* completed,
        std::atomic<int>* error) {
        stream_.reset(IoTestThread::IO_0, fd);
        wait_kind_ = wait_kind;
        cancel_result_ = cancel_result;
        immediate_pending_ = immediate_pending;
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Cancel,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Arm:
            return arm_recv();
        case State::Cancel:
            return cancel_recv();
        case State::Finish:
            return finish_recv();
        }
        return complete(EIO);
    }

    af::TaskResult arm_recv() {
        const af::IoStatus status = stream_.recv_some(*this, &value_, sizeof(value_), recv_);
        if (!status.pending()) {
            return complete(status.failed() ? status.error : EIO);
        }

        wait_kind_->store(static_cast<int>(recv_.wait_kind), std::memory_order_release);
        state_ = State::Cancel;
        if (!schedule(IoTestThread::IO_0)) {
            return complete(EIO);
        }
        return pending();
    }

    af::TaskResult cancel_recv() {
        if (recv_.wait_kind != af::IoWaitKind::Completion) {
            return complete(ENOSYS);
        }

        const bool cancelled = UringIoRuntime::cancel_io(IoTestThread::IO_0, recv_);
        cancel_result_->store(cancelled ? 1 : 0, std::memory_order_release);
        if (!cancelled) {
            return complete(recv_.wait.error == 0 ? EIO : recv_.wait.error);
        }

        const af::IoStatus status = stream_.recv_some(*this, &value_, sizeof(value_), recv_);
        if (!status.pending()) {
            immediate_pending_->store(0, std::memory_order_release);
            return complete(status.failed() ? status.error : EIO);
        }

        immediate_pending_->store(1, std::memory_order_release);
        state_ = State::Finish;
        return pending();
    }

    af::TaskResult finish_recv() {
        const af::IoStatus status = stream_.recv_some(*this, &value_, sizeof(value_), recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.failed() || status.error != ECANCELED) {
            return complete(status.failed() ? status.error : EIO);
        }
        return complete(ECANCELED);
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Arm};
    af::TcpStream<IoTestThread> stream_{};
    char value_{0};
    af::IoOpState recv_{};
    std::atomic<int>* wait_kind_{nullptr};
    std::atomic<int>* cancel_result_{nullptr};
    std::atomic<int>* immediate_pending_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};
