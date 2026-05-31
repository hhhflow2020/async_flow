#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_uring_socket_accept_basic_tasks_fragment.hpp is a runtime_io_uring_socket_accept_tasks implementation fragment"
#endif

class UringTcpAcceptTask final : public UringIoTaskBase {
public:
    explicit UringTcpAcceptTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* armed, std::atomic<int>* completed) {
        listener_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        peer_size_ = sizeof(peer_);
        const af::IoStatus status = listener_.accept_some(
            *this,
            reinterpret_cast<sockaddr*>(&peer_),
            &peer_size_,
            &accepted_fd_,
            accept_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || accepted_fd_ < 0 || peer_size_ == 0U) {
            return failed();
        }
        ::close(accepted_fd_);
        accepted_fd_ = -1;
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpListener<IoTestThread> listener_{};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    int accepted_fd_{-1};
    af::IoOpState accept_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
};
