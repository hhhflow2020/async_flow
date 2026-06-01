#pragma once

class TcpAcceptTask final : public IoTaskBase {
public:
    explicit TcpAcceptTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *armed, std::atomic<int> *completed) {
        listener_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        peer_size_ = sizeof(peer_);
        const af::IoStatus status = listener_.accept_some(
            *this, reinterpret_cast<sockaddr *>(&peer_), &peer_size_, &accepted_fd_, accept_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || accepted_fd_ < 0 || peer_size_ == 0U) {
            return failed();
        }
        af::TcpStream<IoTestThread> accepted(IoTestThread::IO_0, accepted_fd_);
        sockaddr_storage observed_peer{};
        socklen_t observed_peer_size = sizeof(observed_peer);
        const af::IoStatus peer_status = accepted.getpeername(
            *this, reinterpret_cast<sockaddr *>(&observed_peer), &observed_peer_size);
        if (!peer_status.ready() || observed_peer_size == 0U) {
            ::close(accepted_fd_);
            accepted_fd_ = -1;
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
    std::atomic<int> *armed_{nullptr};
    std::atomic<int> *completed_{nullptr};
};
