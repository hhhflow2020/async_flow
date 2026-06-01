#pragma once

class TcpAcceptMultishotBoundaryTask final : public IoTaskBase {
public:
    explicit TcpAcceptMultishotBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *completed, std::atomic<int> *invalid_error,
               std::atomic<int> *null_error, std::atomic<int> *address_error,
               std::atomic<int> *unavailable_error) {
        listener_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        invalid_error_ = invalid_error;
        null_error_ = null_error;
        address_error_ = address_error;
        unavailable_error_ = unavailable_error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::TcpListener<IoTestThread> invalid_listener(IoTestThread::IO_0, -1);
        af::IoOpState invalid_state{};
        af::IoOpState null_state{};
        af::IoOpState address_state{};
        af::IoOpState unavailable_state{};
        sockaddr_storage peer{};
        socklen_t peer_size = sizeof(peer);
        int accepted = -1;

        const af::IoStatus invalid_status =
            invalid_listener.accept_multishot(*this, nullptr, nullptr, &accepted, invalid_state);
        const af::IoStatus null_status =
            listener_.accept_multishot(*this, nullptr, nullptr, nullptr, null_state);
        const af::IoStatus address_status = listener_.accept_multishot(
            *this, reinterpret_cast<sockaddr *>(&peer), &peer_size, &accepted, address_state);
        const af::IoStatus unavailable_status =
            listener_.accept_multishot(*this, nullptr, nullptr, &accepted, unavailable_state);
        if (!invalid_status.failed() || invalid_status.error != EBADF || !null_status.failed() ||
            null_status.error != EINVAL || !address_status.failed() ||
            address_status.error != EINVAL || !unavailable_status.failed() ||
            unavailable_status.error != ENOSYS) {
            return failed();
        }

        invalid_error_->store(invalid_status.error, std::memory_order_release);
        null_error_->store(null_status.error, std::memory_order_release);
        address_error_->store(address_status.error, std::memory_order_release);
        unavailable_error_->store(unavailable_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpListener<IoTestThread> listener_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *invalid_error_{nullptr};
    std::atomic<int> *null_error_{nullptr};
    std::atomic<int> *address_error_{nullptr};
    std::atomic<int> *unavailable_error_{nullptr};
};
