#pragma once

template <typename TaskBaseT>
class BasicUdpVectoredRecvTask final : public TaskBaseT {
public:
    explicit BasicUdpVectoredRecvTask(typename TaskBaseT::FactoryToken token) : TaskBaseT(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* payload_seen) {
        socket_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        payload_seen_ = payload_seen;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        peer_size_ = sizeof(peer_);
        iov_[0] = iovec{&payload_[0], 1};
        iov_[1] = iovec{&payload_[1], 1};
        const af::IoStatus status = socket_.recvv_from_some(
            *this,
            iov_,
            2,
            reinterpret_cast<sockaddr*>(&peer_),
            &peer_size_,
            recv_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return this->pending();
        }
        if (!status.ready() || status.bytes != sizeof(payload_) || peer_size_ == 0U) {
            return this->failed();
        }

        const int combined =
            (static_cast<unsigned char>(payload_[0]) << 8) |
            static_cast<unsigned char>(payload_[1]);
        payload_seen_->store(combined, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::UdpSocket<IoTestThread> socket_{};
    char payload_[2]{};
    iovec iov_[2]{};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* payload_seen_{nullptr};
};

template <typename TaskBaseT, bool ZeroCopy = false>
class BasicUdpVectoredSendToTask final : public TaskBaseT {
public:
    explicit BasicUdpVectoredSendToTask(typename TaskBaseT::FactoryToken token) : TaskBaseT(token) {}

    bool do_it(
        int fd,
        sockaddr_in address,
        socklen_t address_size,
        char first,
        char second,
        std::atomic<int>* completed,
        std::atomic<int>* bytes_sent) {
        socket_.reset(IoTestThread::IO_0, fd);
        address_ = address;
        address_size_ = address_size;
        payload_[0] = first;
        payload_[1] = second;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        iov_[0] = iovec{&payload_[0], 1};
        iov_[1] = iovec{&payload_[1], 1};
        const af::IoStatus status = [&]() {
            if constexpr (ZeroCopy) {
                return socket_.sendv_zc_to_some(
                    *this,
                    iov_,
                    2,
                    reinterpret_cast<const sockaddr*>(&address_),
                    address_size_,
                    send_);
            } else {
                return socket_.sendv_to_some(
                    *this,
                    iov_,
                    2,
                    reinterpret_cast<const sockaddr*>(&address_),
                    address_size_,
                    send_);
            }
        }();
        if (status.pending()) {
            return this->pending();
        }
        if (!status.ready() || status.bytes != sizeof(payload_)) {
            return this->failed();
        }

        bytes_sent_->store(static_cast<int>(status.bytes), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::UdpSocket<IoTestThread> socket_{};
    sockaddr_in address_{};
    socklen_t address_size_{sizeof(address_)};
    char payload_[2]{};
    iovec iov_[2]{};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* bytes_sent_{nullptr};
};

using UdpVectoredRecvTask = BasicUdpVectoredRecvTask<IoTaskBase>;
using UdpVectoredSendToTask = BasicUdpVectoredSendToTask<IoTaskBase>;
using UdpVectoredZcSendToTask = BasicUdpVectoredSendToTask<IoTaskBase, true>;
using UringUdpVectoredRecvTask = BasicUdpVectoredRecvTask<UringIoTaskBase>;
using UringUdpVectoredSendToTask = BasicUdpVectoredSendToTask<UringIoTaskBase>;
using UringUdpVectoredZcSendToTask = BasicUdpVectoredSendToTask<UringIoTaskBase, true>;
