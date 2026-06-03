#pragma once

#include <atomic>

#include "io_vectored_runtime.hpp"
#include "io_vectored_socket_helpers.hpp"

namespace io_vectored_example {

class DatagramReceiverTask final : public VectoredTask {
public:
    explicit DatagramReceiverTask(VectoredTask::FactoryToken token) : VectoredTask(token) {}

    bool do_it(int fd, std::atomic<int> *armed, bool *ok, int *payload_seen) {
        socket_.reset(VectoredThreads::IO_0, fd);
        armed_ = armed;
        ok_ = ok;
        payload_seen_ = payload_seen;
        return schedule(VectoredThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        peer_size_ = sizeof(peer_);
        iov_[0] = iovec{&payload_[0], 1};
        iov_[1] = iovec{&payload_[1], 1};
        const af::IoStatus status = socket_.recvv_from_some(
            *this, iov_, 2, reinterpret_cast<sockaddr *>(&peer_), &peer_size_, read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(payload_)) {
            return failed();
        }

        const int combined = (static_cast<unsigned char>(payload_[0]) << 8) |
                             static_cast<unsigned char>(payload_[1]);
        *payload_seen_ = combined;
        *ok_ = true;
        return done();
    }

    af::UdpSocket<VectoredThread> socket_{};
    char payload_[2]{};
    iovec iov_[2]{};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState read_{};
    std::atomic<int> *armed_{nullptr};
    bool *ok_{nullptr};
    int *payload_seen_{nullptr};
};

class DatagramSenderTask final : public VectoredTask {
public:
    explicit DatagramSenderTask(VectoredTask::FactoryToken token) : VectoredTask(token) {}

    bool do_it(int fd, const VectoredUdpEndpoint &endpoint, bool *ok, int *bytes_sent) {
        socket_.reset(VectoredThreads::IO_0, fd);
        endpoint_ = endpoint;
        ok_ = ok;
        bytes_sent_ = bytes_sent;
        return schedule(VectoredThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        iov_[0] = iovec{&payload_[0], 1};
        iov_[1] = iovec{&payload_[1], 1};
        const af::IoStatus status = socket_.sendv_to_some(
            *this, iov_, 2, reinterpret_cast<const sockaddr *>(&endpoint_.address),
            endpoint_.address_size, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(payload_)) {
            return failed();
        }

        *bytes_sent_ = static_cast<int>(status.bytes);
        *ok_ = true;
        return done();
    }

    af::UdpSocket<VectoredThread> socket_{};
    VectoredUdpEndpoint endpoint_{};
    char payload_[2]{'U', 'D'};
    iovec iov_[2]{};
    af::IoOpState write_{};
    bool *ok_{nullptr};
    int *bytes_sent_{nullptr};
};

} // namespace io_vectored_example
