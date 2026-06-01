#pragma once

#include <cerrno>
#include <cstdint>

#include "io_adapters_results.hpp"
#include "../app_runtime.hpp"

#if defined(__linux__)
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_adapters_example {

class UdpReceiveTask final : public Task {
public:
    explicit UdpReceiveTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, UdpReceiveResult *result) {
        if (fd < 0 || result == nullptr) {
            return false;
        }
        socket_.reset(AppThreads::IO_0, fd);
        result_ = result;
        return schedule(AppThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        peer_size_ = sizeof(peer_);
        const af::IoStatus status =
            socket_.recv_from_some(*this, &result_->value, sizeof(result_->value),
                                   reinterpret_cast<sockaddr *>(&peer_), &peer_size_, recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(result_->value)) {
            return finish(status.failed() ? status.error : EIO);
        }

        result_->ok = result_->value == expected_value;
        result_->error = result_->ok ? 0 : EIO;
        return result_->ok ? done() : failed();
    }

    af::TaskResult finish(int error) {
        result_->error = error == 0 ? EIO : error;
        result_->ok = false;
        return failed();
    }

    static constexpr char expected_value = 'U';

    af::UdpSocket<AppThread> socket_{};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState recv_{};
    UdpReceiveResult *result_{nullptr};
};

class UdpSendTask final : public Task {
public:
    explicit UdpSendTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, sockaddr_in address, socklen_t address_size, UdpSendResult *result) {
        if (fd < 0 || address_size == 0U || result == nullptr) {
            return false;
        }
        socket_.reset(AppThreads::IO_0, fd);
        address_ = address;
        address_size_ = address_size;
        result_ = result;
        return schedule(AppThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = socket_.send_to_some(
            *this, &value_, sizeof(value_), reinterpret_cast<const sockaddr *>(&address_),
            address_size_, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return finish(status.failed() ? status.error : EIO);
        }

        result_->value = value_;
        result_->error = 0;
        result_->ok = true;
        return done();
    }

    af::TaskResult finish(int error) {
        result_->error = error == 0 ? EIO : error;
        result_->ok = false;
        return failed();
    }

    af::UdpSocket<AppThread> socket_{};
    sockaddr_in address_{};
    socklen_t address_size_{sizeof(address_)};
    char value_{'U'};
    af::IoOpState send_{};
    UdpSendResult *result_{nullptr};
};

} // namespace io_adapters_example

#endif
