#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <limits>
#include <thread>
#include <type_traits>

#include <gtest/gtest.h>

#include "af/async_flow.hpp"

#if !defined(_WIN32)
#include <sys/uio.h>
#endif

#if defined(__linux__)
#include <fcntl.h>
#include <linux/openat2.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {

#define AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE 1
#include "support/runtime_io_core_fragment.hpp"
#undef AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE

#define AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE 1
#include "support/runtime_io_basic_tasks_fragment.hpp"
#undef AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE

#if defined(__linux__)
#define AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE 1
#include "support/runtime_io_basic_socket_tasks_fragment.hpp"
#undef AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE

class TcpAcceptTask final : public IoTaskBase {
public:
    explicit TcpAcceptTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

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
        af::TcpStream<IoTestThread> accepted(IoTestThread::IO_0, accepted_fd_);
        sockaddr_storage observed_peer{};
        socklen_t observed_peer_size = sizeof(observed_peer);
        const af::IoStatus peer_status = accepted.getpeername(
            *this,
            reinterpret_cast<sockaddr*>(&observed_peer),
            &observed_peer_size);
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
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
};

class TcpAcceptMultishotBoundaryTask final : public IoTaskBase {
public:
    explicit TcpAcceptMultishotBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* completed,
        std::atomic<int>* invalid_error,
        std::atomic<int>* null_error,
        std::atomic<int>* address_error,
        std::atomic<int>* unavailable_error) {
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
            *this,
            reinterpret_cast<sockaddr*>(&peer),
            &peer_size,
            &accepted,
            address_state);
        const af::IoStatus unavailable_status =
            listener_.accept_multishot(*this, nullptr, nullptr, &accepted, unavailable_state);
        if (!invalid_status.failed() || invalid_status.error != EBADF ||
            !null_status.failed() || null_status.error != EINVAL ||
            !address_status.failed() || address_status.error != EINVAL ||
            !unavailable_status.failed() || unavailable_status.error != ENOSYS) {
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
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* invalid_error_{nullptr};
    std::atomic<int>* null_error_{nullptr};
    std::atomic<int>* address_error_{nullptr};
    std::atomic<int>* unavailable_error_{nullptr};
};

#if defined(__linux__)
class RecvMultishotBoundaryTask final : public IoTaskBase {
public:
    explicit RecvMultishotBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* completed,
        std::atomic<int>* invalid_error,
        std::atomic<int>* null_error,
        std::atomic<int>* unavailable_error,
        std::atomic<int>* register_error) {
        stream_.reset(IoTestThread::IO_0, fd);
        datagram_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        invalid_error_ = invalid_error;
        null_error_ = null_error;
        unavailable_error_ = unavailable_error;
        register_error_ = register_error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        int init_error = 0;
        af::IoProvidedBufferRing ring;
        if (ring.init(3, init_error) || init_error != EINVAL) {
            return failed();
        }
        if (!ring.init(2, init_error)) {
            return failed();
        }
        char first = 0;
        char second = 0;
        const af::IoProvidedBuffer buffers[] = {
            af::IoProvidedBuffer{&first, sizeof(first), 0},
            af::IoProvidedBuffer{&second, sizeof(second), 1},
        };
        int add_error = 0;
        if (!ring.add(buffers, 2, add_error)) {
            return failed();
        }

        int null_register_error = 0;
        const bool null_registered = IoRuntime::io_register_provided_buffer_ring(
            IoTestThread::IO_0,
            nullptr,
            2,
            0,
            &null_register_error);
        int register_error = 0;
        const bool registered = IoRuntime::io_register_provided_buffer_ring(
            IoTestThread::IO_0,
            ring.ring(),
            ring.entries(),
            0,
            &register_error);
        if (null_registered || null_register_error != EINVAL ||
            registered || register_error != ENOSYS) {
            return failed();
        }

        af::TcpStream<IoTestThread> invalid_stream(IoTestThread::IO_0, -1);
        af::UdpSocket<IoTestThread> invalid_datagram(IoTestThread::IO_0, -1);
        af::IoOpState invalid_state{};
        af::IoOpState invalid_datagram_state{};
        af::IoOpState invalid_recvmsg_datagram_state{};
        af::IoOpState null_state{};
        af::IoOpState null_datagram_state{};
        af::IoOpState null_recvmsg_datagram_state{};
        af::IoOpState unavailable_state{};
        af::IoOpState unavailable_datagram_state{};
        af::IoOpState unavailable_recvmsg_datagram_state{};
        std::uint16_t buffer_id = 0;
        const af::IoStatus invalid_status =
            invalid_stream.recv_multishot(*this, 0, &buffer_id, invalid_state);
        const af::IoStatus invalid_datagram_status =
            invalid_datagram.recv_multishot(*this, 0, &buffer_id, invalid_datagram_state);
        const af::IoStatus invalid_recvmsg_datagram_status =
            invalid_datagram.recv_from_multishot(
                *this,
                0,
                sizeof(sockaddr_storage),
                0,
                &buffer_id,
                invalid_recvmsg_datagram_state);
        const af::IoStatus null_status =
            stream_.recv_multishot(*this, 0, nullptr, null_state);
        const af::IoStatus null_datagram_status =
            datagram_.recv_multishot(*this, 0, nullptr, null_datagram_state);
        const af::IoStatus null_recvmsg_datagram_status =
            datagram_.recv_from_multishot(
                *this,
                0,
                sizeof(sockaddr_storage),
                0,
                nullptr,
                null_recvmsg_datagram_state);
        const af::IoStatus unavailable_status =
            stream_.recv_multishot(*this, 0, &buffer_id, unavailable_state);
        const af::IoStatus unavailable_datagram_status =
            datagram_.recv_multishot(*this, 0, &buffer_id, unavailable_datagram_state);
        const af::IoStatus unavailable_recvmsg_datagram_status =
            datagram_.recv_from_multishot(
                *this,
                0,
                sizeof(sockaddr_storage),
                0,
                &buffer_id,
                unavailable_recvmsg_datagram_state);
        if (!invalid_status.failed() || invalid_status.error != EBADF ||
            !invalid_datagram_status.failed() || invalid_datagram_status.error != EBADF ||
            !invalid_recvmsg_datagram_status.failed() ||
            invalid_recvmsg_datagram_status.error != EBADF ||
            !null_status.failed() || null_status.error != EINVAL ||
            !null_datagram_status.failed() || null_datagram_status.error != EINVAL ||
            !null_recvmsg_datagram_status.failed() ||
            null_recvmsg_datagram_status.error != EINVAL ||
            !unavailable_status.failed() || unavailable_status.error != ENOSYS) {
            return failed();
        }
        if (!unavailable_datagram_status.failed() ||
            unavailable_datagram_status.error != ENOSYS ||
            !unavailable_recvmsg_datagram_status.failed() ||
            unavailable_recvmsg_datagram_status.error != ENOSYS) {
            return failed();
        }

#if defined(__linux__)
        alignas(af::detail::IoUringRecvmsgOut) char raw_buffer[256]{};
        auto* raw_header = reinterpret_cast<af::detail::IoUringRecvmsgOut*>(raw_buffer);
        raw_header->namelen = sizeof(sockaddr_in);
        raw_header->controllen = 0;
        raw_header->payloadlen = 1;
        raw_header->flags = 0;
        af::IoRecvmsgMultishotView view{};
        int parse_error = 0;
        constexpr socklen_t name_capacity = sizeof(sockaddr_storage);
        constexpr std::size_t received_size =
            sizeof(af::detail::IoUringRecvmsgOut) + name_capacity + 1U;
        if (!af::io_parse_recvmsg_multishot_buffer(
                raw_buffer,
                sizeof(raw_buffer),
                received_size,
                name_capacity,
                0,
                view,
                parse_error) ||
            view.name_offset != sizeof(af::detail::IoUringRecvmsgOut) ||
            view.name_size != sizeof(sockaddr_in) ||
            view.payload_offset != received_size - 1U ||
            view.payload_size != 1U) {
            return failed();
        }
        if (af::io_parse_recvmsg_multishot_buffer(
                nullptr,
                sizeof(raw_buffer),
                received_size,
                name_capacity,
                0,
                view,
                parse_error) ||
            parse_error != EINVAL) {
            return failed();
        }
#endif

        invalid_error_->store(invalid_status.error, std::memory_order_release);
        null_error_->store(null_status.error, std::memory_order_release);
        unavailable_error_->store(unavailable_status.error, std::memory_order_release);
        register_error_->store(register_error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    af::UdpSocket<IoTestThread> datagram_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* invalid_error_{nullptr};
    std::atomic<int>* null_error_{nullptr};
    std::atomic<int>* unavailable_error_{nullptr};
    std::atomic<int>* register_error_{nullptr};
};
#endif

class TcpConnectTask final : public IoTaskBase {
public:
    explicit TcpConnectTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, sockaddr_in address, socklen_t address_size, std::atomic<int>* completed) {
        stream_.reset(IoTestThread::IO_0, fd);
        address_ = address;
        address_size_ = address_size;
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.connect(
            *this,
            reinterpret_cast<const sockaddr*>(&address_),
            address_size_,
            connect_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    sockaddr_in address_{};
    socklen_t address_size_{sizeof(address_)};
    af::IoOpState connect_{};
    std::atomic<int>* completed_{nullptr};
};

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

class StreamVectoredEchoTask final : public IoTaskBase {
public:
    explicit StreamVectoredEchoTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* request_seen) {
        stream_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        request_seen_ = request_seen;
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
        iovec request_iov[2]{
            iovec{&request_[0], 1},
            iovec{&request_[1], 1},
        };
        const af::IoStatus status = stream_.recvv_some(*this, request_iov, 2, read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }
        const int combined =
            (static_cast<int>(request_[0]) << 8) | static_cast<unsigned char>(request_[1]);
        request_seen_->store(combined, std::memory_order_release);
        state_ = State::SendResponse;
        return again();
    }

    af::TaskResult send_response() {
        iovec response_iov[2]{
            iovec{&response_[0], 1},
            iovec{&response_[1], 1},
        };
        const af::IoStatus status = stream_.sendv_some(*this, response_iov, 2, write_);
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
    char request_[2]{};
    char response_[2]{'X', 'Y'};
    af::IoOpState read_{};
    af::IoOpState write_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* request_seen_{nullptr};
};

class ZeroCopyBoundaryTask final : public IoTaskBase {
public:
    explicit ZeroCopyBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState sendfile_zero{};
        af::IoOpState sendfile_bad{};
        af::IoOpState send_zc_zero{};
        af::IoOpState send_zc_null{};
        af::IoOpState send_zc_bad{};
        af::IoOpState sendv_zc_zero{};
        af::IoOpState sendv_zc_null{};
        af::IoOpState sendv_zc_bad{};
        af::IoOpState send_zc_to_zero{};
        af::IoOpState send_zc_to_null{};
        af::IoOpState send_zc_to_bad{};
        af::IoOpState sendv_zc_to_zero{};
        af::IoOpState sendv_zc_to_bad{};
        af::IoOpState splice_zero{};
        af::IoOpState splice_bad{};
        af::IoOffset offset = 0;
        const char value = 'Z';
        iovec valid_iov{const_cast<char*>(&value), 1};

        const af::IoStatus sendfile_zero_status = af::io_sendfile_some(
            *this,
            IoTestThread::IO_0,
            -1,
            -1,
            nullptr,
            0,
            sendfile_zero);
        const af::IoStatus sendfile_bad_status = af::io_sendfile_some(
            *this,
            IoTestThread::IO_0,
            -1,
            -1,
            &offset,
            1,
            sendfile_bad);
        const af::IoStatus send_zc_zero_status = af::io_send_zc_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            0,
            send_zc_zero);
        const af::IoStatus send_zc_null_status = af::io_send_zc_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            1,
            send_zc_null);
        const af::IoStatus send_zc_bad_status = af::io_send_zc_some(
            *this,
            IoTestThread::IO_0,
            -1,
            &value,
            sizeof(value),
            send_zc_bad);
        const af::IoStatus sendv_zc_zero_status = af::io_sendv_zc_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            0,
            sendv_zc_zero);
        const af::IoStatus sendv_zc_null_status = af::io_sendv_zc_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            1,
            sendv_zc_null);
        const af::IoStatus sendv_zc_bad_status = af::io_sendv_zc_some(
            *this,
            IoTestThread::IO_0,
            -1,
            &valid_iov,
            1,
            sendv_zc_bad);
        const af::IoStatus send_zc_to_zero_status = af::io_send_zc_to_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            0,
            nullptr,
            0,
            send_zc_to_zero);
        const af::IoStatus send_zc_to_null_status = af::io_send_zc_to_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            1,
            nullptr,
            0,
            send_zc_to_null);
        const af::IoStatus send_zc_to_bad_status = af::io_send_zc_to_some(
            *this,
            IoTestThread::IO_0,
            -1,
            &value,
            sizeof(value),
            nullptr,
            0,
            send_zc_to_bad);
        const af::IoStatus sendv_zc_to_zero_status = af::io_sendv_zc_to_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            0,
            nullptr,
            0,
            sendv_zc_to_zero);
        const af::IoStatus sendv_zc_to_bad_status = af::io_sendv_zc_to_some(
            *this,
            IoTestThread::IO_0,
            -1,
            &valid_iov,
            1,
            nullptr,
            0,
            sendv_zc_to_bad);
        const af::IoStatus splice_zero_status = af::io_splice_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            -1,
            nullptr,
            0,
            0,
            splice_zero);
        const af::IoStatus splice_bad_status = af::io_splice_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            -1,
            nullptr,
            1,
            0,
            splice_bad);
        if (!sendfile_zero_status.ready() || sendfile_zero_status.bytes != 0U ||
            !send_zc_zero_status.ready() || send_zc_zero_status.bytes != 0U ||
            !send_zc_null_status.failed() || send_zc_null_status.error != EINVAL ||
            !send_zc_bad_status.failed() || send_zc_bad_status.error != EBADF ||
            !sendv_zc_zero_status.ready() || sendv_zc_zero_status.bytes != 0U ||
            !sendv_zc_null_status.failed() || sendv_zc_null_status.error != EINVAL ||
            !sendv_zc_bad_status.failed() || sendv_zc_bad_status.error != EBADF ||
            !send_zc_to_zero_status.ready() || send_zc_to_zero_status.bytes != 0U ||
            !send_zc_to_null_status.failed() || send_zc_to_null_status.error != EINVAL ||
            !send_zc_to_bad_status.failed() || send_zc_to_bad_status.error != EBADF ||
            !sendv_zc_to_zero_status.ready() || sendv_zc_to_zero_status.bytes != 0U ||
            !sendv_zc_to_bad_status.failed() || sendv_zc_to_bad_status.error != EBADF ||
            !splice_zero_status.ready() || splice_zero_status.bytes != 0U ||
            !sendfile_bad_status.failed() || sendfile_bad_status.error != EBADF ||
            !splice_bad_status.failed() || splice_bad_status.error != EBADF) {
            return failed();
        }
        error_->store(sendfile_bad_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class SendZcSocketTask final : public IoTaskBase {
public:
    explicit SendZcSocketTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        const char* payload,
        std::size_t total_size,
        std::size_t chunk_size,
        std::atomic<int>* completed,
        std::atomic<int>* calls,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        payload_ = payload;
        total_size_ = total_size;
        chunk_size_ = chunk_size == 0U ? total_size : chunk_size;
        completed_ = completed;
        calls_ = calls;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_next();
    }

    af::TaskResult send_next() {
        if (sent_ >= total_size_) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        const std::size_t remaining = total_size_ - sent_;
        const std::size_t count = remaining < chunk_size_ ? remaining : chunk_size_;
        const af::IoStatus status =
            stream_.send_zc_some(*this, payload_ + sent_, count, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > remaining) {
            return failed();
        }

        sent_ += status.bytes;
        calls_->fetch_add(1, std::memory_order_release);
        bytes_sent_->store(sent_, std::memory_order_release);
        return again();
    }

    af::TcpStream<IoTestThread> stream_{};
    const char* payload_{nullptr};
    std::size_t total_size_{0};
    std::size_t chunk_size_{0};
    std::size_t sent_{0};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* calls_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class SendvZcSocketTask final : public IoTaskBase {
public:
    explicit SendvZcSocketTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        const char* first,
        std::size_t first_size,
        const char* second,
        std::size_t second_size,
        std::atomic<int>* completed,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        first_ = first;
        first_size_ = first_size;
        second_ = second;
        second_size_ = second_size;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const std::size_t total_size = first_size_ + second_size_;
        if (sent_ >= total_size) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        int iov_count = 0;
        if (sent_ < first_size_) {
            iov_[iov_count++] = iovec{
                const_cast<char*>(first_ + sent_),
                first_size_ - sent_};
            iov_[iov_count++] = iovec{const_cast<char*>(second_), second_size_};
        } else {
            const std::size_t second_offset = sent_ - first_size_;
            iov_[iov_count++] = iovec{
                const_cast<char*>(second_ + second_offset),
                second_size_ - second_offset};
        }

        const af::IoStatus status = stream_.sendv_zc_some(*this, iov_, iov_count, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > total_size - sent_) {
            return failed();
        }

        sent_ += status.bytes;
        bytes_sent_->store(sent_, std::memory_order_release);
        return again();
    }

    af::TcpStream<IoTestThread> stream_{};
    const char* first_{nullptr};
    const char* second_{nullptr};
    std::size_t first_size_{0};
    std::size_t second_size_{0};
    std::size_t sent_{0};
    iovec iov_[2]{};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class SendfileSocketTask final : public IoTaskBase {
public:
    explicit SendfileSocketTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        int file_fd,
        std::size_t total_size,
        std::size_t chunk_size,
        bool use_null_offset,
        std::atomic<int>* completed,
        std::atomic<int>* calls,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        file_fd_ = file_fd;
        total_size_ = total_size;
        chunk_size_ = chunk_size == 0U ? total_size : chunk_size;
        use_null_offset_ = use_null_offset;
        completed_ = completed;
        calls_ = calls;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_next();
    }

    af::TaskResult send_next() {
        if (sent_ >= total_size_) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        const std::size_t remaining = total_size_ - sent_;
        const std::size_t count = remaining < chunk_size_ ? remaining : chunk_size_;
        af::IoOffset* offset = use_null_offset_ ? nullptr : &offset_;
        const af::IoStatus status = stream_.sendfile_some(*this, file_fd_, offset, count, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > remaining) {
            return failed();
        }
        sent_ += status.bytes;
        calls_->fetch_add(1, std::memory_order_release);
        bytes_sent_->store(sent_, std::memory_order_release);
        return again();
    }

    af::TcpStream<IoTestThread> stream_{};
    int file_fd_{-1};
    af::IoOffset offset_{0};
    std::size_t total_size_{0};
    std::size_t chunk_size_{0};
    std::size_t sent_{0};
    bool use_null_offset_{false};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* calls_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class PendingSendZcTask final : public IoTaskBase {
public:
    explicit PendingSendZcTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        std::atomic<int>* pending_seen,
        std::atomic<int>* completed,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        pending_seen_ = pending_seen;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_byte();
    }

    af::TaskResult send_byte() {
        const af::IoStatus status = stream_.send_zc_some(*this, &value_, sizeof(value_), send_);
        if (status.pending()) {
            pending_seen_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        bytes_sent_->store(status.bytes, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{'Z'};
    af::IoOpState send_{};
    std::atomic<int>* pending_seen_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class PendingSendfileTask final : public IoTaskBase {
public:
    explicit PendingSendfileTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        int file_fd,
        std::atomic<int>* pending_seen,
        std::atomic<int>* completed,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        file_fd_ = file_fd;
        pending_seen_ = pending_seen;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_byte();
    }

    af::TaskResult send_byte() {
        const af::IoStatus status = stream_.sendfile_some(*this, file_fd_, &offset_, 1, send_);
        if (status.pending()) {
            pending_seen_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != 1U || offset_ != 1) {
            return failed();
        }
        bytes_sent_->store(status.bytes, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    int file_fd_{-1};
    af::IoOffset offset_{0};
    af::IoOpState send_{};
    std::atomic<int>* pending_seen_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class UringPendingSendfilePollTask final : public UringIoTaskBase {
public:
    explicit UringPendingSendfilePollTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int socket_fd,
        int file_fd,
        std::atomic<af::IoOpState*>* state,
        std::atomic<int>* wait_kind,
        std::atomic<int>* pending_seen,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(IoTestThread::IO_0, socket_fd);
        file_fd_ = file_fd;
        state_ = state;
        wait_kind_ = wait_kind;
        pending_seen_ = pending_seen;
        completed_ = completed;
        error_ = error;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.sendfile_some(*this, file_fd_, &offset_, 1, send_);
        if (status.pending()) {
            state_->store(&send_, std::memory_order_release);
            wait_kind_->store(static_cast<int>(send_.wait_kind), std::memory_order_release);
            pending_seen_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (status.failed()) {
            error_->store(status.error, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (!status.ready() || status.bytes != 1U || offset_ != 1) {
            return failed();
        }
        bytes_sent_->store(status.bytes, std::memory_order_release);
        error_->store(0, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    int file_fd_{-1};
    af::IoOffset offset_{0};
    af::IoOpState send_{};
    std::atomic<af::IoOpState*>* state_{nullptr};
    std::atomic<int>* wait_kind_{nullptr};
    std::atomic<int>* pending_seen_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

class SplicePipeTask final : public IoTaskBase {
public:
    explicit SplicePipeTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int input_fd,
        int output_fd,
        std::size_t total_size,
        std::atomic<int>* completed,
        std::atomic<std::size_t>* bytes_spliced) {
        input_fd_ = input_fd;
        output_fd_ = output_fd;
        total_size_ = total_size;
        completed_ = completed;
        bytes_spliced_ = bytes_spliced;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return splice_next();
    }

    af::TaskResult splice_next() {
        if (spliced_ >= total_size_) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        const af::IoStatus status = af::io_splice_some(
            *this,
            IoTestThread::IO_0,
            input_fd_,
            nullptr,
            output_fd_,
            nullptr,
            total_size_ - spliced_,
            SPLICE_F_NONBLOCK,
            splice_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > total_size_ - spliced_) {
            return failed();
        }
        spliced_ += status.bytes;
        bytes_spliced_->store(spliced_, std::memory_order_release);
        return again();
    }

    int input_fd_{-1};
    int output_fd_{-1};
    std::size_t total_size_{0};
    std::size_t spliced_{0};
    af::IoOpState splice_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::size_t>* bytes_spliced_{nullptr};
};

class FileAdapterBoundaryTask final : public IoTaskBase {
public:
    explicit FileAdapterBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoFile<IoTestThread> file(IoTestThread::IO_0, -1);
        af::IoOpState read{};
        af::IoOpState write{};
        char value = 0;

        const af::IoStatus zero_read = file.read_some(*this, nullptr, 0, read);
        const af::IoStatus zero_write = file.write_some(*this, nullptr, 0, write);
        const af::IoStatus bad_read = file.read_some(*this, &value, sizeof(value), read);
        if (!zero_read.ready() || zero_read.bytes != 0U ||
            !zero_write.ready() || zero_write.bytes != 0U ||
            !bad_read.failed()) {
            return failed();
        }

        error_->store(bad_read.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class FixedBufferBoundaryTask final : public IoTaskBase {
public:
    explicit FixedBufferBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoFile<IoTestThread> file(IoTestThread::IO_0, -1);
        af::IoOpState zero{};
        af::IoOpState bad{};
        char value = 0;
        iovec buffer{&value, sizeof(value)};

        int register_error = 0;
        const bool registered =
            IoRuntime::io_register_buffers(IoTestThread::IO_0, &buffer, 1, &register_error);
        if (registered || register_error != ENOSYS) {
            return failed();
        }

        const af::IoStatus zero_read =
            file.read_fixed_at(*this, nullptr, 0, 0, 0, zero);
        const af::IoStatus bad_read =
            file.read_fixed_at(*this, &value, sizeof(value), 0, 0, bad);
        if (!zero_read.ready() || zero_read.bytes != 0U || !bad_read.failed()) {
            return failed();
        }

        error_->store(bad_read.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class FixedFileBoundaryTask final : public IoTaskBase {
public:
    explicit FixedFileBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        std::atomic<int>* completed,
        std::atomic<int>* register_error,
        std::atomic<int>* unavailable_error,
        std::atomic<int>* invalid_error,
        std::atomic<int>* null_error) {
        completed_ = completed;
        register_error_ = register_error;
        unavailable_error_ = unavailable_error;
        invalid_error_ = invalid_error;
        null_error_ = null_error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const int fd = -1;
        int register_error = 0;
        const bool registered =
            IoRuntime::io_register_files(IoTestThread::IO_0, &fd, 1, &register_error);
        if (registered || register_error != ENOSYS) {
            return failed();
        }
        int update_error = 0;
        const bool updated =
            IoRuntime::io_update_registered_files(IoTestThread::IO_0, 0, &fd, 1, &update_error);
        if (updated || update_error != ENOSYS) {
            return failed();
        }
        int null_update_error = 0;
        const bool null_update = IoRuntime::io_update_registered_files(
            IoTestThread::IO_0,
            0,
            nullptr,
            1,
            &null_update_error);
        if (null_update || null_update_error != EINVAL) {
            return failed();
        }

        af::IoFixedFile<IoTestThread> missing(IoTestThread::IO_0, 0);
        af::IoFixedFile<IoTestThread> invalid(IoTestThread::IO_0, -1);
        af::IoOpState unavailable{};
        af::IoOpState zero{};
        af::IoOpState bad{};
        af::IoOpState null_data{};
        af::IoOpState fixed_unavailable{};
        af::IoOpState fixed_bad{};
        af::IoOpState fixed_null{};
        af::IoOpState direct_null_path{};
        af::IoOpState direct_null_output{};
        af::IoOpState direct_bad_index{};
        af::IoOpState direct_unavailable{};
        af::IoOpState accept_bad_fd{};
        af::IoOpState accept_null_output{};
        af::IoOpState accept_bad_address{};
        af::IoOpState accept_bad_index{};
        af::IoOpState accept_unavailable{};
        af::IoOpState fixed_recv_unavailable{};
        af::IoOpState fixed_recv_zero{};
        af::IoOpState fixed_recv_bad{};
        af::IoOpState fixed_recv_null{};
        af::IoOpState fixed_send_unavailable{};
        af::IoOpState fixed_send_zero{};
        af::IoOpState fixed_send_bad{};
        af::IoOpState fixed_send_null{};
        af::IoOpState fixed_readv_unavailable{};
        af::IoOpState fixed_readv_zero{};
        af::IoOpState fixed_readv_bad{};
        af::IoOpState fixed_readv_null{};
        af::IoOpState fixed_writev_unavailable{};
        af::IoOpState fixed_writev_zero{};
        af::IoOpState fixed_writev_bad{};
        af::IoOpState fixed_writev_null{};
        af::IoOpState fixed_recvv_unavailable{};
        af::IoOpState fixed_recvv_zero{};
        af::IoOpState fixed_recvv_bad{};
        af::IoOpState fixed_recvv_null{};
        af::IoOpState fixed_sendv_unavailable{};
        af::IoOpState fixed_sendv_zero{};
        af::IoOpState fixed_sendv_bad{};
        af::IoOpState fixed_sendv_null{};
        char value = 0;
        af::IoFixedBuffer buffer{&value, sizeof(value), 0};

        const af::IoStatus unavailable_read =
            missing.read_at(*this, &value, sizeof(value), 0, unavailable);
        const af::IoStatus zero_read = invalid.read_at(*this, nullptr, 0, 0, zero);
        const af::IoStatus bad_read = invalid.read_at(*this, &value, sizeof(value), 0, bad);
        const af::IoStatus null_read =
            missing.read_at(*this, nullptr, sizeof(value), 0, null_data);
        const af::IoStatus fixed_unavailable_read =
            missing.read_fixed_at(*this, buffer, 0, fixed_unavailable);
        const af::IoStatus fixed_bad_read =
            invalid.read_fixed_at(*this, buffer, 0, fixed_bad);
        const af::IoStatus fixed_null_read = missing.read_fixed_at(
            *this,
            af::IoFixedBuffer{nullptr, sizeof(value), 0},
            0,
            fixed_null);
        af::IoFixedFile<IoTestThread> direct_file{};
        const af::IoStatus direct_null_path_status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            O_RDONLY | O_CLOEXEC,
            0,
            0,
            &direct_file,
            direct_null_path);
        const af::IoStatus direct_null_output_status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-direct-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            0,
            nullptr,
            direct_null_output);
        const af::IoStatus direct_bad_index_status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-direct-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            -1,
            &direct_file,
            direct_bad_index);
        const af::IoStatus direct_unavailable_status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-direct-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            0,
            &direct_file,
            direct_unavailable);
        af::IoFixedFile<IoTestThread> accepted_direct{};
        sockaddr_storage peer{};
        socklen_t peer_size = sizeof(peer);
        const int placeholder_fd = STDIN_FILENO;
        const af::IoStatus accept_bad_fd_status = af::io_accept_direct(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &accepted_direct,
            accept_bad_fd);
        const af::IoStatus accept_null_output_status = af::io_accept_direct(
            *this,
            IoTestThread::IO_0,
            placeholder_fd,
            nullptr,
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            nullptr,
            accept_null_output);
        const af::IoStatus accept_bad_address_status = af::io_accept_direct(
            *this,
            IoTestThread::IO_0,
            placeholder_fd,
            reinterpret_cast<sockaddr*>(&peer),
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &accepted_direct,
            accept_bad_address);
        const af::IoStatus accept_bad_index_status = af::io_accept_direct(
            *this,
            IoTestThread::IO_0,
            placeholder_fd,
            nullptr,
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC,
            -1,
            &accepted_direct,
            accept_bad_index);
        const af::IoStatus accept_unavailable_status = af::io_accept_direct(
            *this,
            IoTestThread::IO_0,
            placeholder_fd,
            nullptr,
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &accepted_direct,
            accept_unavailable);
        const af::IoStatus fixed_recv_unavailable_status =
            missing.recv_some(*this, &value, sizeof(value), fixed_recv_unavailable);
        const af::IoStatus fixed_recv_zero_status =
            invalid.recv_some(*this, nullptr, 0, fixed_recv_zero);
        const af::IoStatus fixed_recv_bad_status =
            invalid.recv_some(*this, &value, sizeof(value), fixed_recv_bad);
        const af::IoStatus fixed_recv_null_status =
            missing.recv_some(*this, nullptr, sizeof(value), fixed_recv_null);
        const af::IoStatus fixed_send_unavailable_status =
            missing.send_some(*this, &value, sizeof(value), fixed_send_unavailable);
        const af::IoStatus fixed_send_zero_status =
            invalid.send_some(*this, nullptr, 0, fixed_send_zero);
        const af::IoStatus fixed_send_bad_status =
            invalid.send_some(*this, &value, sizeof(value), fixed_send_bad);
        const af::IoStatus fixed_send_null_status =
            missing.send_some(*this, nullptr, sizeof(value), fixed_send_null);
        iovec valid_iov{&value, sizeof(value)};
        iovec invalid_iov{nullptr, sizeof(value)};
        const af::IoStatus fixed_readv_unavailable_status =
            missing.readv_at(*this, &valid_iov, 1, 0, fixed_readv_unavailable);
        const af::IoStatus fixed_readv_zero_status =
            invalid.readv_at(*this, nullptr, 0, 0, fixed_readv_zero);
        const af::IoStatus fixed_readv_bad_status =
            invalid.readv_at(*this, &valid_iov, 1, 0, fixed_readv_bad);
        const af::IoStatus fixed_readv_null_status =
            missing.readv_at(*this, &invalid_iov, 1, 0, fixed_readv_null);
        const af::IoStatus fixed_writev_unavailable_status =
            missing.writev_at(*this, &valid_iov, 1, 0, fixed_writev_unavailable);
        const af::IoStatus fixed_writev_zero_status =
            invalid.writev_at(*this, nullptr, 0, 0, fixed_writev_zero);
        const af::IoStatus fixed_writev_bad_status =
            invalid.writev_at(*this, &valid_iov, 1, 0, fixed_writev_bad);
        const af::IoStatus fixed_writev_null_status =
            missing.writev_at(*this, &invalid_iov, 1, 0, fixed_writev_null);
        const af::IoStatus fixed_recvv_unavailable_status =
            missing.recvv_some(*this, &valid_iov, 1, fixed_recvv_unavailable);
        const af::IoStatus fixed_recvv_zero_status =
            invalid.recvv_some(*this, nullptr, 0, fixed_recvv_zero);
        const af::IoStatus fixed_recvv_bad_status =
            invalid.recvv_some(*this, &valid_iov, 1, fixed_recvv_bad);
        const af::IoStatus fixed_recvv_null_status =
            missing.recvv_some(*this, &invalid_iov, 1, fixed_recvv_null);
        const af::IoStatus fixed_sendv_unavailable_status =
            missing.sendv_some(*this, &valid_iov, 1, fixed_sendv_unavailable);
        const af::IoStatus fixed_sendv_zero_status =
            invalid.sendv_some(*this, nullptr, 0, fixed_sendv_zero);
        const af::IoStatus fixed_sendv_bad_status =
            invalid.sendv_some(*this, &valid_iov, 1, fixed_sendv_bad);
        const af::IoStatus fixed_sendv_null_status =
            missing.sendv_some(*this, &invalid_iov, 1, fixed_sendv_null);
        if (!unavailable_read.failed() || unavailable_read.error != ENOSYS ||
            !zero_read.ready() || zero_read.bytes != 0U ||
            !bad_read.failed() || bad_read.error != EBADF ||
            !null_read.failed() || null_read.error != EINVAL ||
            !fixed_unavailable_read.failed() || fixed_unavailable_read.error != ENOSYS ||
            !fixed_bad_read.failed() || fixed_bad_read.error != EBADF ||
            !fixed_null_read.failed() || fixed_null_read.error != EINVAL ||
            !direct_null_path_status.failed() || direct_null_path_status.error != EINVAL ||
            !direct_null_output_status.failed() || direct_null_output_status.error != EINVAL ||
            !direct_bad_index_status.failed() || direct_bad_index_status.error != EBADF ||
            !direct_unavailable_status.failed() || direct_unavailable_status.error != ENOSYS ||
            !accept_bad_fd_status.failed() || accept_bad_fd_status.error != EBADF ||
            !accept_null_output_status.failed() || accept_null_output_status.error != EINVAL ||
            !accept_bad_address_status.failed() || accept_bad_address_status.error != EINVAL ||
            !accept_bad_index_status.failed() || accept_bad_index_status.error != EBADF ||
            !accept_unavailable_status.failed() || accept_unavailable_status.error != ENOSYS ||
            !fixed_recv_unavailable_status.failed() || fixed_recv_unavailable_status.error != ENOSYS ||
            !fixed_recv_zero_status.ready() || fixed_recv_zero_status.bytes != 0U ||
            !fixed_recv_bad_status.failed() || fixed_recv_bad_status.error != EBADF ||
            !fixed_recv_null_status.failed() || fixed_recv_null_status.error != EINVAL ||
            !fixed_send_unavailable_status.failed() || fixed_send_unavailable_status.error != ENOSYS ||
            !fixed_send_zero_status.ready() || fixed_send_zero_status.bytes != 0U ||
            !fixed_send_bad_status.failed() || fixed_send_bad_status.error != EBADF ||
            !fixed_send_null_status.failed() || fixed_send_null_status.error != EINVAL ||
            !fixed_readv_unavailable_status.failed() || fixed_readv_unavailable_status.error != ENOSYS ||
            !fixed_readv_zero_status.ready() || fixed_readv_zero_status.bytes != 0U ||
            !fixed_readv_bad_status.failed() || fixed_readv_bad_status.error != EBADF ||
            !fixed_readv_null_status.failed() || fixed_readv_null_status.error != EINVAL ||
            !fixed_writev_unavailable_status.failed() || fixed_writev_unavailable_status.error != ENOSYS ||
            !fixed_writev_zero_status.ready() || fixed_writev_zero_status.bytes != 0U ||
            !fixed_writev_bad_status.failed() || fixed_writev_bad_status.error != EBADF ||
            !fixed_writev_null_status.failed() || fixed_writev_null_status.error != EINVAL ||
            !fixed_recvv_unavailable_status.failed() || fixed_recvv_unavailable_status.error != ENOSYS ||
            !fixed_recvv_zero_status.ready() || fixed_recvv_zero_status.bytes != 0U ||
            !fixed_recvv_bad_status.failed() || fixed_recvv_bad_status.error != EBADF ||
            !fixed_recvv_null_status.failed() || fixed_recvv_null_status.error != EINVAL ||
            !fixed_sendv_unavailable_status.failed() || fixed_sendv_unavailable_status.error != ENOSYS ||
            !fixed_sendv_zero_status.ready() || fixed_sendv_zero_status.bytes != 0U ||
            !fixed_sendv_bad_status.failed() || fixed_sendv_bad_status.error != EBADF ||
            !fixed_sendv_null_status.failed() || fixed_sendv_null_status.error != EINVAL ||
            direct_file.valid() || accepted_direct.valid()) {
            return failed();
        }

        register_error_->store(register_error, std::memory_order_release);
        unavailable_error_->store(unavailable_read.error, std::memory_order_release);
        invalid_error_->store(bad_read.error, std::memory_order_release);
        null_error_->store(null_read.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* register_error_{nullptr};
    std::atomic<int>* unavailable_error_{nullptr};
    std::atomic<int>* invalid_error_{nullptr};
    std::atomic<int>* null_error_{nullptr};
};

class UringFileReadWriteTask final : public UringIoTaskBase {
public:
    explicit UringFileReadWriteTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<char>* byte_read) {
        file_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Write,
        Fsync,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();
        }
        return failed();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_value() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_at(*this, &read_, sizeof(read_), 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_)) {
            return failed();
        }
        byte_read_->store(read_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Write};
    af::IoFile<IoTestThread> file_{};
    char value_{'F'};
    char read_{0};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringFileVectoredReadWriteTask final : public UringIoTaskBase {
public:
    explicit UringFileVectoredReadWriteTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<int>* bytes_read) {
        file_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        bytes_read_ = bytes_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Write,
        Fsync,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();
        }
        return failed();
    }

    af::TaskResult write_value() {
        write_iov_[0] = iovec{&first_, 1};
        write_iov_[1] = iovec{&second_, 1};
        const af::IoStatus status = file_.writev_at(*this, write_iov_, 2, 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 2U) {
            return failed();
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_value() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        read_iov_[0] = iovec{&read_[0], 1};
        read_iov_[1] = iovec{&read_[1], 1};
        const af::IoStatus status = file_.readv_at(*this, read_iov_, 2, 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 2U || read_[0] != first_ || read_[1] != second_) {
            return failed();
        }
        bytes_read_->store(static_cast<int>(status.bytes), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Write};
    af::IoFile<IoTestThread> file_{};
    char first_{'V'};
    char second_{'W'};
    char read_[2]{};
    iovec write_iov_[2]{};
    iovec read_iov_[2]{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* bytes_read_{nullptr};
};

class UringFileCurrentOffsetTask final : public UringIoTaskBase {
public:
    explicit UringFileCurrentOffsetTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* completed,
        std::atomic<int>* packed_read,
        std::atomic<int>* pending_submits) {
        file_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        packed_read_ = packed_read;
        pending_submits_ = pending_submits;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        WriteOne,
        WriteVector,
        Fsync,
        SeekStart,
        ReadOne,
        ReadVector,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::WriteOne:
            return write_one();

        case State::WriteVector:
            return write_vector();

        case State::Fsync:
            return fsync_file();

        case State::SeekStart:
            return seek_start();

        case State::ReadOne:
            return read_one();

        case State::ReadVector:
            return read_vector();
        }
        return failed();
    }

    af::TaskResult write_one() {
        const af::IoStatus status = file_.write_some(*this, &first_, sizeof(first_), write_one_);
        if (status.pending()) {
            pending_submits_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(first_)) {
            return failed();
        }
        state_ = State::WriteVector;
        return again();
    }

    af::TaskResult write_vector() {
        write_iov_[0] = iovec{&second_, 1};
        write_iov_[1] = iovec{&third_, 1};
        const af::IoStatus status = file_.writev_some(*this, write_iov_, 2, write_vector_);
        if (status.pending()) {
            pending_submits_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != 2U) {
            return failed();
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_file() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            pending_submits_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::SeekStart;
        return again();
    }

    af::TaskResult seek_start() {
#if defined(__linux__)
        if (::lseek(file_.fd(), 0, SEEK_SET) < 0) {
            return failed();
        }
        state_ = State::ReadOne;
        return again();
#else
        return failed();
#endif
    }

    af::TaskResult read_one() {
        const af::IoStatus status = file_.read_some(*this, &read_first_, sizeof(read_first_), read_one_);
        if (status.pending()) {
            pending_submits_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_first_) || read_first_ != first_) {
            return failed();
        }
        state_ = State::ReadVector;
        return again();
    }

    af::TaskResult read_vector() {
        read_iov_[0] = iovec{&read_rest_[0], 1};
        read_iov_[1] = iovec{&read_rest_[1], 1};
        const af::IoStatus status = file_.readv_some(*this, read_iov_, 2, read_vector_);
        if (status.pending()) {
            pending_submits_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_rest_) ||
            read_rest_[0] != second_ || read_rest_[1] != third_) {
            return failed();
        }
        const int packed =
            (static_cast<int>(static_cast<unsigned char>(read_first_)) << 16) |
            (static_cast<int>(static_cast<unsigned char>(read_rest_[0])) << 8) |
            static_cast<int>(static_cast<unsigned char>(read_rest_[1]));
        packed_read_->store(packed, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::WriteOne};
    af::IoFile<IoTestThread> file_{};
    char first_{'A'};
    char second_{'B'};
    char third_{'C'};
    char read_first_{0};
    char read_rest_[2]{};
    iovec write_iov_[2]{};
    iovec read_iov_[2]{};
    af::IoOpState write_one_{};
    af::IoOpState write_vector_{};
    af::IoOpState fsync_{};
    af::IoOpState read_one_{};
    af::IoOpState read_vector_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* packed_read_{nullptr};
    std::atomic<int>* pending_submits_{nullptr};
};

class UringFixedBufferFileTask final : public UringIoTaskBase {
public:
    explicit UringFixedBufferFileTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read) {
        file_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Write,
        Fsync,
        Read,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_buffer();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::Unregister:
            return unregister_buffer();
        }
        return failed();
    }

    af::TaskResult register_buffer() {
        const af::IoStatus no_buffer = file_.write_fixed_at(
            *this,
            buffer_,
            1,
            0,
            0,
            no_buffer_);
        if (!no_buffer.failed() || no_buffer.error != ENOBUFS) {
            return failed();
        }

        iovec iov{buffer_, sizeof(buffer_)};
        int error = 0;
        if (!UringIoRuntime::io_register_buffers(IoTestThread::IO_0, &iov, 1, &error)) {
            return failed();
        }

        const af::IoStatus bad_index = file_.write_fixed_at(
            *this,
            buffer_,
            1,
            0,
            1,
            bad_index_);
        if (!bad_index.failed() || bad_index.error != EINVAL) {
            return failed();
        }

        buffer_[0] = value_;
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_fixed_at(
            *this,
            af::IoFixedBuffer{buffer_, 1, 0},
            0,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U) {
            return failed();
        }
        buffer_[0] = 0;
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_value() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_fixed_at(
            *this,
            af::IoFixedBuffer{buffer_, 1, 0},
            0,
            read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U || buffer_[0] != value_) {
            return failed();
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_buffer() {
        int error = 0;
        if (!UringIoRuntime::io_unregister_buffers(IoTestThread::IO_0, &error)) {
            return failed();
        }
        byte_read_->store(buffer_[0], std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Register};
    af::IoFile<IoTestThread> file_{};
    alignas(64) char buffer_[64]{};
    char value_{'B'};
    af::IoOpState no_buffer_{};
    af::IoOpState bad_index_{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringFixedFileTask final : public UringIoTaskBase {
public:
    explicit UringFixedFileTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<char>* byte_read) {
        fd_ = fd;
        file_.reset(IoTestThread::IO_0, 0);
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Write,
        WriteVectored,
        Fsync,
        Read,
        ReadVectored,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_file();

        case State::Write:
            return write_value();

        case State::WriteVectored:
            return write_vectored();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::ReadVectored:
            return read_vectored();

        case State::Unregister:
            return unregister_file();
        }
        return failed();
    }

    af::TaskResult register_file() {
        const af::IoStatus no_table = file_.write_at(
            *this,
            &value_,
            sizeof(value_),
            0,
            no_table_);
        if (!no_table.failed() || no_table.error != ENXIO) {
            return failed();
        }

        int error = 0;
        if (!UringIoRuntime::io_register_files(IoTestThread::IO_0, &fd_, 1, &error)) {
            return failed();
        }

        int duplicate_error = 0;
        if (UringIoRuntime::io_register_files(
                IoTestThread::IO_0,
                &fd_,
                1,
                &duplicate_error) ||
            duplicate_error != EALREADY) {
            return failed();
        }

        af::IoFixedFile<IoTestThread> bad_file(IoTestThread::IO_0, 1);
        const af::IoStatus bad_index = bad_file.write_at(
            *this,
            &value_,
            sizeof(value_),
            0,
            bad_index_);
        if (!bad_index.failed() || bad_index.error != EINVAL) {
            return failed();
        }

        const af::IoStatus no_buffer = file_.write_fixed_at(
            *this,
            buffer_,
            1,
            0,
            0,
            no_buffer_);
        if (!no_buffer.failed() || no_buffer.error != ENOBUFS) {
            return failed();
        }

        iovec iov{buffer_, sizeof(buffer_)};
        int buffer_error = 0;
        if (!UringIoRuntime::io_register_buffers(IoTestThread::IO_0, &iov, 1, &buffer_error)) {
            return failed();
        }

        buffer_[0] = value_;
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_fixed_at(
            *this,
            af::IoFixedBuffer{buffer_, 1, 0},
            0,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U) {
            return failed();
        }
        buffer_[0] = 0;
        state_ = State::WriteVectored;
        return again();
    }

    af::TaskResult write_vectored() {
        write_iov_[0] = iovec{&vector_write_[0], 1};
        write_iov_[1] = iovec{&vector_write_[1], 1};
        const af::IoStatus status = file_.writev_at(
            *this,
            write_iov_,
            2,
            1,
            writev_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(vector_write_)) {
            return failed();
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_value() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_fixed_at(
            *this,
            af::IoFixedBuffer{buffer_, 1, 0},
            0,
            read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U || buffer_[0] != value_) {
            return failed();
        }
        state_ = State::ReadVectored;
        return again();
    }

    af::TaskResult read_vectored() {
        read_iov_[0] = iovec{&vector_read_[0], 1};
        read_iov_[1] = iovec{&vector_read_[1], 1};
        const af::IoStatus status = file_.readv_at(
            *this,
            read_iov_,
            2,
            1,
            readv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() ||
            status.bytes != sizeof(vector_read_) ||
            vector_read_[0] != vector_write_[0] ||
            vector_read_[1] != vector_write_[1]) {
            return failed();
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_file() {
        int error = 0;
        if (!UringIoRuntime::io_unregister_buffers(IoTestThread::IO_0, &error)) {
            return failed();
        }
        if (!UringIoRuntime::io_unregister_files(IoTestThread::IO_0, &error)) {
            return failed();
        }
        byte_read_->store(buffer_[0], std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Register};
    int fd_{-1};
    af::IoFixedFile<IoTestThread> file_{};
    alignas(64) char buffer_[64]{};
    char value_{'F'};
    char vector_write_[2]{'I', 'O'};
    char vector_read_[2]{};
    iovec write_iov_[2]{};
    iovec read_iov_[2]{};
    af::IoOpState no_table_{};
    af::IoOpState bad_index_{};
    af::IoOpState no_buffer_{};
    af::IoOpState write_{};
    af::IoOpState writev_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    af::IoOpState readv_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringFixedFileUpdateTask final : public UringIoTaskBase {
public:
    explicit UringFixedFileUpdateTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int first_fd,
        int second_fd,
        std::atomic<int>* completed,
        std::atomic<int>* packed_read) {
        first_fd_ = first_fd;
        second_fd_ = second_fd;
        completed_ = completed;
        packed_read_ = packed_read;
        file_.reset(IoTestThread::IO_0, 0);
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        ReadFirst,
        Update,
        ReadSecond,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_file();

        case State::ReadFirst:
            return read_first();

        case State::Update:
            return update_file();

        case State::ReadSecond:
            return read_second();

        case State::Unregister:
            return unregister_file();
        }
        return failed();
    }

    af::TaskResult register_file() {
        int error = 0;
        if (UringIoRuntime::io_update_registered_files(
                IoTestThread::IO_0,
                0,
                &second_fd_,
                1,
                &error) ||
            error != ENOENT) {
            return failed();
        }
        if (!UringIoRuntime::io_register_files(IoTestThread::IO_0, &first_fd_, 1, &error)) {
            return failed();
        }
        if (UringIoRuntime::io_update_registered_files(
                IoTestThread::IO_0,
                1,
                &second_fd_,
                1,
                &error) ||
            error != EINVAL) {
            return failed();
        }
        state_ = State::ReadFirst;
        return again();
    }

    af::TaskResult read_first() {
        const af::IoStatus status = file_.read_at(
            *this,
            &first_read_,
            sizeof(first_read_),
            0,
            first_read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(first_read_) || first_read_ != first_value_) {
            return failed();
        }
        state_ = State::Update;
        return again();
    }

    af::TaskResult update_file() {
        int error = 0;
        if (!UringIoRuntime::io_update_registered_files(
                IoTestThread::IO_0,
                0,
                &second_fd_,
                1,
                &error)) {
            return failed();
        }
        state_ = State::ReadSecond;
        return again();
    }

    af::TaskResult read_second() {
        const af::IoStatus status = file_.read_at(
            *this,
            &second_read_,
            sizeof(second_read_),
            0,
            second_read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(second_read_) || second_read_ != second_value_) {
            return failed();
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_file() {
        int error = 0;
        if (!UringIoRuntime::io_unregister_files(IoTestThread::IO_0, &error)) {
            return failed();
        }
        const int packed =
            (static_cast<int>(static_cast<unsigned char>(first_read_)) << 8) |
            static_cast<int>(static_cast<unsigned char>(second_read_));
        packed_read_->store(packed, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Register};
    int first_fd_{-1};
    int second_fd_{-1};
    af::IoFixedFile<IoTestThread> file_{};
    char first_value_{'1'};
    char second_value_{'2'};
    char first_read_{0};
    char second_read_{0};
    af::IoOpState first_read_state_{};
    af::IoOpState second_read_state_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* packed_read_{nullptr};
};

class UringOpenAtDirectFileTask final : public UringIoTaskBase {
public:
    explicit UringOpenAtDirectFileTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        const char* path,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<char>* byte_read) {
        path_ = path;
        completed_ = completed;
        error_ = error;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Open,
        Write,
        Fsync,
        Read,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_sparse_slot();

        case State::Open:
            return open_direct();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::Unregister:
            return complete(0);
        }
        return complete(EIO);
    }

    af::TaskResult register_sparse_slot() {
        const int sparse = -1;
        int error = 0;
        if (!UringIoRuntime::io_register_files(IoTestThread::IO_0, &sparse, 1, &error)) {
            return complete(error == 0 ? EIO : error);
        }
        registered_ = true;
        state_ = State::Open;
        return again();
    }

    af::TaskResult open_direct() {
        const af::IoStatus status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            path_,
            O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC,
            0600U,
            0,
            &file_,
            open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || !file_.valid()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_value() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_at(*this, &read_, sizeof(read_), 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_) || read_ != value_) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult complete(int error) {
        if (registered_) {
            int unregister_error = 0;
            if (!UringIoRuntime::io_unregister_files(IoTestThread::IO_0, &unregister_error) &&
                error == 0) {
                error = unregister_error == 0 ? EIO : unregister_error;
            }
            registered_ = false;
        }
        byte_read_->store(read_, std::memory_order_release);
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Register};
    const char* path_{nullptr};
    af::IoFixedFile<IoTestThread> file_{};
    char value_{'D'};
    char read_{0};
    bool registered_{false};
    af::IoOpState open_{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringBatchedFileWriteTask final : public UringIoTaskBase {
public:
    explicit UringBatchedFileWriteTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::uint64_t offset,
        char value,
        std::atomic<int>* completed) {
        file_.reset(IoTestThread::IO_0, fd);
        offset_ = offset;
        value_ = value;
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), offset_, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::IoFile<IoTestThread> file_{};
    std::uint64_t offset_{0};
    char value_{0};
    af::IoOpState write_{};
    std::atomic<int>* completed_{nullptr};
};

class UringOpenAtFileTask final : public UringIoTaskBase {
public:
    explicit UringOpenAtFileTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(
        const char* path,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read) {
        path_ = path;
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Open,
        Write,
        Fsync,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Open:
            return open_file();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();
        }
        return failed();
    }

    af::TaskResult open_file() {
        int fd = -1;
        const af::IoStatus status = af::io_openat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            path_,
            O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC,
            0600U,
            &fd,
            open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return failed();
        }
        owned_.reset(fd);
        file_.reset(IoTestThread::IO_0, owned_.get());
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_value() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_at(*this, &read_, sizeof(read_), 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_) || read_ != value_) {
            return failed();
        }
        byte_read_->store(read_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Open};
    const char* path_{nullptr};
    af::UniqueFd owned_{};
    af::IoFile<IoTestThread> file_{};
    char value_{'O'};
    char read_{0};
    af::IoOpState open_{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringFileLifecycleTask final : public UringIoTaskBase {
public:
    explicit UringFileLifecycleTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        const char* path,
        const char* renamed_path,
        std::atomic<int>* completed,
        std::atomic<int>* close_released,
        std::atomic<std::uint64_t>* observed_size) {
        path_ = path;
        renamed_path_ = renamed_path;
        completed_ = completed;
        close_released_ = close_released;
        observed_size_ = observed_size;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Open,
        Fallocate,
        Write,
        Fsync,
        Read,
        Statx,
        Rename,
        Unlink,
        Close,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Open:
            return open_file();

        case State::Fallocate:
            return fallocate_file();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::Statx:
            return stat_file();

        case State::Rename:
            return rename_file();

        case State::Unlink:
            return unlink_file();

        case State::Close:
            return close_file();
        }
        return failed();
    }

    af::TaskResult open_file() {
        int fd = -1;
        const af::IoStatus status = af::io_openat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            path_,
            O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC,
            0600U,
            &fd,
            open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return failed();
        }
        owned_.reset(fd);
        file_.reset(IoTestThread::IO_0, owned_.get());
        state_ = State::Fallocate;
        return again();
    }

    af::TaskResult fallocate_file() {
        const af::IoStatus status = af::io_fallocate(
            *this,
            IoTestThread::IO_0,
            owned_.get(),
            FALLOC_FL_KEEP_SIZE,
            0,
            4096,
            fallocate_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_value() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_at(*this, &read_, sizeof(read_), 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_) || read_ != value_) {
            return failed();
        }
        state_ = State::Statx;
        return again();
    }

    af::TaskResult stat_file() {
        const af::IoStatus status = af::io_statx(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            path_,
            0,
            STATX_SIZE,
            &stat_,
            stat_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || stat_.stx_size != sizeof(value_)) {
            return failed();
        }
        observed_size_->store(stat_.stx_size, std::memory_order_release);
        state_ = State::Rename;
        return again();
    }

    af::TaskResult rename_file() {
        const af::IoStatus status = af::io_renameat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            path_,
            AT_FDCWD,
            renamed_path_,
            0,
            rename_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Unlink;
        return again();
    }

    af::TaskResult unlink_file() {
        const af::IoStatus status = af::io_unlinkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            renamed_path_,
            0,
            unlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Close;
        return again();
    }

    af::TaskResult close_file() {
        const af::IoStatus status = af::io_close(*this, IoTestThread::IO_0, owned_, close_);
        if (status.pending()) {
            if (owned_.get() != -1) {
                return failed();
            }
            return pending();
        }
        if (!status.ready() || owned_.get() != -1) {
            return failed();
        }
        close_released_->store(1, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Open};
    const char* path_{nullptr};
    const char* renamed_path_{nullptr};
    af::UniqueFd owned_{};
    af::IoFile<IoTestThread> file_{};
    char value_{'L'};
    char read_{0};
    struct statx stat_{};
    af::IoOpState open_{};
    af::IoOpState fallocate_{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    af::IoOpState stat_state_{};
    af::IoOpState rename_{};
    af::IoOpState unlink_{};
    af::IoOpState close_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* close_released_{nullptr};
    std::atomic<std::uint64_t>* observed_size_{nullptr};
};

class UringFilesystemOpsTask final : public UringIoTaskBase {
public:
    explicit UringFilesystemOpsTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        const char* dir_path,
        const char* file_path,
        const char* hardlink_path,
        const char* symlink_path,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<std::uint64_t>* observed_size) {
        dir_path_ = dir_path;
        file_path_ = file_path;
        hardlink_path_ = hardlink_path;
        symlink_path_ = symlink_path;
        completed_ = completed;
        error_ = error;
        observed_size_ = observed_size;
        how_.flags = O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC;
        how_.mode = 0600U;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Mkdir,
        OpenAt2,
        Write,
        Ftruncate,
        Fsync,
        Statx,
        Close,
        Link,
        Symlink,
        UnlinkFile,
        UnlinkHardlink,
        UnlinkSymlink,
        Rmdir,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Mkdir:
            return mkdir_dir();

        case State::OpenAt2:
            return open_file();

        case State::Write:
            return write_payload();

        case State::Ftruncate:
            return truncate_file();

        case State::Fsync:
            return fsync_file();

        case State::Statx:
            return stat_file();

        case State::Close:
            return close_file();

        case State::Link:
            return link_file();

        case State::Symlink:
            return symlink_file();

        case State::UnlinkFile:
            return unlink_file(file_path_, State::UnlinkHardlink, 0);

        case State::UnlinkHardlink:
            return unlink_file(hardlink_path_, State::UnlinkSymlink, 0);

        case State::UnlinkSymlink:
            return unlink_file(symlink_path_, State::Rmdir, 0);

        case State::Rmdir:
            return unlink_file(dir_path_, State::Rmdir, AT_REMOVEDIR, true);
        }
        return complete(EIO);
    }

    af::TaskResult mkdir_dir() {
        const af::IoStatus status = af::io_mkdirat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            dir_path_,
            0700U,
            mkdir_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::OpenAt2;
        return again();
    }

    af::TaskResult open_file() {
        int fd = -1;
        const af::IoStatus status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            file_path_,
            &how_,
            &fd,
            open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return complete(status.failed() ? status.error : EIO);
        }
        owned_.reset(fd);
        file_.reset(IoTestThread::IO_0, owned_.get());
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_payload() {
        const af::IoStatus status = file_.write_at(*this, payload_, sizeof(payload_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(payload_)) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Ftruncate;
        return again();
    }

    af::TaskResult truncate_file() {
        const af::IoStatus status =
            af::io_ftruncate(*this, IoTestThread::IO_0, owned_.get(), 1, truncate_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_file() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Statx;
        return again();
    }

    af::TaskResult stat_file() {
        const af::IoStatus status = af::io_statx(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            file_path_,
            0,
            STATX_SIZE,
            &stat_,
            stat_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || stat_.stx_size != 1U) {
            return complete(status.failed() ? status.error : EIO);
        }
        observed_size_->store(stat_.stx_size, std::memory_order_release);
        state_ = State::Close;
        return again();
    }

    af::TaskResult close_file() {
        const af::IoStatus status = af::io_close(*this, IoTestThread::IO_0, owned_, close_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || owned_.get() != -1) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Link;
        return again();
    }

    af::TaskResult link_file() {
        const af::IoStatus status = af::io_linkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            file_path_,
            AT_FDCWD,
            hardlink_path_,
            0,
            link_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Symlink;
        return again();
    }

    af::TaskResult symlink_file() {
        const af::IoStatus status = af::io_symlinkat(
            *this,
            IoTestThread::IO_0,
            file_path_,
            AT_FDCWD,
            symlink_path_,
            symlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::UnlinkFile;
        return again();
    }

    af::TaskResult unlink_file(
        const char* path,
        State next_state,
        int flags,
        bool final_state = false) {
        const af::IoStatus status = af::io_unlinkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            path,
            flags,
            unlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        if (final_state) {
            return complete(0);
        }
        state_ = next_state;
        unlink_.reset();
        return again();
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Mkdir};
    const char* dir_path_{nullptr};
    const char* file_path_{nullptr};
    const char* hardlink_path_{nullptr};
    const char* symlink_path_{nullptr};
    struct open_how how_{};
    af::UniqueFd owned_{};
    af::IoFile<IoTestThread> file_{};
    char payload_[2]{'F', 'S'};
    struct statx stat_{};
    af::IoOpState mkdir_{};
    af::IoOpState open_{};
    af::IoOpState write_{};
    af::IoOpState truncate_{};
    af::IoOpState fsync_{};
    af::IoOpState stat_state_{};
    af::IoOpState close_{};
    af::IoOpState link_{};
    af::IoOpState symlink_{};
    af::IoOpState unlink_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<std::uint64_t>* observed_size_{nullptr};
};

class UringStreamFallbackTask final : public UringIoTaskBase {
public:
    explicit UringStreamFallbackTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

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
    af::TaskResult run() override {
        const af::IoStatus status = stream_.recv_some(*this, &value_, sizeof(value_), read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        byte_read_->store(value_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringCancellableSocketRecvTask final : public UringIoTaskBase {
public:
    explicit UringCancellableSocketRecvTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<af::IoOpState*>* state,
        std::atomic<int>* wait_kind,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<int>* bytes) {
        stream_.reset(IoTestThread::IO_0, fd);
        state_ = state;
        wait_kind_ = wait_kind;
        armed_ = armed;
        completed_ = completed;
        error_ = error;
        bytes_ = bytes;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.recv_some(*this, &value_, sizeof(value_), recv_);
        if (status.pending()) {
            state_->store(&recv_, std::memory_order_release);
            wait_kind_->store(static_cast<int>(recv_.wait_kind), std::memory_order_release);
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }

        if (status.failed()) {
            error_->store(status.error, std::memory_order_release);
        } else if (status.ready()) {
            error_->store(0, std::memory_order_release);
            bytes_->store(static_cast<int>(status.bytes), std::memory_order_release);
        } else if (status.closed()) {
            error_->store(0, std::memory_order_release);
            bytes_->store(0, std::memory_order_release);
        } else {
            return failed();
        }

        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{0};
    af::IoOpState recv_{};
    std::atomic<af::IoOpState*>* state_{nullptr};
    std::atomic<int>* wait_kind_{nullptr};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<int>* bytes_{nullptr};
};

class UringCancelIoStateTask final : public UringIoTaskBase {
public:
    explicit UringCancelIoStateTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(
        std::atomic<af::IoOpState*>* state,
        std::atomic<int>* completed,
        std::atomic<int>* result,
        std::atomic<int>* error) {
        state_ = state;
        completed_ = completed;
        result_ = result;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState* state = state_->load(std::memory_order_acquire);
        if (state == nullptr) {
            return failed();
        }

        const bool ok = UringIoRuntime::cancel_io(IoTestThread::IO_0, *state);
        result_->store(ok ? 1 : 0, std::memory_order_release);
        error_->store(state->wait.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<af::IoOpState*>* state_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* result_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class UringStreamSendTask final : public UringIoTaskBase {
public:
    explicit UringStreamSendTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* completed) {
        stream_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.send_some(*this, &value_, sizeof(value_), write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{'S'};
    af::IoOpState write_{};
    std::atomic<int>* completed_{nullptr};
};

class UringStreamSendZcTask final : public UringIoTaskBase {
public:
    explicit UringStreamSendZcTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* completed) {
        stream_.reset(IoTestThread::IO_0, fd);
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_value();
    }

    af::TaskResult send_value() {
        const af::IoStatus status = stream_.send_zc_some(*this, &value_, sizeof(value_), write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    char value_{'Z'};
    af::IoOpState write_{};
    std::atomic<int>* completed_{nullptr};
};

class UringStreamVectoredTask final : public UringIoTaskBase {
public:
    explicit UringStreamVectoredTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* request_seen) {
        stream_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        request_seen_ = request_seen;
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
        request_iov_[0] = iovec{&request_[0], 1};
        request_iov_[1] = iovec{&request_[1], 1};
        const af::IoStatus status = stream_.recvv_some(*this, request_iov_, 2, read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }
        const int combined =
            (static_cast<int>(request_[0]) << 8) | static_cast<unsigned char>(request_[1]);
        request_seen_->store(combined, std::memory_order_release);
        state_ = State::SendResponse;
        return again();
    }

    af::TaskResult send_response() {
        response_iov_[0] = iovec{&response_[0], 1};
        response_iov_[1] = iovec{&response_[1], 1};
        const af::IoStatus status = stream_.sendv_some(*this, response_iov_, 2, write_);
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
    char request_[2]{};
    char response_[2]{'U', 'V'};
    iovec request_iov_[2]{};
    iovec response_iov_[2]{};
    af::IoOpState read_{};
    af::IoOpState write_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* request_seen_{nullptr};
};

class UringUdpRecvTask final : public UringIoTaskBase {
public:
    explicit UringUdpRecvTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read) {
        socket_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        peer_size_ = sizeof(peer_);
        const af::IoStatus status = socket_.recv_from_some(
            *this,
            &value_,
            sizeof(value_),
            reinterpret_cast<sockaddr*>(&peer_),
            &peer_size_,
            recv_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_) || peer_size_ == 0U) {
            return failed();
        }
        byte_read_->store(value_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::UdpSocket<IoTestThread> socket_{};
    char value_{0};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UringUdpSendToTask final : public UringIoTaskBase {
public:
    explicit UringUdpSendToTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        sockaddr_in address,
        socklen_t address_size,
        char value,
        std::atomic<int>* completed,
        std::atomic<int>* bytes_sent) {
        socket_.reset(IoTestThread::IO_0, fd);
        address_ = address;
        address_size_ = address_size;
        value_ = value;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = socket_.send_to_some(
            *this,
            &value_,
            sizeof(value_),
            reinterpret_cast<const sockaddr*>(&address_),
            address_size_,
            send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        bytes_sent_->store(static_cast<int>(status.bytes), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::UdpSocket<IoTestThread> socket_{};
    sockaddr_in address_{};
    socklen_t address_size_{sizeof(address_)};
    char value_{0};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* bytes_sent_{nullptr};
};

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

class UringTcpAcceptDirectTask final : public UringIoTaskBase {
public:
    explicit UringTcpAcceptDirectTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<int>* packed_read) {
        listener_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        error_ = error;
        packed_read_ = packed_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Accept,
        Recv,
        Send,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_sparse_slot();

        case State::Accept:
            return accept_direct();

        case State::Recv:
            return recv_request();

        case State::Send:
            return send_response();

        case State::Unregister:
            return complete(0);
        }
        return complete(EIO);
    }

    af::TaskResult register_sparse_slot() {
        const int sparse = -1;
        int error = 0;
        if (!UringIoRuntime::io_register_files(IoTestThread::IO_0, &sparse, 1, &error)) {
            return complete(error == 0 ? EIO : error);
        }
        registered_ = true;
        state_ = State::Accept;
        return again();
    }

    af::TaskResult accept_direct() {
        const af::IoStatus status = listener_.accept_direct(
            *this,
            nullptr,
            nullptr,
            0,
            &accepted_,
            accept_);
        if (status.pending()) {
            if (!armed_once_) {
                armed_once_ = true;
                armed_->fetch_add(1, std::memory_order_release);
            }
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        if (!accepted_.valid()) {
            return complete(EIO);
        }
        state_ = State::Recv;
        return again();
    }

    af::TaskResult recv_request() {
        request_iov_[0] = iovec{&request_[0], 1};
        request_iov_[1] = iovec{&request_[1], 1};
        const af::IoStatus status =
            accepted_.recvv_some(*this, request_iov_, 2, recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return complete(status.failed() ? status.error : EIO);
        }
        packed_read_->store(pack_request(), std::memory_order_release);
        state_ = State::Send;
        return again();
    }

    af::TaskResult send_response() {
        response_iov_[0] = iovec{&response_[0], 1};
        response_iov_[1] = iovec{&response_[1], 1};
        const af::IoStatus status =
            accepted_.sendv_some(*this, response_iov_, 2, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult complete(int error) {
        if (registered_) {
            int unregister_error = 0;
            if (!UringIoRuntime::io_unregister_files(IoTestThread::IO_0, &unregister_error) &&
                error == 0) {
                error = unregister_error == 0 ? EIO : unregister_error;
            }
            registered_ = false;
        }
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    [[nodiscard]] int pack_request() const noexcept {
        return (static_cast<int>(static_cast<unsigned char>(request_[0])) << 8) |
               static_cast<int>(static_cast<unsigned char>(request_[1]));
    }

    State state_{State::Register};
    af::TcpListener<IoTestThread> listener_{};
    af::IoFixedFile<IoTestThread> accepted_{};
    char request_[2]{};
    char response_[2]{'O', 'K'};
    iovec request_iov_[2]{};
    iovec response_iov_[2]{};
    bool registered_{false};
    bool armed_once_{false};
    af::IoOpState accept_{};
    af::IoOpState recv_{};
    af::IoOpState send_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<int>* packed_read_{nullptr};
};

class UringTcpAcceptMultishotTask final : public UringIoTaskBase {
public:
    explicit UringTcpAcceptMultishotTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        int target_accepts,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* accepted_count,
        std::atomic<int>* error) {
        listener_.reset(IoTestThread::IO_0, fd);
        target_accepts_ = target_accepts;
        armed_ = armed;
        completed_ = completed;
        accepted_count_ = accepted_count;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Accept,
        Cancel,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Accept:
            return accept_one();
        case State::Cancel:
            return finish_cancel();
        }
        return failed();
    }

    af::TaskResult accept_one() {
        const af::IoStatus status = listener_.accept_multishot(
            *this,
            nullptr,
            nullptr,
            &accepted_fd_,
            accept_);
        if (status.pending()) {
            if (!armed_once_) {
                armed_once_ = true;
                armed_->fetch_add(1, std::memory_order_release);
            }
            return pending();
        }
        if (status.failed()) {
            error_->store(status.error, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (!status.ready() || accepted_fd_ < 0) {
            return failed();
        }

        ::close(accepted_fd_);
        accepted_fd_ = -1;
        const int accepted = accepted_count_->fetch_add(1, std::memory_order_acq_rel) + 1;
        if (accepted < target_accepts_) {
            return pending();
        }

        if (!accept_.waiting) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (!UringIoRuntime::cancel_io(IoTestThread::IO_0, accept_)) {
            error_->store(accept_.wait.error == 0 ? EIO : accept_.wait.error, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        state_ = State::Cancel;
        return pending();
    }

    af::TaskResult finish_cancel() {
        int ignored = -1;
        const af::IoStatus status =
            listener_.accept_multishot(*this, nullptr, nullptr, &ignored, accept_);
        if (status.pending()) {
            return pending();
        }
        if (!status.failed() || status.error != ECANCELED) {
            return failed();
        }
        error_->store(0, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Accept};
    af::TcpListener<IoTestThread> listener_{};
    int accepted_fd_{-1};
    int target_accepts_{0};
    bool armed_once_{false};
    af::IoOpState accept_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* accepted_count_{nullptr};
    std::atomic<int>* error_{nullptr};
};

#if defined(__linux__)
class UringRecvMultishotTask final : public UringIoTaskBase {
public:
    explicit UringRecvMultishotTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        int target_reads,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* read_count,
        std::atomic<int>* packed_read,
        std::atomic<int>* error) {
        return do_it(
            fd,
            target_reads,
            armed,
            completed,
            read_count,
            packed_read,
            error,
            false);
    }

    bool do_it(
        int fd,
        int target_reads,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* read_count,
        std::atomic<int>* packed_read,
        std::atomic<int>* error,
        bool datagram) {
        stream_.reset(IoTestThread::IO_0, fd);
        datagram_.reset(IoTestThread::IO_0, fd);
        target_reads_ = target_reads;
        datagram_mode_ = datagram;
        armed_ = armed;
        completed_ = completed;
        read_count_ = read_count;
        packed_read_ = packed_read;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Recv,
        Cancel,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_ring();
        case State::Recv:
            return recv_one();
        case State::Cancel:
            return finish_cancel();
        case State::Unregister:
            return unregister_ring();
        }
        return failed();
    }

    af::TaskResult register_ring() {
        int init_error = 0;
        if (!ring_.init(buffer_count, init_error)) {
            return complete(init_error == 0 ? EIO : init_error);
        }
        af::IoProvidedBuffer buffers[buffer_count]{};
        for (std::uint16_t i = 0; i < buffer_count; ++i) {
            buffers[i] = af::IoProvidedBuffer{&buffers_[i], sizeof(buffers_[i]), i};
        }
        int add_error = 0;
        if (!ring_.add(buffers, buffer_count, add_error)) {
            return complete(add_error == 0 ? EIO : add_error);
        }

        int register_error = 0;
        if (!UringIoRuntime::io_register_provided_buffer_ring(
                IoTestThread::IO_0,
                ring_.ring(),
                ring_.entries(),
                buffer_group,
                &register_error)) {
            return complete(register_error == 0 ? EIO : register_error);
        }
        registered_ = true;
        state_ = State::Recv;
        return again();
    }

    af::TaskResult recv_one() {
        std::uint16_t buffer_id = 0;
        const af::IoStatus status = datagram_mode_
            ? datagram_.recv_multishot(*this, buffer_group, &buffer_id, recv_)
            : stream_.recv_multishot(*this, buffer_group, &buffer_id, recv_);
        if (status.pending()) {
            if (!armed_once_) {
                armed_once_ = true;
                armed_->fetch_add(1, std::memory_order_release);
            }
            return pending();
        }
        if (status.failed()) {
            return complete(status.error);
        }
        if (!status.ready() || status.bytes != 1U || buffer_id >= buffer_count) {
            return complete(EIO);
        }

        const int previous = read_count_->fetch_add(1, std::memory_order_acq_rel);
        const int shifted = previous == 0 ? 8 : 0;
        packed_read_->fetch_or(
            static_cast<int>(static_cast<unsigned char>(buffers_[buffer_id])) << shifted,
            std::memory_order_acq_rel);

        const af::IoProvidedBuffer buffer{
            &buffers_[buffer_id],
            sizeof(buffers_[buffer_id]),
            buffer_id};
        int add_error = 0;
        if (!ring_.add(&buffer, 1, add_error)) {
            return stop_recv(add_error == 0 ? EIO : add_error);
        }

        if (previous + 1 < target_reads_) {
            return pending();
        }
        if (!recv_.waiting) {
            state_ = State::Unregister;
            return again();
        }
        finish_error_ = 0;
        if (!UringIoRuntime::cancel_io(IoTestThread::IO_0, recv_)) {
            return complete(recv_.wait.error == 0 ? EIO : recv_.wait.error);
        }
        state_ = State::Cancel;
        return pending();
    }

    af::TaskResult finish_cancel() {
        std::uint16_t ignored = 0;
        const af::IoStatus status = datagram_mode_
            ? datagram_.recv_multishot(*this, buffer_group, &ignored, recv_)
            : stream_.recv_multishot(*this, buffer_group, &ignored, recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.failed() || status.error != ECANCELED) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_ring() {
        if (registered_) {
            int unregister_error = 0;
            if (!UringIoRuntime::io_unregister_provided_buffer_ring(
                    IoTestThread::IO_0,
                    buffer_group,
                    &unregister_error)) {
                return complete(unregister_error == 0 ? EIO : unregister_error);
            }
            registered_ = false;
        }
        return complete(finish_error_);
    }

    af::TaskResult complete(int error) {
        if (registered_) {
            int unregister_error = 0;
            if (UringIoRuntime::io_unregister_provided_buffer_ring(
                    IoTestThread::IO_0,
                    buffer_group,
                    &unregister_error)) {
                registered_ = false;
            }
        }
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TaskResult stop_recv(int error) {
        finish_error_ = error == 0 ? EIO : error;
        if (!recv_.waiting) {
            state_ = State::Unregister;
            return again();
        }
        if (!UringIoRuntime::cancel_io(IoTestThread::IO_0, recv_)) {
            return complete(recv_.wait.error == 0 ? finish_error_ : recv_.wait.error);
        }
        state_ = State::Cancel;
        return pending();
    }

    static constexpr std::uint16_t buffer_group = 7;
    static constexpr unsigned buffer_count = 2;
    State state_{State::Register};
    af::TcpStream<IoTestThread> stream_{};
    af::UdpSocket<IoTestThread> datagram_{};
    af::IoProvidedBufferRing ring_{};
    char buffers_[buffer_count]{};
    int target_reads_{0};
    int finish_error_{0};
    bool datagram_mode_{false};
    bool armed_once_{false};
    bool registered_{false};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* read_count_{nullptr};
    std::atomic<int>* packed_read_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class UringUdpRecvmsgMultishotTask final : public UringIoTaskBase {
public:
    explicit UringUdpRecvmsgMultishotTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(
        int fd,
        in_port_t expected_port,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* read_count,
        std::atomic<int>* packed_read,
        std::atomic<int>* peer_count,
        std::atomic<int>* error) {
        socket_.reset(IoTestThread::IO_0, fd);
        expected_port_ = expected_port;
        armed_ = armed;
        completed_ = completed;
        read_count_ = read_count;
        packed_read_ = packed_read;
        peer_count_ = peer_count;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Recv,
        Cancel,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_ring();
        case State::Recv:
            return recv_one();
        case State::Cancel:
            return finish_cancel();
        case State::Unregister:
            return unregister_ring();
        }
        return complete(EIO);
    }

    af::TaskResult register_ring() {
        int init_error = 0;
        if (!ring_.init(buffer_count, init_error)) {
            return complete(init_error == 0 ? EIO : init_error);
        }
        af::IoProvidedBuffer buffers[buffer_count]{};
        for (std::uint16_t i = 0; i < buffer_count; ++i) {
            buffers[i] = af::IoProvidedBuffer{buffers_[i], buffer_size, i};
        }
        int add_error = 0;
        if (!ring_.add(buffers, buffer_count, add_error)) {
            return complete(add_error == 0 ? EIO : add_error);
        }

        int register_error = 0;
        if (!UringIoRuntime::io_register_provided_buffer_ring(
                IoTestThread::IO_0,
                ring_.ring(),
                ring_.entries(),
                buffer_group,
                &register_error)) {
            return complete(register_error == 0 ? EIO : register_error);
        }
        registered_ = true;
        state_ = State::Recv;
        return again();
    }

    af::TaskResult recv_one() {
        std::uint16_t buffer_id = 0;
        const af::IoStatus status = socket_.recv_from_multishot(
            *this,
            buffer_group,
            name_capacity,
            0,
            &buffer_id,
            recv_);
        if (status.pending()) {
            if (!armed_once_) {
                armed_once_ = true;
                armed_->fetch_add(1, std::memory_order_release);
            }
            return pending();
        }
        if (status.failed()) {
            return complete(status.error);
        }
        if (!status.ready() || buffer_id >= buffer_count) {
            return complete(EIO);
        }

        af::IoRecvmsgMultishotView view{};
        int parse_error = 0;
        if (!af::io_parse_recvmsg_multishot_buffer(
                buffers_[buffer_id],
                buffer_size,
                status.bytes,
                name_capacity,
                0,
                view,
                parse_error)) {
            return stop_recv(parse_error == 0 ? EIO : parse_error);
        }
        if (view.name_size < sizeof(sockaddr_in) || view.payload_size != 1U) {
            return stop_recv(EIO);
        }

        const auto* address = reinterpret_cast<const sockaddr_in*>(
            buffers_[buffer_id] + view.name_offset);
        if (address->sin_family != AF_INET || address->sin_port != expected_port_) {
            return stop_recv(EIO);
        }
        peer_count_->fetch_add(1, std::memory_order_acq_rel);

        const int previous = read_count_->fetch_add(1, std::memory_order_acq_rel);
        const int shifted = previous == 0 ? 8 : 0;
        packed_read_->fetch_or(
            static_cast<int>(
                static_cast<unsigned char>(buffers_[buffer_id][view.payload_offset])) << shifted,
            std::memory_order_acq_rel);

        const af::IoProvidedBuffer buffer{buffers_[buffer_id], buffer_size, buffer_id};
        int add_error = 0;
        if (!ring_.add(&buffer, 1, add_error)) {
            return stop_recv(add_error == 0 ? EIO : add_error);
        }

        if (previous + 1 < target_reads) {
            return pending();
        }
        if (!recv_.waiting) {
            state_ = State::Unregister;
            return again();
        }
        finish_error_ = 0;
        if (!UringIoRuntime::cancel_io(IoTestThread::IO_0, recv_)) {
            return complete(recv_.wait.error == 0 ? EIO : recv_.wait.error);
        }
        state_ = State::Cancel;
        return pending();
    }

    af::TaskResult finish_cancel() {
        std::uint16_t ignored = 0;
        const af::IoStatus status = socket_.recv_from_multishot(
            *this,
            buffer_group,
            name_capacity,
            0,
            &ignored,
            recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.failed() || status.error != ECANCELED) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_ring() {
        if (registered_) {
            int unregister_error = 0;
            if (!UringIoRuntime::io_unregister_provided_buffer_ring(
                    IoTestThread::IO_0,
                    buffer_group,
                    &unregister_error)) {
                return complete(unregister_error == 0 ? EIO : unregister_error);
            }
            registered_ = false;
        }
        return complete(finish_error_);
    }

    af::TaskResult complete(int error) {
        if (registered_) {
            int unregister_error = 0;
            if (UringIoRuntime::io_unregister_provided_buffer_ring(
                    IoTestThread::IO_0,
                    buffer_group,
                    &unregister_error)) {
                registered_ = false;
            }
        }
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TaskResult stop_recv(int error) {
        finish_error_ = error == 0 ? EIO : error;
        if (!recv_.waiting) {
            state_ = State::Unregister;
            return again();
        }
        if (!UringIoRuntime::cancel_io(IoTestThread::IO_0, recv_)) {
            return complete(recv_.wait.error == 0 ? finish_error_ : recv_.wait.error);
        }
        state_ = State::Cancel;
        return pending();
    }

    static constexpr std::uint16_t buffer_group = 8;
    static constexpr unsigned buffer_count = 2;
    static constexpr int target_reads = 2;
    static constexpr socklen_t name_capacity = sizeof(sockaddr_storage);
    static constexpr std::size_t buffer_size =
        sizeof(af::detail::IoUringRecvmsgOut) + name_capacity + 16U;

    State state_{State::Register};
    af::UdpSocket<IoTestThread> socket_{};
    af::IoProvidedBufferRing ring_{};
    alignas(64) char buffers_[buffer_count][buffer_size]{};
    in_port_t expected_port_{0};
    int finish_error_{0};
    bool armed_once_{false};
    bool registered_{false};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* read_count_{nullptr};
    std::atomic<int>* packed_read_{nullptr};
    std::atomic<int>* peer_count_{nullptr};
    std::atomic<int>* error_{nullptr};
};
#endif

class UringTcpConnectTask final : public UringIoTaskBase {
public:
    explicit UringTcpConnectTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, sockaddr_in address, socklen_t address_size, std::atomic<int>* completed) {
        stream_.reset(IoTestThread::IO_0, fd);
        address_ = address;
        address_size_ = address_size;
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.connect(
            *this,
            reinterpret_cast<const sockaddr*>(&address_),
            address_size_,
            connect_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    sockaddr_in address_{};
    socklen_t address_size_{sizeof(address_)};
    af::IoOpState connect_{};
    std::atomic<int>* completed_{nullptr};
};

class SocketHangupTask final : public IoTaskBase {
public:
    explicit SocketHangupTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* armed, std::atomic<int>* closed) {
        fd_ = fd;
        armed_ = armed;
        closed_ = closed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.closed()) {
            return failed();
        }
        closed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int fd_{-1};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* closed_{nullptr};
};

class DuplicateWaitRejectedTask final : public IoTaskBase {
public:
    explicit DuplicateWaitRejectedTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* rejected, std::atomic<int>* error) {
        fd_ = fd;
        rejected_ = rejected;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoResult result{};
        if (wait_io(IoTestThread::IO_0, fd_, af::io_readable, &result)) {
            return failed();
        }
        error_->store(result.error, std::memory_order_release);
        rejected_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int fd_{-1};
    std::atomic<int>* rejected_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class BadFdReadTask final : public IoTaskBase {
public:
    explicit BadFdReadTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        char value = 0;
        af::IoOpState read{};
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            -1,
            &value,
            sizeof(value),
            read);
        if (!status.failed()) {
            return failed();
        }
        error_->store(status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class CancellableSocketReadTask final : public IoTaskBase {
public:
    explicit CancellableSocketReadTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<af::IoOpState*>* state,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* error) {
        fd_ = fd;
        state_ = state;
        armed_ = armed;
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_machine_) {
        case State::Arm:
            return arm_read();

        case State::Finish:
            return finish_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_machine_ = State::Finish;
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        if (!status.pending()) {
            return failed();
        }
        state_->store(&read_, std::memory_order_release);
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    af::TaskResult finish_read() {
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        if (!status.failed() || status.error != ECANCELED) {
            return failed();
        }
        error_->store(status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_machine_{State::Arm};
    int fd_{-1};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<af::IoOpState*>* state_{nullptr};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class CancelIoStateTask final : public IoTaskBase {
public:
    explicit CancelIoStateTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        std::atomic<af::IoOpState*>* state,
        bool cancel_twice,
        std::atomic<int>* completed,
        std::atomic<int>* first_result,
        std::atomic<int>* second_result,
        std::atomic<int>* error) {
        state_ = state;
        cancel_twice_ = cancel_twice;
        completed_ = completed;
        first_result_ = first_result;
        second_result_ = second_result;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState* state = state_->load(std::memory_order_acquire);
        if (state == nullptr) {
            return failed();
        }

        const bool first = IoRuntime::cancel_io(IoTestThread::IO_0, *state);
        first_result_->store(first ? 1 : 0, std::memory_order_release);
        error_->store(state->wait.error, std::memory_order_release);

        if (cancel_twice_) {
            const bool second = IoRuntime::cancel_io(IoTestThread::IO_0, *state);
            second_result_->store(second ? 1 : 0, std::memory_order_release);
        }

        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<af::IoOpState*>* state_{nullptr};
    bool cancel_twice_{false};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* first_result_{nullptr};
    std::atomic<int>* second_result_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class CancelIdleIoStateTask final : public IoTaskBase {
public:
    explicit CancelIdleIoStateTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* result, std::atomic<int>* error) {
        completed_ = completed;
        result_ = result;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState state{};
        const bool ok = IoRuntime::cancel_io(IoTestThread::IO_0, state);
        result_->store(ok ? 1 : 0, std::memory_order_release);
        error_->store(state.wait.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* result_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class TimeoutSocketReadTask final : public IoTaskBase {
public:
    explicit TimeoutSocketReadTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::chrono::nanoseconds timeout,
        std::atomic<af::IoOpState*>* state,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<char>* byte_read) {
        fd_ = fd;
        timeout_ = timeout;
        state_ = state;
        armed_ = armed;
        completed_ = completed;
        error_ = error;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Resume,
    };

    af::TaskResult run() override {
        switch (state_machine_) {
        case State::Arm:
            return arm_read();

        case State::Resume:
            return resume_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_machine_ = State::Resume;
        deadline_.set_after(timeout_);
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        if (!status.pending()) {
            return failed();
        }
        const af::IoStatus timeout = af::arm_io_timeout(
            *this,
            IoTestThread::IO_0,
            deadline_,
            read_);
        if (!timeout.pending()) {
            return failed();
        }
        state_->store(&read_, std::memory_order_release);
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    af::TaskResult resume_read() {
        const af::IoStatus timeout = af::arm_io_timeout(
            *this,
            IoTestThread::IO_0,
            deadline_,
            read_);
        if (timeout.pending()) {
            return pending();
        }
        if (timeout.failed()) {
            error_->store(timeout.error, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (!timeout.ready()) {
            return failed();
        }

        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        if (status.ready() && status.bytes == sizeof(value_)) {
            byte_read_->store(value_, std::memory_order_release);
            error_->store(0, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (status.failed()) {
            error_->store(status.error, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        return failed();
    }

    State state_machine_{State::Arm};
    int fd_{-1};
    std::chrono::nanoseconds timeout_{0};
    char value_{0};
    af::IoOpState read_{};
    af::IoDeadline deadline_{};
    std::atomic<af::IoOpState*>* state_{nullptr};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class ZeroByteIoTask final : public IoTaskBase {
public:
    explicit ZeroByteIoTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState read{};
        af::IoOpState write{};
        const af::IoStatus read_status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            0,
            read);
        const af::IoStatus write_status = af::io_write_some(
            *this,
            IoTestThread::IO_0,
            -1,
            nullptr,
            0,
            write);
        if (!read_status.ready() || read_status.bytes != 0U ||
            !write_status.ready() || write_status.bytes != 0U) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
};

class VectoredBoundaryTask final : public IoTaskBase {
public:
    explicit VectoredBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoFile<IoTestThread> file(IoTestThread::IO_0, -1);
        af::TcpStream<IoTestThread> stream(IoTestThread::IO_0, -1);
        af::IoOpState readv{};
        af::IoOpState writev{};
        af::IoOpState readv_at{};
        af::IoOpState writev_at{};
        af::IoOpState recvv{};
        af::IoOpState sendv{};
        af::IoOpState sendv_zc{};
        af::IoOpState recv_from{};
        af::IoOpState send_to{};
        af::IoOpState send_zc_to{};
        af::IoOpState bad_file{};
        af::IoOpState bad_iov_state{};
        af::IoOpState bad_iov_zc_state{};
        af::IoOpState bad_count_state{};
        af::IoOpState bad_datagram_state{};
        af::IoOpState bad_datagram_zc_state{};

        char value = 'v';
        iovec valid_iov{&value, 1};
        iovec invalid_iov{nullptr, 1};
        af::UdpSocket<IoTestThread> datagram(IoTestThread::IO_0, -1);

        const af::IoStatus zero_readv = file.readv_some(*this, nullptr, 0, readv);
        const af::IoStatus zero_writev = file.writev_some(*this, nullptr, 0, writev);
        const af::IoStatus zero_readv_at = file.readv_at(*this, nullptr, 0, 0, readv_at);
        const af::IoStatus zero_writev_at = file.writev_at(*this, nullptr, 0, 0, writev_at);
        const af::IoStatus zero_recvv = stream.recvv_some(*this, nullptr, 0, recvv);
        const af::IoStatus zero_sendv = stream.sendv_some(*this, nullptr, 0, sendv);
        const af::IoStatus zero_sendv_zc =
            stream.sendv_zc_some(*this, nullptr, 0, sendv_zc);
        const af::IoStatus zero_recvv_from =
            datagram.recvv_from_some(*this, nullptr, 0, nullptr, nullptr, recv_from);
        const af::IoStatus zero_sendv_to =
            datagram.sendv_to_some(*this, nullptr, 0, nullptr, 0, send_to);
        const af::IoStatus zero_sendv_zc_to =
            datagram.sendv_zc_to_some(*this, nullptr, 0, nullptr, 0, send_zc_to);
        const af::IoStatus bad_file_status =
            file.writev_at(*this, &valid_iov, 1, 0, bad_file);
        const af::IoStatus bad_iov =
            stream.sendv_some(*this, &invalid_iov, 1, bad_iov_state);
        const af::IoStatus bad_iov_zc =
            stream.sendv_zc_some(*this, &invalid_iov, 1, bad_iov_zc_state);
        const af::IoStatus bad_count =
            stream.recvv_some(*this, &valid_iov, -1, bad_count_state);
        const af::IoStatus bad_datagram =
            datagram.sendv_to_some(*this, &invalid_iov, 1, nullptr, 0, bad_datagram_state);
        const af::IoStatus bad_datagram_zc =
            datagram.sendv_zc_to_some(*this, &invalid_iov, 1, nullptr, 0, bad_datagram_zc_state);

        if (!zero_readv.ready() || zero_readv.bytes != 0U ||
            !zero_writev.ready() || zero_writev.bytes != 0U ||
            !zero_readv_at.ready() || zero_readv_at.bytes != 0U ||
            !zero_writev_at.ready() || zero_writev_at.bytes != 0U ||
            !zero_recvv.ready() || zero_recvv.bytes != 0U ||
            !zero_sendv.ready() || zero_sendv.bytes != 0U ||
            !zero_sendv_zc.ready() || zero_sendv_zc.bytes != 0U ||
            !zero_recvv_from.ready() || zero_recvv_from.bytes != 0U ||
            !zero_sendv_to.ready() || zero_sendv_to.bytes != 0U ||
            !zero_sendv_zc_to.ready() || zero_sendv_zc_to.bytes != 0U ||
            !bad_file_status.failed() || bad_file_status.error != EBADF ||
            !bad_iov.failed() || bad_iov.error != EINVAL ||
            !bad_iov_zc.failed() || bad_iov_zc.error != EINVAL ||
            !bad_count.failed() || bad_count.error != EINVAL ||
            !bad_datagram.failed() || bad_datagram.error != EINVAL ||
            !bad_datagram_zc.failed() || bad_datagram_zc.error != EINVAL) {
            return failed();
        }

        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
};

template <typename TaskBaseT>
class BasicTimerFdTask final : public TaskBaseT {
public:
    explicit BasicTimerFdTask(typename TaskBaseT::FactoryToken token) : TaskBaseT(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<std::uint64_t>* expirations) {
        timer_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        expirations_ = expirations;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        std::uint64_t count = 0;
        const af::IoStatus status = timer_.wait(*this, &count, wait_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return this->pending();
        }
        if (!status.ready() || status.bytes != sizeof(count) || count == 0U) {
            return this->failed();
        }
        expirations_->store(count, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::IoTimer<IoTestThread> timer_{};
    af::IoOpState wait_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint64_t>* expirations_{nullptr};
};

using TimerFdTask = BasicTimerFdTask<IoTaskBase>;
using UringTimerFdTask = BasicTimerFdTask<UringIoTaskBase>;

template <typename TaskBaseT>
class BasicUringTimeoutTask final : public TaskBaseT {
public:
    explicit BasicUringTimeoutTask(typename TaskBaseT::FactoryToken token) : TaskBaseT(token) {}

    bool do_it(
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* error) {
        armed_ = armed;
        completed_ = completed;
        error_ = error;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = af::io_wait_timeout(
            *this,
            IoTestThread::IO_0,
            std::chrono::milliseconds(1),
            wait_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return this->pending();
        }
        error_->store(status.failed() ? status.error : 0, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::IoOpState wait_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

using UringTimeoutTask = BasicUringTimeoutTask<UringIoTaskBase>;

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

class OpenAtBoundaryTask final : public IoTaskBase {
public:
    explicit OpenAtBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState null_path{};
        af::IoOpState null_output{};
        af::IoOpState no_uring{};
        af::IoOpState close_no_uring{};
        af::IoOpState stat_no_uring{};
        af::IoOpState fallocate_no_uring{};
        af::IoOpState rename_no_uring{};
        af::IoOpState unlink_no_uring{};
        af::IoOpState openat2_no_uring{};
        af::IoOpState mkdir_no_uring{};
        af::IoOpState symlink_no_uring{};
        af::IoOpState link_no_uring{};
        af::IoOpState ftruncate_no_uring{};
        af::IoOpState stat_null_path{};
        af::IoOpState stat_null_output{};
        af::IoOpState fallocate_bad_fd{};
        af::IoOpState rename_null_old{};
        af::IoOpState rename_null_new{};
        af::IoOpState unlink_null_path{};
        af::IoOpState openat2_null_path{};
        af::IoOpState openat2_null_how{};
        af::IoOpState openat2_null_output{};
        af::IoOpState mkdir_null_path{};
        af::IoOpState symlink_null_target{};
        af::IoOpState symlink_null_path{};
        af::IoOpState link_null_old{};
        af::IoOpState link_null_new{};
        af::IoOpState ftruncate_bad_fd{};
        struct statx stat{};
        struct open_how how{};
        how.flags = O_RDONLY | O_CLOEXEC;
        int opened = -1;
        const af::IoStatus null_path_status = af::io_openat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            O_RDONLY | O_CLOEXEC,
            0,
            &opened,
            null_path);
        const af::IoStatus null_output_status = af::io_openat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            nullptr,
            null_output);
        const af::IoStatus no_uring_status = af::io_openat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            &opened,
            no_uring);
        af::UniqueFd event = af::make_eventfd();
        if (!event) {
            return failed();
        }
        const af::IoStatus close_no_uring_status =
            af::io_close(*this, IoTestThread::IO_0, event, close_no_uring);
        const af::IoStatus stat_no_uring_status = af::io_statx(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary",
            0,
            STATX_SIZE,
            &stat,
            stat_no_uring);
        const af::IoStatus fallocate_no_uring_status = af::io_fallocate(
            *this,
            IoTestThread::IO_0,
            event.get(),
            FALLOC_FL_KEEP_SIZE,
            0,
            4096,
            fallocate_no_uring);
        const af::IoStatus rename_no_uring_status = af::io_renameat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary-old",
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary-new",
            0,
            rename_no_uring);
        const af::IoStatus unlink_no_uring_status = af::io_unlinkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary",
            0,
            unlink_no_uring);
        const af::IoStatus openat2_no_uring_status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat2-boundary",
            &how,
            &opened,
            openat2_no_uring);
        const af::IoStatus mkdir_no_uring_status = af::io_mkdirat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-mkdirat-boundary",
            0700U,
            mkdir_no_uring);
        const af::IoStatus symlink_no_uring_status = af::io_symlinkat(
            *this,
            IoTestThread::IO_0,
            "/tmp/asyncflow-symlinkat-target",
            AT_FDCWD,
            "/tmp/asyncflow-symlinkat-boundary",
            symlink_no_uring);
        const af::IoStatus link_no_uring_status = af::io_linkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-linkat-old",
            AT_FDCWD,
            "/tmp/asyncflow-linkat-new",
            0,
            link_no_uring);
        const af::IoStatus ftruncate_no_uring_status =
            af::io_ftruncate(*this, IoTestThread::IO_0, event.get(), 0, ftruncate_no_uring);
        const af::IoStatus stat_null_path_status = af::io_statx(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            0,
            STATX_SIZE,
            &stat,
            stat_null_path);
        const af::IoStatus stat_null_output_status = af::io_statx(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary",
            0,
            STATX_SIZE,
            nullptr,
            stat_null_output);
        const af::IoStatus fallocate_bad_fd_status = af::io_fallocate(
            *this,
            IoTestThread::IO_0,
            -1,
            FALLOC_FL_KEEP_SIZE,
            0,
            4096,
            fallocate_bad_fd);
        const af::IoStatus rename_null_old_status = af::io_renameat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary-new",
            0,
            rename_null_old);
        const af::IoStatus rename_null_new_status = af::io_renameat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary-old",
            AT_FDCWD,
            nullptr,
            0,
            rename_null_new);
        const af::IoStatus unlink_null_path_status = af::io_unlinkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            0,
            unlink_null_path);
        const af::IoStatus openat2_null_path_status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            &how,
            &opened,
            openat2_null_path);
        const af::IoStatus openat2_null_how_status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat2-boundary",
            nullptr,
            &opened,
            openat2_null_how);
        const af::IoStatus openat2_null_output_status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat2-boundary",
            &how,
            nullptr,
            openat2_null_output);
        const af::IoStatus mkdir_null_path_status = af::io_mkdirat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            0700U,
            mkdir_null_path);
        const af::IoStatus symlink_null_target_status = af::io_symlinkat(
            *this,
            IoTestThread::IO_0,
            nullptr,
            AT_FDCWD,
            "/tmp/asyncflow-symlinkat-boundary",
            symlink_null_target);
        const af::IoStatus symlink_null_path_status = af::io_symlinkat(
            *this,
            IoTestThread::IO_0,
            "/tmp/asyncflow-symlinkat-target",
            AT_FDCWD,
            nullptr,
            symlink_null_path);
        const af::IoStatus link_null_old_status = af::io_linkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            AT_FDCWD,
            "/tmp/asyncflow-linkat-new",
            0,
            link_null_old);
        const af::IoStatus link_null_new_status = af::io_linkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-linkat-old",
            AT_FDCWD,
            nullptr,
            0,
            link_null_new);
        const af::IoStatus ftruncate_bad_fd_status =
            af::io_ftruncate(*this, IoTestThread::IO_0, -1, 0, ftruncate_bad_fd);
        if (!null_path_status.failed() || null_path_status.error != EINVAL ||
            !null_output_status.failed() || null_output_status.error != EINVAL ||
            !no_uring_status.failed() || no_uring_status.error != ENOSYS ||
            !close_no_uring_status.failed() || close_no_uring_status.error != ENOSYS ||
            event.get() < 0 ||
            !stat_no_uring_status.failed() || stat_no_uring_status.error != ENOSYS ||
            !fallocate_no_uring_status.failed() || fallocate_no_uring_status.error != ENOSYS ||
            !rename_no_uring_status.failed() || rename_no_uring_status.error != ENOSYS ||
            !unlink_no_uring_status.failed() || unlink_no_uring_status.error != ENOSYS ||
            !openat2_no_uring_status.failed() || openat2_no_uring_status.error != ENOSYS ||
            !mkdir_no_uring_status.failed() || mkdir_no_uring_status.error != ENOSYS ||
            !symlink_no_uring_status.failed() || symlink_no_uring_status.error != ENOSYS ||
            !link_no_uring_status.failed() || link_no_uring_status.error != ENOSYS ||
            !ftruncate_no_uring_status.failed() || ftruncate_no_uring_status.error != ENOSYS ||
            !stat_null_path_status.failed() || stat_null_path_status.error != EINVAL ||
            !stat_null_output_status.failed() || stat_null_output_status.error != EINVAL ||
            !fallocate_bad_fd_status.failed() || fallocate_bad_fd_status.error != EBADF ||
            !rename_null_old_status.failed() || rename_null_old_status.error != EINVAL ||
            !rename_null_new_status.failed() || rename_null_new_status.error != EINVAL ||
            !unlink_null_path_status.failed() || unlink_null_path_status.error != EINVAL ||
            !openat2_null_path_status.failed() || openat2_null_path_status.error != EINVAL ||
            !openat2_null_how_status.failed() || openat2_null_how_status.error != EINVAL ||
            !openat2_null_output_status.failed() || openat2_null_output_status.error != EINVAL ||
            !mkdir_null_path_status.failed() || mkdir_null_path_status.error != EINVAL ||
            !symlink_null_target_status.failed() || symlink_null_target_status.error != EINVAL ||
            !symlink_null_path_status.failed() || symlink_null_path_status.error != EINVAL ||
            !link_null_old_status.failed() || link_null_old_status.error != EINVAL ||
            !link_null_new_status.failed() || link_null_new_status.error != EINVAL ||
            !ftruncate_bad_fd_status.failed() || ftruncate_bad_fd_status.error != EBADF ||
            opened != -1) {
            return failed();
        }
        error_->store(no_uring_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class SocketLifecycleSetupTask final : public IoTaskBase {
public:
    explicit SocketLifecycleSetupTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<int>* reuse_value,
        std::atomic<int>* local_port,
        std::atomic<std::uint16_t>* ran_on) {
        completed_ = completed;
        error_ = error;
        reuse_value_ = reuse_value;
        local_port_ = local_port;
        ran_on_ = ran_on;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        CreateSocket,
        FinishSocket,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::CreateSocket:
            return create_socket();
        case State::FinishSocket:
            return finish_socket();
        }
        return failed();
    }

    af::TaskResult create_socket() {
        state_ = State::FinishSocket;
        return consume_socket_status(af::io_socket(
            *this,
            IoTestThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &opened_fd_,
            socket_));
    }

    af::TaskResult finish_socket() {
        return consume_socket_status(af::io_socket(
            *this,
            IoTestThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &opened_fd_,
            socket_));
    }

    af::TaskResult consume_socket_status(const af::IoStatus status) {
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || opened_fd_ < 0) {
            return complete(status.failed() ? status.error : EIO);
        }
        owned_.reset(opened_fd_);
        opened_fd_ = -1;
        listener_.reset(IoTestThread::IO_0, owned_.get());
        return configure_listener();
    }

    af::TaskResult configure_listener() {
        ran_on_->store(IoRuntime::current_thread_index(), std::memory_order_release);

        const int one = 1;
        const af::IoStatus set_status = listener_.setsockopt(
            *this,
            SOL_SOCKET,
            SO_REUSEADDR,
            &one,
            sizeof(one));
        if (!set_status.ready()) {
            return complete(set_status.error);
        }

        int reuse = 0;
        socklen_t reuse_size = sizeof(reuse);
        const af::IoStatus get_status = listener_.getsockopt(
            *this,
            SOL_SOCKET,
            SO_REUSEADDR,
            &reuse,
            &reuse_size);
        if (!get_status.ready()) {
            return complete(get_status.error);
        }
        reuse_value_->store(reuse, std::memory_order_release);

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        const af::IoStatus bind_status = listener_.bind(
            *this,
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address));
        if (!bind_status.ready()) {
            return complete(bind_status.error);
        }

        const af::IoStatus listen_status = listener_.listen(*this, 16);
        if (!listen_status.ready()) {
            return complete(listen_status.error);
        }

        sockaddr_in local{};
        socklen_t local_size = sizeof(local);
        const af::IoStatus name_status = listener_.getsockname(
            *this,
            reinterpret_cast<sockaddr*>(&local),
            &local_size);
        if (!name_status.ready()) {
            return complete(name_status.error);
        }
        if (local.sin_family != AF_INET || local.sin_port == 0 || local_size == 0U) {
            return complete(EIO);
        }
        local_port_->store(static_cast<int>(ntohs(local.sin_port)), std::memory_order_release);
        return complete(0);
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::CreateSocket};
    af::IoOpState socket_{};
    int opened_fd_{-1};
    af::UniqueFd owned_{};
    af::TcpListener<IoTestThread> listener_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<int>* reuse_value_{nullptr};
    std::atomic<int>* local_port_{nullptr};
    std::atomic<std::uint16_t>* ran_on_{nullptr};
};

class SocketLifecycleBoundaryTask final : public IoTaskBase {
public:
    explicit SocketLifecycleBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState null_socket{};
        int one = 1;
        int opened = -1;
        int value = 0;
        socklen_t value_size = sizeof(value);
        sockaddr_storage name{};
        socklen_t name_size = sizeof(name);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;

        const af::IoStatus null_socket_status = af::io_socket(
            *this,
            IoTestThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            nullptr,
            null_socket);
        const af::IoStatus bad_setsockopt_status = af::io_setsockopt(
            *this,
            IoTestThread::IO_0,
            -1,
            SOL_SOCKET,
            SO_REUSEADDR,
            &one,
            sizeof(one));
        const af::IoStatus null_setsockopt_status = af::io_setsockopt(
            *this,
            IoTestThread::IO_0,
            0,
            SOL_SOCKET,
            SO_REUSEADDR,
            nullptr,
            sizeof(one));
        const af::IoStatus bad_getsockopt_status = af::io_getsockopt(
            *this,
            IoTestThread::IO_0,
            -1,
            SOL_SOCKET,
            SO_REUSEADDR,
            &value,
            &value_size);
        const af::IoStatus null_getsockopt_status = af::io_getsockopt(
            *this,
            IoTestThread::IO_0,
            0,
            SOL_SOCKET,
            SO_REUSEADDR,
            nullptr,
            &value_size);
        const af::IoStatus bad_getsockname_status = af::io_getsockname(
            *this,
            IoTestThread::IO_0,
            -1,
            reinterpret_cast<sockaddr*>(&name),
            &name_size);
        const af::IoStatus null_getsockname_status = af::io_getsockname(
            *this,
            IoTestThread::IO_0,
            0,
            nullptr,
            &name_size);
        const af::IoStatus bad_getpeername_status = af::io_getpeername(
            *this,
            IoTestThread::IO_0,
            -1,
            reinterpret_cast<sockaddr*>(&name),
            &name_size);
        const af::IoStatus null_getpeername_status = af::io_getpeername(
            *this,
            IoTestThread::IO_0,
            0,
            reinterpret_cast<sockaddr*>(&name),
            nullptr);
        const af::IoStatus bad_bind_status = af::io_bind(
            *this,
            IoTestThread::IO_0,
            -1,
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address));
        const af::IoStatus null_bind_status = af::io_bind(
            *this,
            IoTestThread::IO_0,
            0,
            nullptr,
            sizeof(address));
        const af::IoStatus bad_listen_status =
            af::io_listen(*this, IoTestThread::IO_0, -1, 16);

        af::UniqueFd temp(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        if (!temp) {
            return complete(EIO);
        }
        const af::IoStatus wrong_thread_status =
            af::io_listen(*this, IoTestThread::Logic_0, temp.get(), 16);
        const af::IoStatus wrong_name_thread_status = af::io_getsockname(
            *this,
            IoTestThread::Logic_0,
            temp.get(),
            reinterpret_cast<sockaddr*>(&name),
            &name_size);

        const bool ok =
            null_socket_status.failed() && null_socket_status.error == EINVAL &&
            bad_setsockopt_status.failed() && bad_setsockopt_status.error == EBADF &&
            null_setsockopt_status.failed() && null_setsockopt_status.error == EINVAL &&
            bad_getsockopt_status.failed() && bad_getsockopt_status.error == EBADF &&
            null_getsockopt_status.failed() && null_getsockopt_status.error == EINVAL &&
            bad_getsockname_status.failed() && bad_getsockname_status.error == EBADF &&
            null_getsockname_status.failed() && null_getsockname_status.error == EINVAL &&
            bad_getpeername_status.failed() && bad_getpeername_status.error == EBADF &&
            null_getpeername_status.failed() && null_getpeername_status.error == EINVAL &&
            bad_bind_status.failed() && bad_bind_status.error == EBADF &&
            null_bind_status.failed() && null_bind_status.error == EINVAL &&
            bad_listen_status.failed() && bad_listen_status.error == EBADF &&
            wrong_thread_status.failed() && wrong_thread_status.error == EINVAL &&
            wrong_name_thread_status.failed() && wrong_name_thread_status.error == EINVAL &&
            opened == -1;
        return complete(ok ? 0 : EIO);
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class UringSocketCreateTask final : public UringIoTaskBase {
public:
    explicit UringSocketCreateTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        CreateSocket,
        FinishSocket,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::CreateSocket:
            return create_socket();
        case State::FinishSocket:
            return finish_socket();
        }
        return failed();
    }

    af::TaskResult create_socket() {
        state_ = State::FinishSocket;
        return consume_socket_status(af::io_socket(
            *this,
            IoTestThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &opened_fd_,
            socket_));
    }

    af::TaskResult finish_socket() {
        return consume_socket_status(af::io_socket(
            *this,
            IoTestThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &opened_fd_,
            socket_));
    }

    af::TaskResult consume_socket_status(const af::IoStatus status) {
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || opened_fd_ < 0) {
            return complete(status.failed() ? status.error : EIO);
        }
        af::UniqueFd fd(opened_fd_);
        opened_fd_ = -1;
        return complete(0);
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::CreateSocket};
    af::IoOpState socket_{};
    int opened_fd_{-1};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class PendingSocketWaitTask final : public FastIoTaskBase {
public:
    explicit PendingSocketWaitTask(FastIoTaskBase::FactoryToken token) : FastIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* armed) {
        fd_ = fd;
        armed_ = armed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        if (!wait_io(IoTestThread::IO_0, fd_, af::io_readable, &result_)) {
            return failed();
        }
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    int fd_{-1};
    af::IoResult result_{};
    std::atomic<int>* armed_{nullptr};
};

class FastIoDoneTask final : public FastIoTaskBase {
public:
    explicit FastIoDoneTask(FastIoTaskBase::FactoryToken token) : FastIoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed) {
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
};

void close_pair(int fds[2]) {
    if (fds[0] >= 0) {
        ::close(fds[0]);
    }
    if (fds[1] >= 0) {
        ::close(fds[1]);
    }
}

bool fill_until_blocked(int fd) {
    char data[4096]{};
    bool blocked = false;
    for (;;) {
        const ssize_t n = ::write(fd, data, sizeof(data));
        if (n > 0) {
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            blocked = true;
        }
        break;
    }
    return blocked;
}

void drain_available(int fd) {
    char data[4096]{};
    for (;;) {
        const ssize_t n = ::read(fd, data, sizeof(data));
        if (n > 0) {
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
}

bool read_exact_until(int fd, char* output, std::size_t size) {
    std::size_t offset = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (offset < size) {
        const ssize_t n = ::read(fd, output + offset, size - offset);
        if (n > 0) {
            offset += static_cast<std::size_t>(n);
            continue;
        }
        if (n == 0) {
            return false;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return false;
        }
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

bool write_exact_until(int fd, const char* input, std::size_t size) {
    std::size_t offset = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (offset < size) {
        const ssize_t n = ::write(fd, input + offset, size - offset);
        if (n > 0) {
            offset += static_cast<std::size_t>(n);
            continue;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != ENOTCONN) {
            return false;
        }
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

bool create_tcp_listener(int& listener, sockaddr_in& address, socklen_t& address_size) {
    listener = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listener < 0) {
        return false;
    }

    address = sockaddr_in{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
        ::listen(listener, 16) != 0) {
        ::close(listener);
        listener = -1;
        return false;
    }

    address_size = sizeof(address);
    if (::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_size) != 0) {
        ::close(listener);
        listener = -1;
        return false;
    }
    return true;
}

int accept_tcp_until_ready(int listener) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    for (;;) {
        const int fd = ::accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd >= 0) {
            return fd;
        }
        if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            return -1;
        }
        if (std::chrono::steady_clock::now() > deadline) {
            return -1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void close_fd(int& fd) {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}
#endif

} // namespace
