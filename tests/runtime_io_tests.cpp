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
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {

template <typename T>
bool wait_until_at_least(std::atomic<T>& value, T expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

enum class IoTestThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct IoTestRuntimeTraits {
    using Thread = IoTestThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(IoTestThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(IoTestThread thread) noexcept {
        return thread == IoTestThread::IO_0 ? af::ThreadKind::Epoll : af::ThreadKind::Worker;
    }
};

using IoRuntime = af::AsyncRuntime<IoTestRuntimeTraits>;
using IoTaskBase = IoRuntime::Task;

struct FastIoRuntimeTraits {
    using Thread = IoTestThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(IoTestThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;

    static constexpr af::ThreadKind thread_kind(IoTestThread thread) noexcept {
        return thread == IoTestThread::IO_0 ? af::ThreadKind::Epoll : af::ThreadKind::Worker;
    }
};

using FastIoRuntime = af::AsyncRuntime<FastIoRuntimeTraits>;
using FastIoTaskBase = FastIoRuntime::Task;

struct UringIoRuntimeTraits {
    using Thread = IoTestThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(IoTestThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(IoTestThread thread) noexcept {
        return thread == IoTestThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using UringIoRuntime = af::AsyncRuntime<UringIoRuntimeTraits>;
using UringIoTaskBase = UringIoRuntime::Task;

TEST(IoAdapterTraits, AdaptersAreThinTriviallyCopyableViews) {
    EXPECT_TRUE(std::is_trivially_copyable_v<af::IoFile<IoTestThread>>);
    EXPECT_TRUE(std::is_trivially_copyable_v<af::IoFixedFile<IoTestThread>>);
    EXPECT_TRUE(std::is_trivially_copyable_v<af::TcpStream<IoTestThread>>);
    EXPECT_TRUE(std::is_trivially_copyable_v<af::TcpListener<IoTestThread>>);
    EXPECT_TRUE(std::is_trivially_copyable_v<af::UdpSocket<IoTestThread>>);
    EXPECT_TRUE(std::is_trivially_copyable_v<af::IoEvent<IoTestThread>>);
    EXPECT_TRUE(std::is_trivially_copyable_v<af::IoTimer<IoTestThread>>);
    EXPECT_LE(sizeof(af::IoFile<IoTestThread>), 8U);
    EXPECT_LE(sizeof(af::IoFixedFile<IoTestThread>), 8U);
    EXPECT_LE(sizeof(af::TcpStream<IoTestThread>), 8U);
    EXPECT_LE(sizeof(af::TcpListener<IoTestThread>), 8U);
    EXPECT_LE(sizeof(af::UdpSocket<IoTestThread>), 8U);
    EXPECT_LE(sizeof(af::IoEvent<IoTestThread>), 8U);
    EXPECT_LE(sizeof(af::IoTimer<IoTestThread>), 8U);
}

class IoRuntimeFixture : public testing::Test {
protected:
    void SetUp() override {
        IoRuntime::init();
    }

    void TearDown() override {
        IoRuntime::shutdown();
    }
};

class UringIoRuntimeFixture : public testing::Test {
protected:
    void SetUp() override {
        UringIoRuntime::init();
    }

    void TearDown() override {
        UringIoRuntime::shutdown();
    }
};

class IoHopTask final : public IoTaskBase {
public:
    explicit IoHopTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<std::uint16_t>* ran_on) {
        completed_ = completed;
        ran_on_ = ran_on;
        return schedule(IoTestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Logic,
        Io,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Logic:
            state_ = State::Io;
            return pending_on(IoTestThread::IO_0);

        case State::Io:
            ran_on_->store(IoRuntime::current_thread_index(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        return failed();
    }

    State state_{State::Logic};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint16_t>* ran_on_{nullptr};
};

class WorkerIoWaitRejectedTask final : public IoTaskBase {
public:
    explicit WorkerIoWaitRejectedTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        af::IoResult result{};
        const bool ok = wait_io(IoTestThread::Logic_0, 0, af::io_readable, &result);
        if (ok) {
            return failed();
        }
        error_->store(result.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

#if defined(__linux__)
class SocketReadableTask final : public IoTaskBase {
public:
    explicit SocketReadableTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read) {
        fd_ = fd;
        armed_ = armed;
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Arm:
            return arm_read();

        case State::Read:
            return finish_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_ = State::Read;
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
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        byte_read_->store(value_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Arm};
    int fd_{-1};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class SocketRepeatedReadableTask final : public IoTaskBase {
public:
    explicit SocketRepeatedReadableTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* reads,
        char* output) {
        fd_ = fd;
        armed_ = armed;
        completed_ = completed;
        reads_ = reads;
        output_ = output;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Arm:
            return arm_read();

        case State::Read:
            return finish_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_ = State::Read;
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        return handle_status(status);
    }

    af::TaskResult finish_read() {
        const af::IoStatus status = af::io_read_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            read_);
        return handle_status(status);
    }

    af::TaskResult handle_status(af::IoStatus status) {
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }

        output_[read_count_] = value_;
        ++read_count_;
        reads_->store(static_cast<int>(read_count_), std::memory_order_release);
        if (read_count_ == expected_reads_) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        state_ = State::Arm;
        return arm_read();
    }

    static constexpr std::size_t expected_reads_{2};
    State state_{State::Arm};
    int fd_{-1};
    char value_{0};
    std::size_t read_count_{0};
    af::IoOpState read_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* reads_{nullptr};
    char* output_{nullptr};
};

class SocketWritableTask final : public IoTaskBase {
public:
    explicit SocketWritableTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int>* armed, std::atomic<int>* completed) {
        fd_ = fd;
        armed_ = armed;
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = af::io_write_some(
            *this,
            IoTestThread::IO_0,
            fd_,
            &value_,
            sizeof(value_),
            write_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    int fd_{-1};
    char value_{'w'};
    af::IoOpState write_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
};

class UdpRecvTask final : public IoTaskBase {
public:
    explicit UdpRecvTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<char>* byte_read,
        std::size_t expected_bytes = 1U) {
        socket_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        byte_read_ = byte_read;
        expected_bytes_ = expected_bytes;
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
        if (!status.ready() || status.bytes != expected_bytes_) {
            return failed();
        }
        if (status.bytes != 0U) {
            byte_read_->store(value_, std::memory_order_release);
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::UdpSocket<IoTestThread> socket_{};
    char value_{0};
    std::size_t expected_bytes_{1U};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};

class UdpSendToTask final : public IoTaskBase {
public:
    explicit UdpSendToTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

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

template <typename TaskBaseT>
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
        const af::IoStatus status = socket_.sendv_to_some(
            *this,
            iov_,
            2,
            reinterpret_cast<const sockaddr*>(&address_),
            address_size_,
            send_);
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
using UringUdpVectoredRecvTask = BasicUdpVectoredRecvTask<UringIoTaskBase>;
using UringUdpVectoredSendToTask = BasicUdpVectoredSendToTask<UringIoTaskBase>;

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
        af::IoOpState invalid_state{};
        af::IoOpState null_state{};
        af::IoOpState unavailable_state{};
        std::uint16_t buffer_id = 0;
        const af::IoStatus invalid_status =
            invalid_stream.recv_multishot(*this, 0, &buffer_id, invalid_state);
        const af::IoStatus null_status =
            stream_.recv_multishot(*this, 0, nullptr, null_state);
        const af::IoStatus unavailable_status =
            stream_.recv_multishot(*this, 0, &buffer_id, unavailable_state);
        if (!invalid_status.failed() || invalid_status.error != EBADF ||
            !null_status.failed() || null_status.error != EINVAL ||
            !unavailable_status.failed() || unavailable_status.error != ENOSYS) {
            return failed();
        }

        invalid_error_->store(invalid_status.error, std::memory_order_release);
        null_error_->store(null_status.error, std::memory_order_release);
        unavailable_error_->store(unavailable_status.error, std::memory_order_release);
        register_error_->store(register_error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
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
        af::IoOpState splice_zero{};
        af::IoOpState splice_bad{};
        af::IoOffset offset = 0;
        const char value = 'Z';

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
        if (!unavailable_read.failed() || unavailable_read.error != ENOSYS ||
            !zero_read.ready() || zero_read.bytes != 0U ||
            !bad_read.failed() || bad_read.error != EBADF ||
            !null_read.failed() || null_read.error != EINVAL ||
            !fixed_unavailable_read.failed() || fixed_unavailable_read.error != ENOSYS ||
            !fixed_bad_read.failed() || fixed_bad_read.error != EBADF ||
            !fixed_null_read.failed() || fixed_null_read.error != EINVAL) {
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
        Fsync,
        Read,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_file();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

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
    af::IoOpState no_table_{};
    af::IoOpState bad_index_{};
    af::IoOpState no_buffer_{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
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
        stream_.reset(IoTestThread::IO_0, fd);
        target_reads_ = target_reads;
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
        const af::IoStatus status = stream_.recv_multishot(
            *this,
            buffer_group,
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
        const af::IoStatus status =
            stream_.recv_multishot(*this, buffer_group, &ignored, recv_);
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
    af::IoProvidedBufferRing ring_{};
    char buffers_[buffer_count]{};
    int target_reads_{0};
    int finish_error_{0};
    bool armed_once_{false};
    bool registered_{false};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* read_count_{nullptr};
    std::atomic<int>* packed_read_{nullptr};
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
        af::IoOpState recv_from{};
        af::IoOpState send_to{};
        af::IoOpState bad_file{};
        af::IoOpState bad_iov_state{};
        af::IoOpState bad_count_state{};
        af::IoOpState bad_datagram_state{};

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
        const af::IoStatus zero_recvv_from =
            datagram.recvv_from_some(*this, nullptr, 0, nullptr, nullptr, recv_from);
        const af::IoStatus zero_sendv_to =
            datagram.sendv_to_some(*this, nullptr, 0, nullptr, 0, send_to);
        const af::IoStatus bad_file_status =
            file.writev_at(*this, &valid_iov, 1, 0, bad_file);
        const af::IoStatus bad_iov =
            stream.sendv_some(*this, &invalid_iov, 1, bad_iov_state);
        const af::IoStatus bad_count =
            stream.recvv_some(*this, &valid_iov, -1, bad_count_state);
        const af::IoStatus bad_datagram =
            datagram.sendv_to_some(*this, &invalid_iov, 1, nullptr, 0, bad_datagram_state);

        if (!zero_readv.ready() || zero_readv.bytes != 0U ||
            !zero_writev.ready() || zero_writev.bytes != 0U ||
            !zero_readv_at.ready() || zero_readv_at.bytes != 0U ||
            !zero_writev_at.ready() || zero_writev_at.bytes != 0U ||
            !zero_recvv.ready() || zero_recvv.bytes != 0U ||
            !zero_sendv.ready() || zero_sendv.bytes != 0U ||
            !zero_recvv_from.ready() || zero_recvv_from.bytes != 0U ||
            !zero_sendv_to.ready() || zero_sendv_to.bytes != 0U ||
            !bad_file_status.failed() || bad_file_status.error != EBADF ||
            !bad_iov.failed() || bad_iov.error != EINVAL ||
            !bad_count.failed() || bad_count.error != EINVAL ||
            !bad_datagram.failed() || bad_datagram.error != EINVAL) {
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
        af::IoOpState stat_null_path{};
        af::IoOpState stat_null_output{};
        af::IoOpState fallocate_bad_fd{};
        af::IoOpState rename_null_old{};
        af::IoOpState rename_null_new{};
        af::IoOpState unlink_null_path{};
        struct statx stat{};
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
        if (!null_path_status.failed() || null_path_status.error != EINVAL ||
            !null_output_status.failed() || null_output_status.error != EINVAL ||
            !no_uring_status.failed() || no_uring_status.error != ENOSYS ||
            !close_no_uring_status.failed() || close_no_uring_status.error != ENOSYS ||
            event.get() < 0 ||
            !stat_no_uring_status.failed() || stat_no_uring_status.error != ENOSYS ||
            !fallocate_no_uring_status.failed() || fallocate_no_uring_status.error != ENOSYS ||
            !rename_no_uring_status.failed() || rename_no_uring_status.error != ENOSYS ||
            !unlink_no_uring_status.failed() || unlink_no_uring_status.error != ENOSYS ||
            !stat_null_path_status.failed() || stat_null_path_status.error != EINVAL ||
            !stat_null_output_status.failed() || stat_null_output_status.error != EINVAL ||
            !fallocate_bad_fd_status.failed() || fallocate_bad_fd_status.error != EBADF ||
            !rename_null_old_status.failed() || rename_null_old_status.error != EINVAL ||
            !rename_null_new_status.failed() || rename_null_new_status.error != EINVAL ||
            !unlink_null_path_status.failed() || unlink_null_path_status.error != EINVAL ||
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

TEST_F(IoRuntimeFixture, IoThreadUsesConfiguredThreadKindAndAcceptsTasks) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{IoRuntime::invalid_thread_index};

    ASSERT_TRUE(IoRuntime::start_task<IoHopTask>(&completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), IoRuntime::thread_index(IoTestThread::IO_0));
}

TEST_F(IoRuntimeFixture, WorkerThreadDoesNotExposeIoBackend) {
    EXPECT_FALSE(IoRuntime::io_backend_available(IoTestThread::Logic_0));

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<WorkerIoWaitRejectedTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_NE(error.load(std::memory_order_acquire), 0);
}

TEST_F(IoRuntimeFixture, EpollIoThreadResumesTaskWhenFdBecomesReadable) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};

    ASSERT_TRUE(IoRuntime::start_task<SocketReadableTask>(fds[0], &armed, &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 'x';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadRearmsReadableFdWithSameState) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> reads{0};
    char output[2]{};

    ASSERT_TRUE(IoRuntime::start_task<SocketRepeatedReadableTask>(
        fds[0],
        &armed,
        &completed,
        &reads,
        output));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char first = 'a';
    ASSERT_EQ(::write(fds[1], &first, sizeof(first)), 1);
    ASSERT_TRUE(wait_until_at_least(reads, 1));
    ASSERT_TRUE(wait_until_at_least(armed, 2));

    const char second = 'b';
    ASSERT_EQ(::write(fds[1], &second, sizeof(second)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(output[0], first);
    EXPECT_EQ(output[1], second);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadCancelsPendingReadWait) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> read_completed{0};
    std::atomic<int> read_error{0};
    std::atomic<int> cancel_completed{0};
    std::atomic<int> first_cancel{0};
    std::atomic<int> second_cancel{-1};
    std::atomic<int> cancel_error{0};

    ASSERT_TRUE(IoRuntime::start_task<CancellableSocketReadTask>(
        fds[0],
        &state,
        &armed,
        &read_completed,
        &read_error));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    ASSERT_TRUE(IoRuntime::start_task<CancelIoStateTask>(
        &state,
        true,
        &cancel_completed,
        &first_cancel,
        &second_cancel,
        &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));
    ASSERT_TRUE(wait_until_at_least(read_completed, 1));

    EXPECT_EQ(first_cancel.load(std::memory_order_acquire), 1);
    EXPECT_EQ(second_cancel.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(read_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(read_completed.load(std::memory_order_acquire), 1);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadRejectsCancelForIdleState) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> result{-1};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<CancelIdleIoStateTask>(&completed, &result, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(result.load(std::memory_order_acquire), 0);
    EXPECT_EQ(error.load(std::memory_order_acquire), ENOENT);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadTimesOutPendingRead) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<TimeoutSocketReadTask>(
        fds[0],
        std::chrono::milliseconds(1),
        &state,
        &armed,
        &completed,
        &error,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), ETIMEDOUT);
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), char{0});

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadCancelsTimeoutWhenReadCompletes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<TimeoutSocketReadTask>(
        fds[0],
        std::chrono::seconds(1),
        &state,
        &armed,
        &completed,
        &error,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 't';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadCancelsTimeoutWhenIoIsCanceled) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> read_completed{0};
    std::atomic<int> read_error{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<TimeoutSocketReadTask>(
        fds[0],
        std::chrono::milliseconds(20),
        &state,
        &armed,
        &read_completed,
        &read_error,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    std::atomic<int> cancel_completed{0};
    std::atomic<int> first_cancel{0};
    std::atomic<int> second_cancel{-1};
    std::atomic<int> cancel_error{0};
    ASSERT_TRUE(IoRuntime::start_task<CancelIoStateTask>(
        &state,
        false,
        &cancel_completed,
        &first_cancel,
        &second_cancel,
        &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));
    ASSERT_TRUE(wait_until_at_least(read_completed, 1));

    EXPECT_EQ(first_cancel.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(read_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), char{0});

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(read_completed.load(std::memory_order_acquire), 1);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, IoHelpersHandleInvalidAndZeroByteOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> bad_fd_completed{0};
    std::atomic<int> bad_fd_error{0};
    ASSERT_TRUE(IoRuntime::start_task<BadFdReadTask>(&bad_fd_completed, &bad_fd_error));
    ASSERT_TRUE(wait_until_at_least(bad_fd_completed, 1));
    EXPECT_EQ(bad_fd_error.load(std::memory_order_acquire), EBADF);

    std::atomic<int> zero_completed{0};
    ASSERT_TRUE(IoRuntime::start_task<ZeroByteIoTask>(&zero_completed));
    ASSERT_TRUE(wait_until_at_least(zero_completed, 1));
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, ZeroCopyHelpersHandleInvalidAndZeroCountOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<ZeroCopyBoundaryTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), EBADF);
#else
    GTEST_SKIP() << "zero-copy helpers are Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, FileAdapterHandlesInvalidAndZeroByteOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<FileAdapterBoundaryTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), EBADF);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, FixedBufferHelpersHandleInvalidAndUnavailableBackend) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<FixedBufferBoundaryTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), EBADF);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, FixedFileHelpersHandleInvalidAndUnavailableBackend) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> register_error{0};
    std::atomic<int> unavailable_error{0};
    std::atomic<int> invalid_error{0};
    std::atomic<int> null_error{0};
    ASSERT_TRUE(IoRuntime::start_task<FixedFileBoundaryTask>(
        &completed,
        &register_error,
        &unavailable_error,
        &invalid_error,
        &null_error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(register_error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(unavailable_error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(invalid_error.load(std::memory_order_acquire), EBADF);
    EXPECT_EQ(null_error.load(std::memory_order_acquire), EINVAL);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, VectoredHelpersHandleInvalidAndZeroLengthOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    ASSERT_TRUE(IoRuntime::start_task<VectoredBoundaryTask>(&completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, TimerFdAdapterHandlesInvalidOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int error = 0;
    EXPECT_FALSE(af::arm_timerfd_after(-1, std::chrono::milliseconds(1), error));
    EXPECT_EQ(error, EBADF);
    EXPECT_FALSE(af::arm_timerfd_after(0, std::chrono::nanoseconds{0}, error));
    EXPECT_EQ(error, EINVAL);

    std::atomic<int> completed{0};
    std::atomic<int> task_error{0};
    ASSERT_TRUE(IoRuntime::start_task<TimerBoundaryTask>(&completed, &task_error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(task_error.load(std::memory_order_acquire), EBADF);
#else
    GTEST_SKIP() << "timerfd is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EventFdAdapterHandlesInvalidOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int error = 0;
    EXPECT_FALSE(af::write_eventfd(-1, 1, error));
    EXPECT_EQ(error, EBADF);

    af::UniqueFd event = af::make_eventfd();
    ASSERT_TRUE(event);
    EXPECT_FALSE(af::write_eventfd(
        event.get(),
        std::numeric_limits<std::uint64_t>::max(),
        error));
    EXPECT_EQ(error, EINVAL);

    std::atomic<int> completed{0};
    std::atomic<int> task_error{0};
    ASSERT_TRUE(IoRuntime::start_task<EventBoundaryTask>(&completed, &task_error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(task_error.load(std::memory_order_acquire), EBADF);
#else
    GTEST_SKIP() << "eventfd is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, OpenAtHelperHandlesInvalidOperations) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<OpenAtBoundaryTask>(&completed, &error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), ENOSYS);
#else
    GTEST_SKIP() << "openat helper is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadResumesTimerFdFromAdapter) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    af::UniqueFd timer = af::make_timerfd();
    ASSERT_TRUE(timer);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<std::uint64_t> expirations{0};
    ASSERT_TRUE(IoRuntime::start_task<TimerFdTask>(
        timer.get(),
        &armed,
        &completed,
        &expirations));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int error = 0;
    ASSERT_TRUE(af::arm_timerfd_after(timer.get(), std::chrono::milliseconds(1), error)) << error;
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_GE(expirations.load(std::memory_order_acquire), std::uint64_t{1});
#else
    GTEST_SKIP() << "timerfd is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadResumesEventFdFromAdapter) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    af::UniqueFd event = af::make_eventfd();
    ASSERT_TRUE(event);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<std::uint64_t> value{0};
    ASSERT_TRUE(IoRuntime::start_task<EventFdTask>(
        event.get(),
        &armed,
        &completed,
        &value));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int error = 0;
    ASSERT_TRUE(af::write_eventfd(event.get(), 7, error)) << error;
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(value.load(std::memory_order_acquire), std::uint64_t{7});
#else
    GTEST_SKIP() << "eventfd is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, StreamAdapterReceivesAndSendsSocketBytes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<StreamAdapterEchoTask>(
        fds[0],
        &armed,
        &completed,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char request = 'Q';
    ASSERT_EQ(::write(fds[1], &request, sizeof(request)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), request);

    char response = 0;
    ASSERT_EQ(::read(fds[1], &response, sizeof(response)), 1);
    EXPECT_EQ(response, 'R');

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, StreamAdapterReceivesAndSendsVectoredSocketBytes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> request_seen{0};
    ASSERT_TRUE(IoRuntime::start_task<StreamVectoredEchoTask>(
        fds[0],
        &armed,
        &completed,
        &request_seen));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char request[2]{'A', 'B'};
    ASSERT_EQ(::write(fds[1], request, sizeof(request)), 2);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(request_seen.load(std::memory_order_acquire), ('A' << 8) | 'B');

    char response[2]{};
    ASSERT_EQ(::read(fds[1], response, sizeof(response)), 2);
    EXPECT_EQ(response[0], 'X');
    EXPECT_EQ(response[1], 'Y');

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, StreamAdapterSendZcSendsSocketBytes) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    const char payload[] = "asyncflow-send-zc";
    constexpr std::size_t payload_size = sizeof(payload) - 1U;
    std::atomic<int> completed{0};
    std::atomic<int> calls{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<SendZcSocketTask>(
        fds[0],
        payload,
        payload_size,
        3,
        &completed,
        &calls,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), payload_size);
    EXPECT_GT(calls.load(std::memory_order_acquire), 1);

    char received[payload_size]{};
    ASSERT_TRUE(read_exact_until(fds[1], received, payload_size));
    EXPECT_EQ(std::memcmp(received, payload, payload_size), 0);

    close_pair(fds);
#else
    GTEST_SKIP() << "send_zc helper is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, SendZcWaitsForSocketWritableWhenBufferIsFull) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
    ASSERT_TRUE(fill_until_blocked(fds[0]));

    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<PendingSendZcTask>(
        fds[0],
        &pending_seen,
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));

    drain_available(fds[1]);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1U);

    close_pair(fds);
#else
    GTEST_SKIP() << "send_zc helper is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, StreamAdapterSendfileSendsFileToSocket) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    char path[] = "/tmp/asyncflow-sendfile-XXXXXX";
    int file = ::mkstemp(path);
    ASSERT_GE(file, 0);
    static_cast<void>(::unlink(path));
    const char payload[] = "asyncflow-sendfile";
    constexpr std::size_t payload_size = sizeof(payload) - 1U;
    ASSERT_EQ(::write(file, payload, payload_size), static_cast<ssize_t>(payload_size));
    ASSERT_EQ(::lseek(file, 0, SEEK_SET), 0);

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> completed{0};
    std::atomic<int> calls{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<SendfileSocketTask>(
        fds[0],
        file,
        payload_size,
        3,
        true,
        &completed,
        &calls,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), payload_size);
    EXPECT_GT(calls.load(std::memory_order_acquire), 1);

    char received[payload_size]{};
    ASSERT_TRUE(read_exact_until(fds[1], received, payload_size));
    EXPECT_EQ(std::memcmp(received, payload, payload_size), 0);

    close_pair(fds);
    close_fd(file);
#else
    GTEST_SKIP() << "sendfile is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, SendfileWaitsForSocketWritableWhenBufferIsFull) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    char path[] = "/tmp/asyncflow-sendfile-pending-XXXXXX";
    int file = ::mkstemp(path);
    ASSERT_GE(file, 0);
    static_cast<void>(::unlink(path));
    const char payload = 'P';
    ASSERT_EQ(::write(file, &payload, sizeof(payload)), static_cast<ssize_t>(sizeof(payload)));

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
    ASSERT_TRUE(fill_until_blocked(fds[0]));

    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<PendingSendfileTask>(
        fds[0],
        file,
        &pending_seen,
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));

    drain_available(fds[1]);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1U);

    close_pair(fds);
    close_fd(file);
#else
    GTEST_SKIP() << "sendfile is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringPollReadinessResumesSendfileWhenSocketWritable) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_poll_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring poll backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-poll-sendfile-XXXXXX";
    int file = ::mkstemp(path);
    ASSERT_GE(file, 0);
    static_cast<void>(::unlink(path));
    const char payload = 'R';
    ASSERT_EQ(::write(file, &payload, sizeof(payload)), static_cast<ssize_t>(sizeof(payload)));

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
    ASSERT_TRUE(fill_until_blocked(fds[0]));

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> wait_kind{-1};
    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringPendingSendfilePollTask>(
        fds[0],
        file,
        &state,
        &wait_kind,
        &pending_seen,
        &completed,
        &error,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));
    EXPECT_EQ(
        wait_kind.load(std::memory_order_acquire),
        static_cast<int>(af::IoWaitKind::Readiness));

    drain_available(fds[1]);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1U);

    close_pair(fds);
    close_fd(file);
#else
    GTEST_SKIP() << "io_uring poll backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringPollReadinessCancelPendingSendfileWait) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_poll_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring poll backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-poll-cancel-XXXXXX";
    int file = ::mkstemp(path);
    ASSERT_GE(file, 0);
    static_cast<void>(::unlink(path));
    const char payload = 'C';
    ASSERT_EQ(::write(file, &payload, sizeof(payload)), static_cast<ssize_t>(sizeof(payload)));

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
    ASSERT_TRUE(fill_until_blocked(fds[0]));

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> wait_kind{-1};
    std::atomic<int> pending_seen{0};
    std::atomic<int> completed{0};
    std::atomic<int> error{-1};
    std::atomic<std::size_t> bytes_sent{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringPendingSendfilePollTask>(
        fds[0],
        file,
        &state,
        &wait_kind,
        &pending_seen,
        &completed,
        &error,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(pending_seen, 1));
    ASSERT_EQ(
        wait_kind.load(std::memory_order_acquire),
        static_cast<int>(af::IoWaitKind::Readiness));

    std::atomic<int> cancel_completed{0};
    std::atomic<int> cancel_result{0};
    std::atomic<int> cancel_error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringCancelIoStateTask>(
        &state,
        &cancel_completed,
        &cancel_result,
        &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));
    EXPECT_EQ(cancel_result.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), 0);

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 0U);

    close_pair(fds);
    close_fd(file);
#else
    GTEST_SKIP() << "io_uring poll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, SpliceTransfersPipeContentWithNullOffsets) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int input[2]{-1, -1};
    int output[2]{-1, -1};
    ASSERT_EQ(::pipe2(input, O_NONBLOCK | O_CLOEXEC), 0);
    ASSERT_EQ(::pipe2(output, O_NONBLOCK | O_CLOEXEC), 0);

    const char payload[] = "splice-through-kernel";
    constexpr std::size_t payload_size = sizeof(payload) - 1U;
    ASSERT_EQ(::write(input[1], payload, payload_size), static_cast<ssize_t>(payload_size));

    std::atomic<int> completed{0};
    std::atomic<std::size_t> bytes_spliced{0};
    ASSERT_TRUE(IoRuntime::start_task<SplicePipeTask>(
        input[0],
        output[1],
        payload_size,
        &completed,
        &bytes_spliced));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_spliced.load(std::memory_order_acquire), payload_size);

    char received[payload_size]{};
    ASSERT_TRUE(read_exact_until(output[0], received, payload_size));
    EXPECT_EQ(std::memcmp(received, payload, payload_size), 0);

    close_pair(input);
    close_pair(output);
#else
    GTEST_SKIP() << "splice is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadAcceptsTcpConnectionFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    ASSERT_TRUE(IoRuntime::start_task<TcpAcceptTask>(listener, &armed, &completed));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    const int rc = ::connect(client, reinterpret_cast<sockaddr*>(&address), address_size);
    ASSERT_TRUE(rc == 0 || errno == EINPROGRESS);

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    close_fd(client);
    close_fd(listener);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, AcceptMultishotReportsInvalidAndUnavailableBackend) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    std::atomic<int> completed{0};
    std::atomic<int> invalid_error{0};
    std::atomic<int> null_error{0};
    std::atomic<int> address_error{0};
    std::atomic<int> unavailable_error{0};
    ASSERT_TRUE(IoRuntime::start_task<TcpAcceptMultishotBoundaryTask>(
        listener,
        &completed,
        &invalid_error,
        &null_error,
        &address_error,
        &unavailable_error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(invalid_error.load(std::memory_order_acquire), EBADF);
    EXPECT_EQ(null_error.load(std::memory_order_acquire), EINVAL);
    EXPECT_EQ(address_error.load(std::memory_order_acquire), EINVAL);
    EXPECT_EQ(unavailable_error.load(std::memory_order_acquire), ENOSYS);

    close_fd(listener);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, RecvMultishotReportsInvalidAndUnavailableBackend) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> completed{0};
    std::atomic<int> invalid_error{0};
    std::atomic<int> null_error{0};
    std::atomic<int> unavailable_error{0};
    std::atomic<int> register_error{0};
    ASSERT_TRUE(IoRuntime::start_task<RecvMultishotBoundaryTask>(
        fds[0],
        &completed,
        &invalid_error,
        &null_error,
        &unavailable_error,
        &register_error));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(invalid_error.load(std::memory_order_acquire), EBADF);
    EXPECT_EQ(null_error.load(std::memory_order_acquire), EINVAL);
    EXPECT_EQ(unavailable_error.load(std::memory_order_acquire), ENOSYS);
    EXPECT_EQ(register_error.load(std::memory_order_acquire), ENOSYS);

    close_pair(fds);
#else
    GTEST_SKIP() << "provided buffer recv_multishot is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadConnectsTcpStreamFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    int client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);

    std::atomic<int> completed{0};
    ASSERT_TRUE(IoRuntime::start_task<TcpConnectTask>(client, address, address_size, &completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    int accepted = accept_tcp_until_ready(listener);
    ASSERT_GE(accepted, 0);
    close_fd(accepted);
    close_fd(client);
    close_fd(listener);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadFallsBackToEpollReadiness) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringStreamFallbackTask>(
        fds[0],
        &armed,
        &completed,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 'U';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadCancelsPendingRecvCompletion) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<af::IoOpState*> state{nullptr};
    std::atomic<int> wait_kind{-1};
    std::atomic<int> armed{0};
    std::atomic<int> recv_completed{0};
    std::atomic<int> recv_error{0};
    std::atomic<int> recv_bytes{-1};
    ASSERT_TRUE(UringIoRuntime::start_task<UringCancellableSocketRecvTask>(
        fds[0],
        &state,
        &wait_kind,
        &armed,
        &recv_completed,
        &recv_error,
        &recv_bytes));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    if (wait_kind.load(std::memory_order_acquire) !=
        static_cast<int>(af::IoWaitKind::Completion)) {
        const char value = 'f';
        ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
        ASSERT_TRUE(wait_until_at_least(recv_completed, 1));
        close_pair(fds);
        GTEST_SKIP() << "recv did not remain as an io_uring completion operation";
    }

    std::atomic<int> cancel_completed{0};
    std::atomic<int> cancel_result{0};
    std::atomic<int> cancel_error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringCancelIoStateTask>(
        &state,
        &cancel_completed,
        &cancel_result,
        &cancel_error));
    ASSERT_TRUE(wait_until_at_least(cancel_completed, 1));

    if (!wait_until_at_least(recv_completed, 1)) {
        const char value = 'u';
        ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
        ASSERT_TRUE(wait_until_at_least(recv_completed, 1));
    }

    EXPECT_EQ(cancel_result.load(std::memory_order_acquire), 1);
    EXPECT_EQ(cancel_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(recv_error.load(std::memory_order_acquire), ECANCELED);
    EXPECT_EQ(recv_bytes.load(std::memory_order_acquire), -1);

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadHandlesTimerFdViaEpollFallback) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd timer = af::make_timerfd();
    ASSERT_TRUE(timer);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<std::uint64_t> expirations{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTimerFdTask>(
        timer.get(),
        &armed,
        &completed,
        &expirations));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int error = 0;
    ASSERT_TRUE(af::arm_timerfd_after(timer.get(), std::chrono::milliseconds(1), error)) << error;
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_GE(expirations.load(std::memory_order_acquire), std::uint64_t{1});
#else
    GTEST_SKIP() << "timerfd is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadHandlesEventFdViaEpollFallback) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd event = af::make_eventfd();
    ASSERT_TRUE(event);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<std::uint64_t> value{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringEventFdTask>(
        event.get(),
        &armed,
        &completed,
        &value));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int error = 0;
    ASSERT_TRUE(af::write_eventfd(event.get(), 9, error)) << error;
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(value.load(std::memory_order_acquire), std::uint64_t{9});
#else
    GTEST_SKIP() << "eventfd is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadSendsStreamBytesOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> completed{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringStreamSendTask>(fds[0], &completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    char value = 0;
    ASSERT_EQ(::read(fds[1], &value, sizeof(value)), 1);
    EXPECT_EQ(value, 'S');

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadSendZcSendsStreamBytesOrFallsBack) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> completed{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringStreamSendZcTask>(fds[0], &completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    char value = 0;
    ASSERT_EQ(::read(fds[1], &value, sizeof(value)), 1);
    EXPECT_EQ(value, 'Z');

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadHandlesVectoredStreamOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> request_seen{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringStreamVectoredTask>(
        fds[0],
        &armed,
        &completed,
        &request_seen));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char request[2]{'C', 'D'};
    ASSERT_EQ(::write(fds[1], request, sizeof(request)), 2);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(request_seen.load(std::memory_order_acquire), ('C' << 8) | 'D');

    char response[2]{};
    ASSERT_EQ(::read(fds[1], response, sizeof(response)), 2);
    EXPECT_EQ(response[0], 'U');
    EXPECT_EQ(response[1], 'V');

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadAcceptsTcpConnectionOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTcpAcceptTask>(listener, &armed, &completed));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    int client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);
    const int rc = ::connect(client, reinterpret_cast<sockaddr*>(&address), address_size);
    ASSERT_TRUE(rc == 0 || errno == EINPROGRESS);

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    close_fd(client);
    close_fd(listener);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringAcceptMultishotAcceptsMultipleConnections) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    constexpr int target_accepts = 2;
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> accepted_count{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTcpAcceptMultishotTask>(
        listener,
        target_accepts,
        &armed,
        &completed,
        &accepted_count,
        &error));

    if (!wait_until_at_least(armed, 1)) {
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        const int submit_error = error.load(std::memory_order_acquire);
        if (submit_error == EINVAL || submit_error == EOPNOTSUPP || submit_error == ENOSYS) {
            close_fd(listener);
            GTEST_SKIP() << "io_uring multishot accept unsupported";
        }
        FAIL() << "multishot accept was not armed, error=" << submit_error;
    }

    int clients[target_accepts]{-1, -1};
    for (int& client : clients) {
        client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        ASSERT_GE(client, 0);
        const int rc = ::connect(client, reinterpret_cast<sockaddr*>(&address), address_size);
        ASSERT_TRUE(rc == 0 || errno == EINPROGRESS);
    }

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), 0);
    EXPECT_EQ(accepted_count.load(std::memory_order_acquire), target_accepts);

    for (int client : clients) {
        close_fd(client);
    }
    close_fd(listener);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringRecvMultishotUsesProvidedBuffers) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    constexpr int target_reads = 2;
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> read_count{0};
    std::atomic<int> packed_read{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringRecvMultishotTask>(
        fds[0],
        target_reads,
        &armed,
        &completed,
        &read_count,
        &packed_read,
        &error));

    if (!wait_until_at_least(armed, 1)) {
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        const int setup_error = error.load(std::memory_order_acquire);
        if (setup_error == EINVAL ||
            setup_error == EOPNOTSUPP ||
            setup_error == ENOSYS ||
            setup_error == ENOBUFS) {
            close_pair(fds);
            GTEST_SKIP() << "io_uring provided buffer recv_multishot unsupported";
        }
        FAIL() << "recv_multishot was not armed, error=" << setup_error;
    }

    const char payload[] = {'A', 'B'};
    ASSERT_EQ(::write(fds[1], payload, sizeof(payload)), static_cast<ssize_t>(sizeof(payload)));

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    const int task_error = error.load(std::memory_order_acquire);
    if (task_error == EINVAL ||
        task_error == EOPNOTSUPP ||
        task_error == ENOSYS ||
        task_error == ENOBUFS) {
        close_pair(fds);
        GTEST_SKIP() << "io_uring provided buffer recv_multishot unsupported";
    }
    EXPECT_EQ(task_error, 0);
    EXPECT_EQ(read_count.load(std::memory_order_acquire), target_reads);
    EXPECT_EQ(
        packed_read.load(std::memory_order_acquire),
        (static_cast<int>('A') << 8) | static_cast<int>('B'));

    close_pair(fds);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadConnectsTcpStreamOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    ASSERT_TRUE(create_tcp_listener(listener, address, address_size));

    int client = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(client, 0);

    std::atomic<int> completed{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringTcpConnectTask>(
        client,
        address,
        address_size,
        &completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    int accepted = accept_tcp_until_ready(listener);
    ASSERT_GE(accepted, 0);
    close_fd(accepted);
    close_fd(client);
    close_fd(listener);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadReceivesUdpDatagramOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(receiver);
    af::UniqueFd sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(sender);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(
        ::getsockname(receiver.get(), reinterpret_cast<sockaddr*>(&address), &address_size),
        0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpRecvTask>(
        receiver.get(),
        &armed,
        &completed,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 'g';
    ASSERT_EQ(::sendto(
                  sender.get(),
                  &value,
                  sizeof(value),
                  0,
                  reinterpret_cast<sockaddr*>(&address),
                  address_size),
              1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadReceivesVectoredUdpDatagramOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(receiver);
    af::UniqueFd sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(sender);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(
        ::getsockname(receiver.get(), reinterpret_cast<sockaddr*>(&address), &address_size),
        0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> payload_seen{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpVectoredRecvTask>(
        receiver.get(),
        &armed,
        &completed,
        &payload_seen));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char payload[2]{'q', 'r'};
    ASSERT_EQ(::sendto(
                  sender.get(),
                  payload,
                  sizeof(payload),
                  0,
                  reinterpret_cast<sockaddr*>(&address),
                  address_size),
              2);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(payload_seen.load(std::memory_order_acquire), ('q' << 8) | 'r');
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadSendsUdpDatagramOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(receiver);
    af::UniqueFd sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(sender);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(
        ::getsockname(receiver.get(), reinterpret_cast<sockaddr*>(&address), &address_size),
        0);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    const char value = 'm';
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpSendToTask>(
        sender.get(),
        address,
        address_size,
        value,
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1);

    char received = 0;
    sockaddr_storage peer{};
    socklen_t peer_size = sizeof(peer);
    ASSERT_EQ(::recvfrom(
                  receiver.get(),
                  &received,
                  sizeof(received),
                  0,
                  reinterpret_cast<sockaddr*>(&peer),
                  &peer_size),
              1);
    EXPECT_EQ(received, value);
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadSendsVectoredUdpDatagramOrFallsBackToEpoll) {
#if defined(__linux__)
    if (!UringIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io backend unavailable";
    }

    af::UniqueFd receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(receiver);
    af::UniqueFd sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    ASSERT_TRUE(sender);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(
        ::getsockname(receiver.get(), reinterpret_cast<sockaddr*>(&address), &address_size),
        0);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringUdpVectoredSendToTask>(
        sender.get(),
        address,
        address_size,
        'x',
        'y',
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 2);

    char received[2]{};
    sockaddr_storage peer{};
    socklen_t peer_size = sizeof(peer);
    ASSERT_EQ(::recvfrom(
                  receiver.get(),
                  received,
                  sizeof(received),
                  0,
                  reinterpret_cast<sockaddr*>(&peer),
                  &peer_size),
              2);
    EXPECT_EQ(received[0], 'x');
    EXPECT_EQ(received[1], 'y');
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringFileAdapterWritesFsyncsAndReadsAtOffset) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFileReadWriteTask>(
        file.get(),
        &completed,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), 'F');

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringFileAdapterWritesAndReadsVectoredAtOffset) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-vectored-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFileVectoredReadWriteTask>(
        file.get(),
        &completed,
        &bytes_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_read.load(std::memory_order_acquire), 2);

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringFileAdapterUsesAsyncCurrentOffsetReadWrite) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-current-offset-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<int> packed_read{0};
    std::atomic<int> pending_submits{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFileCurrentOffsetTask>(
        file.get(),
        &completed,
        &packed_read,
        &pending_submits));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(
        packed_read.load(std::memory_order_acquire),
        (static_cast<int>('A') << 16) | (static_cast<int>('B') << 8) | static_cast<int>('C'));
    EXPECT_GE(pending_submits.load(std::memory_order_acquire), 4);

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringRegisteredBufferReadsAndWritesAtOffset) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-fixed-buffer-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFixedBufferFileTask>(
        file.get(),
        &completed,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), 'B');

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringFixedFileWritesFsyncsAndReadsAtOffset) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-fixed-file-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFixedFileTask>(
        file.get(),
        &completed,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), 'F');

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringFixedFileTableUpdatesRegisteredSlot) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char first_path[] = "/tmp/asyncflow-uring-fixed-update-a-XXXXXX";
    const int first_fd = ::mkstemp(first_path);
    ASSERT_GE(first_fd, 0);
    af::UniqueFd first(first_fd);
    char second_path[] = "/tmp/asyncflow-uring-fixed-update-b-XXXXXX";
    const int second_fd = ::mkstemp(second_path);
    ASSERT_GE(second_fd, 0);
    af::UniqueFd second(second_fd);

    const char first_payload = '1';
    const char second_payload = '2';
    ASSERT_EQ(::write(first.get(), &first_payload, sizeof(first_payload)), 1);
    ASSERT_EQ(::write(second.get(), &second_payload, sizeof(second_payload)), 1);

    std::atomic<int> completed{0};
    std::atomic<int> packed_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFixedFileUpdateTask>(
        first.get(),
        second.get(),
        &completed,
        &packed_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(
        packed_read.load(std::memory_order_acquire),
        (static_cast<int>('1') << 8) | static_cast<int>('2'));

    first.reset();
    second.reset();
    static_cast<void>(::unlink(first_path));
    static_cast<void>(::unlink(second_path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringBatchedSubmitCompletesBurstWrites) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-uring-batch-XXXXXX";
    const int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    af::UniqueFd file(fd);

    constexpr int write_count = 16;
    std::atomic<int> completed{0};
    for (int i = 0; i < write_count; ++i) {
        ASSERT_TRUE(UringIoRuntime::start_task<UringBatchedFileWriteTask>(
            file.get(),
            static_cast<std::uint64_t>(i),
            static_cast<char>('a' + i),
            &completed));
    }

    ASSERT_TRUE(wait_until_at_least(completed, write_count));

    char observed[write_count]{};
    ASSERT_EQ(::pread(file.get(), observed, sizeof(observed), 0), write_count);
    for (int i = 0; i < write_count; ++i) {
        EXPECT_EQ(observed[i], static_cast<char>('a' + i));
    }

    file.reset();
    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringThreadOpensFileWithOpenAt) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-openat-XXXXXX";
    int seed = ::mkstemp(path);
    ASSERT_GE(seed, 0);
    close_fd(seed);
    static_cast<void>(::unlink(path));

    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringOpenAtFileTask>(
        path,
        &completed,
        &byte_read));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), 'O');

    static_cast<void>(::unlink(path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(UringIoRuntimeFixture, IoUringFileLifecycleRunsOnIoThread) {
#if defined(__linux__)
    if (!UringIoRuntime::io_uring_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "io_uring backend unavailable";
    }

    char path[] = "/tmp/asyncflow-lifecycle-XXXXXX";
    int seed = ::mkstemp(path);
    ASSERT_GE(seed, 0);
    close_fd(seed);
    static_cast<void>(::unlink(path));

    char renamed_path[sizeof(path) + 8]{};
    ASSERT_GT(std::snprintf(renamed_path, sizeof(renamed_path), "%s.renamed", path), 0);
    static_cast<void>(::unlink(renamed_path));

    std::atomic<int> completed{0};
    std::atomic<int> close_released{0};
    std::atomic<std::uint64_t> observed_size{0};
    ASSERT_TRUE(UringIoRuntime::start_task<UringFileLifecycleTask>(
        path,
        renamed_path,
        &completed,
        &close_released,
        &observed_size));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    EXPECT_EQ(close_released.load(std::memory_order_acquire), 1);
    EXPECT_EQ(observed_size.load(std::memory_order_acquire), std::uint64_t{1});
    EXPECT_EQ(::access(path, F_OK), -1);
    EXPECT_EQ(errno, ENOENT);
    errno = 0;
    EXPECT_EQ(::access(renamed_path, F_OK), -1);
    EXPECT_EQ(errno, ENOENT);

    static_cast<void>(::unlink(path));
    static_cast<void>(::unlink(renamed_path));
#else
    GTEST_SKIP() << "io_uring backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadRejectsDuplicateFdWait) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<SocketReadableTask>(fds[0], &armed, &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    std::atomic<int> rejected{0};
    std::atomic<int> error{0};
    ASSERT_TRUE(IoRuntime::start_task<DuplicateWaitRejectedTask>(fds[0], &rejected, &error));
    ASSERT_TRUE(wait_until_at_least(rejected, 1));
    EXPECT_EQ(error.load(std::memory_order_acquire), EALREADY);

    const char value = 'd';
    ASSERT_EQ(::write(fds[1], &value, sizeof(value)), 1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadResumesTaskWhenFdBecomesWritable) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);
    ASSERT_TRUE(fill_until_blocked(fds[0]));

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    ASSERT_TRUE(IoRuntime::start_task<SocketWritableTask>(fds[0], &armed, &completed));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    drain_available(fds[1]);
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadResumesUdpRecvFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int receiver = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(receiver, 0);
    int sender = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(sender, 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(::getsockname(receiver, reinterpret_cast<sockaddr*>(&address), &address_size), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    ASSERT_TRUE(IoRuntime::start_task<UdpRecvTask>(receiver, &armed, &completed, &byte_read));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char value = 'u';
    ASSERT_EQ(::sendto(
                  sender,
                  &value,
                  sizeof(value),
                  0,
                  reinterpret_cast<sockaddr*>(&address),
                  address_size),
              1);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), value);

    close_fd(sender);
    close_fd(receiver);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadReceivesVectoredUdpDatagramFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int receiver = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(receiver, 0);
    int sender = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(sender, 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(::getsockname(receiver, reinterpret_cast<sockaddr*>(&address), &address_size), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> payload_seen{0};
    ASSERT_TRUE(IoRuntime::start_task<UdpVectoredRecvTask>(
        receiver,
        &armed,
        &completed,
        &payload_seen));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    const char payload[2]{'u', 'v'};
    ASSERT_EQ(::sendto(
                  sender,
                  payload,
                  sizeof(payload),
                  0,
                  reinterpret_cast<sockaddr*>(&address),
                  address_size),
              2);
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(payload_seen.load(std::memory_order_acquire), ('u' << 8) | 'v');

    close_fd(sender);
    close_fd(receiver);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadAcceptsUdpZeroLengthDatagram) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int receiver = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(receiver, 0);
    int sender = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(sender, 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(::getsockname(receiver, reinterpret_cast<sockaddr*>(&address), &address_size), 0);

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<char> byte_read{'z'};

    const char value = 0;
    ASSERT_EQ(::sendto(
                  sender,
                  &value,
                  0,
                  0,
                  reinterpret_cast<sockaddr*>(&address),
                  address_size),
              0);

    ASSERT_TRUE(IoRuntime::start_task<UdpRecvTask>(receiver, &armed, &completed, &byte_read, 0U));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(byte_read.load(std::memory_order_acquire), 'z');

    close_fd(sender);
    close_fd(receiver);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadSendsUdpDatagramFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int receiver = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(receiver, 0);
    int sender = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(sender, 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(::getsockname(receiver, reinterpret_cast<sockaddr*>(&address), &address_size), 0);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    const char value = 's';
    ASSERT_TRUE(IoRuntime::start_task<UdpSendToTask>(
        sender,
        address,
        address_size,
        value,
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 1);

    char received = 0;
    sockaddr_storage peer{};
    socklen_t peer_size = sizeof(peer);
    ASSERT_EQ(::recvfrom(
                  receiver,
                  &received,
                  sizeof(received),
                  0,
                  reinterpret_cast<sockaddr*>(&peer),
                  &peer_size),
              1);
    EXPECT_EQ(received, value);

    close_fd(sender);
    close_fd(receiver);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadSendsVectoredUdpDatagramFromHelper) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int receiver = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(receiver, 0);
    int sender = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT_GE(sender, 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ASSERT_EQ(::bind(receiver, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

    socklen_t address_size = sizeof(address);
    ASSERT_EQ(::getsockname(receiver, reinterpret_cast<sockaddr*>(&address), &address_size), 0);

    std::atomic<int> completed{0};
    std::atomic<int> bytes_sent{0};
    ASSERT_TRUE(IoRuntime::start_task<UdpVectoredSendToTask>(
        sender,
        address,
        address_size,
        'd',
        'g',
        &completed,
        &bytes_sent));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(bytes_sent.load(std::memory_order_acquire), 2);

    char received[2]{};
    sockaddr_storage peer{};
    socklen_t peer_size = sizeof(peer);
    ASSERT_EQ(::recvfrom(
                  receiver,
                  received,
                  sizeof(received),
                  0,
                  reinterpret_cast<sockaddr*>(&peer),
                  &peer_size),
              2);
    EXPECT_EQ(received[0], 'd');
    EXPECT_EQ(received[1], 'g');

    close_fd(sender);
    close_fd(receiver);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST_F(IoRuntimeFixture, EpollIoThreadReportsPeerHangupAsClosedRead) {
#if defined(__linux__)
    if (!IoRuntime::io_backend_available(IoTestThread::IO_0)) {
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds), 0);

    std::atomic<int> armed{0};
    std::atomic<int> closed{0};
    ASSERT_TRUE(IoRuntime::start_task<SocketHangupTask>(fds[0], &armed, &closed));
    ASSERT_TRUE(wait_until_at_least(armed, 1));

    ::close(fds[1]);
    fds[1] = -1;
    ASSERT_TRUE(wait_until_at_least(closed, 1));

    close_pair(fds);
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}

TEST(RuntimeIo, StopImmediatelyDropsPendingIoWaitsAndCanRestart) {
#if defined(__linux__)
    FastIoRuntime::init();
    if (!FastIoRuntime::io_backend_available(IoTestThread::IO_0)) {
        FastIoRuntime::shutdown();
        GTEST_SKIP() << "epoll backend unavailable";
    }

    int fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        FastIoRuntime::shutdown();
        FAIL() << "socketpair failed";
    }

    std::atomic<int> armed{0};
    if (!FastIoRuntime::start_task<PendingSocketWaitTask>(fds[0], &armed)) {
        close_pair(fds);
        FastIoRuntime::shutdown();
        FAIL() << "failed to start pending IO task";
    }
    if (!wait_until_at_least(armed, 1)) {
        close_pair(fds);
        FastIoRuntime::shutdown();
        FAIL() << "pending IO task was not armed";
    }

    FastIoRuntime::shutdown();
    close_pair(fds);

    FastIoRuntime::init();
    std::atomic<int> completed{0};
    const bool started = FastIoRuntime::start_task<FastIoDoneTask>(&completed);
    EXPECT_TRUE(started);
    if (started) {
        EXPECT_TRUE(wait_until_at_least(completed, 1));
    }
    FastIoRuntime::shutdown();
#else
    GTEST_SKIP() << "epoll backend is Linux-only";
#endif
}
