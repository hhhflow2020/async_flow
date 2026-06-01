#pragma once

#include <atomic>
#include <cerrno>
#include <cstdint>

#include "io_uring_recv_multishot_runtime.hpp"

#if defined(__linux__)

namespace io_uring_recv_multishot_example {

class RecvMultishotTask final : public RecvTaskBase {
public:
    explicit RecvMultishotTask(RecvTaskBase::FactoryToken token) : RecvTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *armed, int *packed_read, std::atomic<int> *error) {
        stream_.reset(RecvThreads::IO_0, fd);
        armed_ = armed;
        packed_read_ = packed_read;
        error_ = error;
        return schedule(RecvThreads::IO_0);
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

    af::TaskResult complete(int error) {
        if (registered_) {
            int unregister_error = 0;
            if (recv_async::io_unregister_provided_buffer_ring(RecvThreads::IO_0, buffer_group,
                                                               &unregister_error)) {
                registered_ = false;
            }
        }
        error_->store(error, std::memory_order_release);
        return done();
    }

    af::TaskResult register_ring() {
        int init_error = 0;
        if (!ring_.init(buffer_count, init_error)) {
            return complete(init_error == 0 ? EIO : init_error);
        }

        af::IoProvidedBuffer provided[buffer_count]{};
        for (std::uint16_t i = 0; i < buffer_count; ++i) {
            provided[i] = af::IoProvidedBuffer{&buffers_[i], sizeof(buffers_[i]), i};
        }
        int add_error = 0;
        if (!ring_.add(provided, buffer_count, add_error)) {
            return complete(add_error == 0 ? EIO : add_error);
        }

        int register_error = 0;
        if (!recv_async::io_register_provided_buffer_ring(
                RecvThreads::IO_0, ring_.ring(), ring_.entries(), buffer_group, &register_error)) {
            return complete(register_error == 0 ? EIO : register_error);
        }
        registered_ = true;
        state_ = State::Recv;
        return again();
    }

    af::TaskResult unregister_ring() {
        if (registered_) {
            int unregister_error = 0;
            if (!recv_async::io_unregister_provided_buffer_ring(RecvThreads::IO_0, buffer_group,
                                                                &unregister_error)) {
                return complete(unregister_error == 0 ? EIO : unregister_error);
            }
            registered_ = false;
        }
        return complete(finish_error_);
    }

    af::TaskResult recv_one() {
        std::uint16_t buffer_id = 0;
        const af::IoStatus status = stream_.recv_multishot(*this, buffer_group, &buffer_id, recv_);
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

        const int shifted = received_ == 0 ? 8 : 0;
        *packed_read_ |= static_cast<int>(static_cast<unsigned char>(buffers_[buffer_id]))
                         << shifted;
        ++received_;

        const af::IoProvidedBuffer buffer{&buffers_[buffer_id], sizeof(buffers_[buffer_id]),
                                          buffer_id};
        int add_error = 0;
        if (!ring_.add(&buffer, 1, add_error)) {
            return stop_recv(add_error == 0 ? EIO : add_error);
        }

        if (received_ < target_reads) {
            return pending();
        }
        if (!recv_.waiting) {
            state_ = State::Unregister;
            return again();
        }
        finish_error_ = 0;
        if (!recv_async::cancel_io(RecvThreads::IO_0, recv_)) {
            return complete(recv_.wait.error == 0 ? EIO : recv_.wait.error);
        }
        state_ = State::Cancel;
        return pending();
    }

    af::TaskResult finish_cancel() {
        std::uint16_t ignored = 0;
        const af::IoStatus status = stream_.recv_multishot(*this, buffer_group, &ignored, recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.failed() || status.error != ECANCELED) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult stop_recv(int error) {
        finish_error_ = error == 0 ? EIO : error;
        if (!recv_.waiting) {
            state_ = State::Unregister;
            return again();
        }
        if (!recv_async::cancel_io(RecvThreads::IO_0, recv_)) {
            return complete(recv_.wait.error == 0 ? finish_error_ : recv_.wait.error);
        }
        state_ = State::Cancel;
        return pending();
    }

    static constexpr std::uint16_t buffer_group = 9;
    static constexpr unsigned buffer_count = 2;
    static constexpr int target_reads = 2;

    State state_{State::Register};
    af::TcpStream<RecvThread> stream_{};
    af::IoProvidedBufferRing ring_{};
    char buffers_[buffer_count]{};
    int received_{0};
    int finish_error_{0};
    bool armed_once_{false};
    bool registered_{false};
    af::IoOpState recv_{};
    std::atomic<int> *armed_{nullptr};
    int *packed_read_{nullptr};
    std::atomic<int> *error_{nullptr};
};

} // namespace io_uring_recv_multishot_example

#endif
