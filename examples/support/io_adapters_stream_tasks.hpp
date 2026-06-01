#pragma once

#include <cerrno>
#include <cstdint>

#include "io_adapters_results.hpp"
#include "../app_runtime.hpp"

#if defined(__linux__)

namespace io_adapters_example {

class StreamEchoTask final : public Task {
public:
    explicit StreamEchoTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, StreamEchoResult *result) {
        if (fd < 0 || result == nullptr) {
            return false;
        }
        stream_.reset(AppThread::IO_0, fd);
        result_ = result;
        return schedule(AppThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Read,
        Write,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Read:
            return read_request();
        case State::Write:
            return write_response();
        }
        return finish(EIO);
    }

    af::TaskResult read_request() {
        const af::IoStatus status = stream_.recv_some(*this, &request_, sizeof(request_), read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return finish(status.failed() ? status.error : EIO);
        }

        result_->request = request_;
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_response() {
        const af::IoStatus status = stream_.send_some(*this, &response_, sizeof(response_), write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return finish(status.failed() ? status.error : EIO);
        }

        result_->response = response_;
        result_->error = 0;
        result_->ok = true;
        return done();
    }

    af::TaskResult finish(int error) {
        result_->error = error == 0 ? EIO : error;
        result_->ok = false;
        return failed();
    }

    State state_{State::Read};
    af::TcpStream<AppThread> stream_{};
    char request_{0};
    char response_{'S'};
    af::IoOpState read_{};
    af::IoOpState write_{};
    StreamEchoResult *result_{nullptr};
};

class StreamPeerTask final : public Task {
public:
    explicit StreamPeerTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, StreamPeerResult *result) {
        if (fd < 0 || result == nullptr) {
            return false;
        }
        stream_.reset(AppThread::IO_0, fd);
        result_ = result;
        return schedule(AppThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Write,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Write:
            return write_request();
        case State::Read:
            return read_response();
        }
        return finish(EIO);
    }

    af::TaskResult write_request() {
        const af::IoStatus status = stream_.send_some(*this, &request_, sizeof(request_), write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return finish(status.failed() ? status.error : EIO);
        }

        state_ = State::Read;
        return again();
    }

    af::TaskResult read_response() {
        const af::IoStatus status =
            stream_.recv_some(*this, &result_->response, sizeof(result_->response), read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(result_->response)) {
            return finish(status.failed() ? status.error : EIO);
        }

        result_->ok = result_->response == expected_response;
        result_->error = result_->ok ? 0 : EIO;
        return result_->ok ? done() : failed();
    }

    af::TaskResult finish(int error) {
        result_->error = error == 0 ? EIO : error;
        result_->ok = false;
        return failed();
    }

    static constexpr char expected_response = 'S';

    State state_{State::Write};
    af::TcpStream<AppThread> stream_{};
    char request_{'T'};
    af::IoOpState write_{};
    af::IoOpState read_{};
    StreamPeerResult *result_{nullptr};
};

} // namespace io_adapters_example

#endif
