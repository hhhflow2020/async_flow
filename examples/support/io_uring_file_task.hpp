#pragma once

#include <cstdint>

#include "io_uring_file_runtime.hpp"

#if defined(__linux__)
#include <unistd.h>

namespace io_uring_file_example {

class FileRoundTripTask final : public FileTaskBase {
public:
    explicit FileRoundTripTask(FileTaskBase::FactoryToken token) : FileTaskBase(token) {}

    bool do_it(int fd, char *byte_read) {
        file_.reset(FileThreads::IO_0, fd);
        byte_read_ = byte_read;
        return schedule(FileThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Write,
        Fsync,
        SeekStart,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::SeekStart:
            return seek_start();

        case State::Read:
            return read_value();
        }
        return failed();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_some(*this, &value_, sizeof(value_), write_);
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
        state_ = State::SeekStart;
        return again();
    }

    af::TaskResult seek_start() {
        if (::lseek(file_.fd(), 0, SEEK_SET) < 0) {
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_some(*this, &read_, sizeof(read_), read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_)) {
            return failed();
        }
        *byte_read_ = read_;
        return done();
    }

    State state_{State::Write};
    af::IoFile<FileThread> file_{};
    char value_{'I'};
    char read_{0};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    char *byte_read_{nullptr};
};

} // namespace io_uring_file_example

#else

namespace io_uring_file_example {

class FileRoundTripTask final : public FileTaskBase {
public:
    explicit FileRoundTripTask(FileTaskBase::FactoryToken token) : FileTaskBase(token) {}

    bool do_it(int fd, char *byte_read) {
        static_cast<void>(fd);
        static_cast<void>(byte_read);
        return false;
    }

private:
    af::TaskResult run() override {
        return failed();
    }
};

} // namespace io_uring_file_example

#endif
