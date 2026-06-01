#pragma once

#include <cerrno>
#include <cstdint>

#include "io_uring_fixed_file_runtime.hpp"

#if defined(__linux__)

namespace io_uring_fixed_file_example {

class FixedFileRoundTripTask final : public FixedFileTask {
public:
    explicit FixedFileRoundTripTask(FixedFileTask::FactoryToken token) : FixedFileTask(token) {}

    bool do_it(
        int fd,
        int updated_fd,
        int* error,
        char* byte_read,
        int* vectored_read,
        char* updated_byte_read) {
        fd_ = fd;
        updated_fd_ = updated_fd;
        error_ = error;
        byte_read_ = byte_read;
        vectored_read_ = vectored_read;
        updated_byte_read_ = updated_byte_read;
        file_.reset(FixedFileThread::IO_0, 0);
        return schedule(FixedFileThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Write,
        WriteVectored,
        Fsync,
        Read,
        ReadVectored,
        Update,
        ReadUpdated,
        Unregister,
    };

#define AF_EXAMPLE_IO_URING_FIXED_FILE_TASK_FRAGMENT_INCLUDE 1
#include "io_uring_fixed_file_task_flow.hpp"
#include "io_uring_fixed_file_task_registration.hpp"
#include "io_uring_fixed_file_task_io.hpp"
#undef AF_EXAMPLE_IO_URING_FIXED_FILE_TASK_FRAGMENT_INCLUDE

    State state_{State::Register};
    int fd_{-1};
    int updated_fd_{-1};
    af::IoFixedFile<FixedFileThread> file_{};
    alignas(64) char buffer_[64]{};
    char value_{'F'};
    char vector_write_[2]{'I', 'O'};
    char vector_read_[2]{};
    char updated_read_{0};
    iovec write_iov_[2]{};
    iovec read_iov_[2]{};
    af::IoOpState write_{};
    af::IoOpState writev_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    af::IoOpState readv_{};
    af::IoOpState updated_read_state_{};
    int* error_{nullptr};
    char* byte_read_{nullptr};
    int* vectored_read_{nullptr};
    char* updated_byte_read_{nullptr};
};

} // namespace io_uring_fixed_file_example

#endif
