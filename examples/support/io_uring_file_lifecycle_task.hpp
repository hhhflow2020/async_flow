#pragma once

#include <array>
#include <cstdio>
#include <cstdint>

#include "io_uring_file_lifecycle_runtime.hpp"

#if defined(__linux__)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace io_uring_file_lifecycle_example {

class FileLifecycleTask final : public LifecycleTaskBase {
public:
    explicit FileLifecycleTask(LifecycleTaskBase::FactoryToken token)
        : LifecycleTaskBase(token) {}

    bool do_it(
        const char* path,
        const char* renamed_path,
        std::uint64_t* observed_size) {
        if (!copy_path(path_.data(), path_.size(), path) ||
            !copy_path(renamed_path_.data(), renamed_path_.size(), renamed_path)) {
            return false;
        }
        observed_size_ = observed_size;
        return schedule(LifecycleThread::IO_0);
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

#define AF_EXAMPLE_IO_URING_FILE_LIFECYCLE_TASK_FRAGMENT_INCLUDE 1
#include "io_uring_file_lifecycle_task_flow.hpp"
#include "io_uring_file_lifecycle_task_file_ops.hpp"
#include "io_uring_file_lifecycle_task_namespace_ops.hpp"
#undef AF_EXAMPLE_IO_URING_FILE_LIFECYCLE_TASK_FRAGMENT_INCLUDE

    State state_{State::Open};
    std::array<char, 160> path_{};
    std::array<char, 176> renamed_path_{};
    af::UniqueFd owned_{};
    af::IoFile<LifecycleThread> file_{};
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
    std::uint64_t* observed_size_{nullptr};
};

} // namespace io_uring_file_lifecycle_example

#endif
