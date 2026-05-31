#pragma once

#include <cerrno>
#include <cstdint>

#include "io_uring_filesystem_ops_runtime.hpp"

#if defined(__linux__)
#include <fcntl.h>
#include <linux/openat2.h>
#include <sys/stat.h>
#include <unistd.h>

namespace io_uring_filesystem_ops_example {

class FilesystemOpsTask final : public FsTaskBase {
public:
    explicit FilesystemOpsTask(FsTaskBase::FactoryToken token) : FsTaskBase(token) {}

    bool do_it(
        const char* dir_path,
        const char* file_path,
        const char* hardlink_path,
        const char* symlink_path,
        FsResult* result) {
        dir_path_ = dir_path;
        file_path_ = file_path;
        hardlink_path_ = hardlink_path;
        symlink_path_ = symlink_path;
        result_ = result;
        how_.flags = O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC;
        how_.mode = 0600U;
        return schedule(FsThread::IO_0);
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

#define AF_EXAMPLE_IO_URING_FILESYSTEM_OPS_TASK_FRAGMENT_INCLUDE 1
#include "io_uring_filesystem_ops_task_flow.hpp"
#include "io_uring_filesystem_ops_task_data.hpp"
#include "io_uring_filesystem_ops_task_namespace.hpp"
#undef AF_EXAMPLE_IO_URING_FILESYSTEM_OPS_TASK_FRAGMENT_INCLUDE

    State state_{State::Mkdir};
    const char* dir_path_{nullptr};
    const char* file_path_{nullptr};
    const char* hardlink_path_{nullptr};
    const char* symlink_path_{nullptr};
    FsResult* result_{nullptr};
    struct open_how how_{};
    af::UniqueFd owned_{};
    af::IoFile<FsThread> file_{};
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
};

} // namespace io_uring_filesystem_ops_example

#endif
