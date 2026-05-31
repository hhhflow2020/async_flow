#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_file_filesystem_ops_tasks_fragment.hpp is a runtime_io_file_tasks implementation fragment"
#endif

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

#define AF_RUNTIME_IO_FILESYSTEM_OPS_TASK_FRAGMENT_INCLUDE 1
#include "runtime_io_file_filesystem_ops_flow_tasks_fragment.hpp"
#include "runtime_io_file_filesystem_ops_data_tasks_fragment.hpp"
#include "runtime_io_file_filesystem_ops_namespace_tasks_fragment.hpp"
#undef AF_RUNTIME_IO_FILESYSTEM_OPS_TASK_FRAGMENT_INCLUDE

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
