#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_file_lifecycle_tasks_fragment.hpp is a runtime_io_file_tasks implementation fragment"
#endif

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

#define AF_RUNTIME_IO_FILE_LIFECYCLE_TASK_FRAGMENT_INCLUDE 1
#include "runtime_io_file_lifecycle_task_flow_fragment.hpp"
#include "runtime_io_file_lifecycle_task_file_ops_fragment.hpp"
#include "runtime_io_file_lifecycle_task_namespace_ops_fragment.hpp"
#undef AF_RUNTIME_IO_FILE_LIFECYCLE_TASK_FRAGMENT_INCLUDE

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
