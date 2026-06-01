#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_file_fixed_file_rw_tasks_fragment.hpp is a runtime_io_file_tasks implementation fragment"
#endif

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

#define AF_RUNTIME_IO_FILE_FIXED_FILE_RW_TASK_FRAGMENT_INCLUDE 1
#include "runtime_io_file_fixed_file_rw_task_flow_fragment.hpp"
#include "runtime_io_file_fixed_file_rw_task_registration_fragment.hpp"
#include "runtime_io_file_fixed_file_rw_task_io_fragment.hpp"
#undef AF_RUNTIME_IO_FILE_FIXED_FILE_RW_TASK_FRAGMENT_INCLUDE

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
