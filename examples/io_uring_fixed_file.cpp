#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <iostream>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace {

enum class FixedFileThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct FixedFileRuntimeTraits {
    using Thread = FixedFileThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(FixedFileThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(FixedFileThread thread) noexcept {
        return thread == FixedFileThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using fixed_file_async = af::AsyncRuntime<FixedFileRuntimeTraits>;
using FixedFileTask = fixed_file_async::Task;

#if defined(__linux__)
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

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_file();

        case State::Write:
            return write_value();

        case State::WriteVectored:
            return write_vectored();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::ReadVectored:
            return read_vectored();

        case State::Update:
            return update_file();

        case State::ReadUpdated:
            return read_updated_value();

        case State::Unregister:
            return unregister_file();
        }
        return complete(EIO);
    }

    af::TaskResult register_file() {
        int error = 0;
        if (!fixed_file_async::io_register_files(FixedFileThread::IO_0, &fd_, 1, &error)) {
            return complete(error == 0 ? EIO : error);
        }
        iovec iov{buffer_, sizeof(buffer_)};
        if (!fixed_file_async::io_register_buffers(FixedFileThread::IO_0, &iov, 1, &error)) {
            return complete(error == 0 ? EIO : error);
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
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        buffer_[0] = 0;
        state_ = State::WriteVectored;
        return again();
    }

    af::TaskResult write_vectored() {
        write_iov_[0] = iovec{&vector_write_[0], 1};
        write_iov_[1] = iovec{&vector_write_[1], 1};
        const af::IoStatus status = file_.writev_at(
            *this,
            write_iov_,
            2,
            1,
            writev_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(vector_write_)) {
            return complete(status.failed() ? status.error : EIO);
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
            return complete(status.failed() ? status.error : EIO);
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
        if (!status.ready() || status.bytes != 1U) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::ReadVectored;
        return again();
    }

    af::TaskResult read_vectored() {
        read_iov_[0] = iovec{&vector_read_[0], 1};
        read_iov_[1] = iovec{&vector_read_[1], 1};
        const af::IoStatus status = file_.readv_at(
            *this,
            read_iov_,
            2,
            1,
            readv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() ||
            status.bytes != sizeof(vector_read_) ||
            vector_read_[0] != vector_write_[0] ||
            vector_read_[1] != vector_write_[1]) {
            return complete(status.failed() ? status.error : EIO);
        }
        *vectored_read_ = pack_vectored_read();
        state_ = State::Update;
        return again();
    }

    af::TaskResult update_file() {
        int error = 0;
        if (!fixed_file_async::io_update_registered_files(
                FixedFileThread::IO_0,
                0,
                &updated_fd_,
                1,
                &error)) {
            return complete(error == 0 ? EIO : error);
        }
        state_ = State::ReadUpdated;
        return again();
    }

    af::TaskResult read_updated_value() {
        const af::IoStatus status = file_.read_at(
            *this,
            &updated_read_,
            sizeof(updated_read_),
            0,
            updated_read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(updated_read_)) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_file() {
        int error = 0;
        if (!fixed_file_async::io_unregister_buffers(FixedFileThread::IO_0, &error)) {
            return complete(error == 0 ? EIO : error);
        }
        if (!fixed_file_async::io_unregister_files(FixedFileThread::IO_0, &error)) {
            return complete(error == 0 ? EIO : error);
        }
        *byte_read_ = buffer_[0];
        *updated_byte_read_ = updated_read_;
        return complete(0);
    }

    af::TaskResult complete(int error) {
        *error_ = error;
        return done();
    }

    [[nodiscard]] int pack_vectored_read() const noexcept {
        return (static_cast<int>(static_cast<unsigned char>(vector_read_[0])) << 8) |
               static_cast<int>(static_cast<unsigned char>(vector_read_[1]));
    }

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
#endif

} // namespace

int main() {
#if defined(__linux__)
    fixed_file_async::init();
    if (!fixed_file_async::io_uring_backend_available(FixedFileThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        fixed_file_async::shutdown();
        return 0;
    }

    char path[] = "/tmp/asyncflow-fixed-file-XXXXXX";
    const int fd = ::mkstemp(path);
    if (fd < 0) {
        std::cout << "mkstemp failed\n";
        fixed_file_async::shutdown();
        return 1;
    }
    af::UniqueFd file(fd);
    char updated_path[] = "/tmp/asyncflow-fixed-file-update-XXXXXX";
    const int updated_fd = ::mkstemp(updated_path);
    if (updated_fd < 0) {
        std::cout << "mkstemp update failed\n";
        file.reset();
        static_cast<void>(::unlink(path));
        fixed_file_async::shutdown();
        return 1;
    }
    af::UniqueFd updated_file(updated_fd);
    const char updated_payload = 'U';
    if (::write(updated_file.get(), &updated_payload, sizeof(updated_payload)) !=
        static_cast<ssize_t>(sizeof(updated_payload))) {
        std::cout << "write update file failed\n";
        updated_file.reset();
        file.reset();
        static_cast<void>(::unlink(updated_path));
        static_cast<void>(::unlink(path));
        fixed_file_async::shutdown();
        return 1;
    }

    int error = 0;
    char byte_read = 0;
    int vectored_read = 0;
    char updated_byte_read = 0;
    const bool started = fixed_file_async::start_task<FixedFileRoundTripTask>(
        file.get(),
        updated_file.get(),
        &error,
        &byte_read,
        &vectored_read,
        &updated_byte_read);
    AF_ASSERT(started);

    if (!started) {
        std::cout << "io_uring fixed file task start failed\n";
        updated_file.reset();
        file.reset();
        static_cast<void>(::unlink(updated_path));
        static_cast<void>(::unlink(path));
        fixed_file_async::shutdown();
        return 1;
    }

    fixed_file_async::shutdown();

    if (error != 0) {
        std::cout << "io_uring fixed file failed error=" << error << '\n';
        updated_file.reset();
        file.reset();
        static_cast<void>(::unlink(updated_path));
        static_cast<void>(::unlink(path));
        return 1;
    }

    const int vectored = vectored_read;
    std::cout << "io_uring fixed file byte="
              << byte_read
              << " vectored="
              << static_cast<char>((vectored >> 8) & 0xff)
              << static_cast<char>(vectored & 0xff)
              << " updated=" << updated_byte_read << '\n';
    updated_file.reset();
    file.reset();
    static_cast<void>(::unlink(updated_path));
    static_cast<void>(::unlink(path));
    return 0;
#else
    std::cout << "io_uring fixed file example is Linux-only\n";
    return 0;
#endif
}
