#include <cstdio>
#include <cstdint>
#include <iostream>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <fcntl.h>
#include <linux/openat2.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {

enum class FsThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct FsRuntimeTraits {
    using Thread = FsThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(FsThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(FsThread thread) noexcept {
        return thread == FsThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using fs_async = af::AsyncRuntime<FsRuntimeTraits>;
using FsTaskBase = fs_async::Task;

struct FsResult {
    int error{0};
    std::uint64_t observed_size{0};
};

#if defined(__linux__)
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

    af::TaskResult run() override {
        switch (state_) {
        case State::Mkdir:
            return mkdir_dir();
        case State::OpenAt2:
            return open_file();
        case State::Write:
            return write_payload();
        case State::Ftruncate:
            return truncate_file();
        case State::Fsync:
            return fsync_file();
        case State::Statx:
            return stat_file();
        case State::Close:
            return close_file();
        case State::Link:
            return link_file();
        case State::Symlink:
            return symlink_file();
        case State::UnlinkFile:
            return unlink_path(file_path_, State::UnlinkHardlink, 0);
        case State::UnlinkHardlink:
            return unlink_path(hardlink_path_, State::UnlinkSymlink, 0);
        case State::UnlinkSymlink:
            return unlink_path(symlink_path_, State::Rmdir, 0);
        case State::Rmdir:
            return unlink_path(dir_path_, State::Rmdir, AT_REMOVEDIR, true);
        }
        return finish(EIO);
    }

    af::TaskResult mkdir_dir() {
        const af::IoStatus status =
            af::io_mkdirat(*this, FsThread::IO_0, AT_FDCWD, dir_path_, 0700U, mkdir_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::OpenAt2;
        return again();
    }

    af::TaskResult open_file() {
        int fd = -1;
        const af::IoStatus status = af::io_openat2(
            *this,
            FsThread::IO_0,
            AT_FDCWD,
            file_path_,
            &how_,
            &fd,
            open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return finish(status.failed() ? status.error : EIO);
        }
        owned_.reset(fd);
        file_.reset(FsThread::IO_0, owned_.get());
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_payload() {
        const af::IoStatus status = file_.write_at(*this, payload_, sizeof(payload_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(payload_)) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::Ftruncate;
        return again();
    }

    af::TaskResult truncate_file() {
        const af::IoStatus status =
            af::io_ftruncate(*this, FsThread::IO_0, owned_.get(), 1, truncate_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_file() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::Statx;
        return again();
    }

    af::TaskResult stat_file() {
        const af::IoStatus status = af::io_statx(
            *this,
            FsThread::IO_0,
            AT_FDCWD,
            file_path_,
            0,
            STATX_SIZE,
            &stat_,
            stat_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || stat_.stx_size != 1U) {
            return finish(status.failed() ? status.error : EIO);
        }
        result_->observed_size = stat_.stx_size;
        state_ = State::Close;
        return again();
    }

    af::TaskResult close_file() {
        const af::IoStatus status = af::io_close(*this, FsThread::IO_0, owned_, close_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || owned_.get() != -1) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::Link;
        return again();
    }

    af::TaskResult link_file() {
        const af::IoStatus status = af::io_linkat(
            *this,
            FsThread::IO_0,
            AT_FDCWD,
            file_path_,
            AT_FDCWD,
            hardlink_path_,
            0,
            link_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::Symlink;
        return again();
    }

    af::TaskResult symlink_file() {
        const af::IoStatus status = af::io_symlinkat(
            *this,
            FsThread::IO_0,
            file_path_,
            AT_FDCWD,
            symlink_path_,
            symlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        state_ = State::UnlinkFile;
        return again();
    }

    af::TaskResult unlink_path(
        const char* path,
        State next_state,
        int flags,
        bool final_state = false) {
        const af::IoStatus status =
            af::io_unlinkat(*this, FsThread::IO_0, AT_FDCWD, path, flags, unlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        if (final_state) {
            return finish(0);
        }
        state_ = next_state;
        unlink_.reset();
        return again();
    }

    af::TaskResult finish(int error) {
        result_->error = error;
        return done();
    }

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
#endif

} // namespace

int main() {
#if !defined(__linux__)
    std::cout << "io_uring filesystem ops example is Linux-only\n";
    return 0;
#else
    fs_async::init();
    if (!fs_async::io_uring_backend_available(FsThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        fs_async::shutdown();
        return 0;
    }

    char dir_path[] = "/tmp/asyncflow-fsops-example-XXXXXX";
    if (::mkdtemp(dir_path) == nullptr) {
        std::perror("mkdtemp");
        fs_async::shutdown();
        return 1;
    }
    static_cast<void>(::rmdir(dir_path));

    char file_path[sizeof(dir_path) + 8]{};
    char hardlink_path[sizeof(dir_path) + 12]{};
    char symlink_path[sizeof(dir_path) + 12]{};
    std::snprintf(file_path, sizeof(file_path), "%s/file", dir_path);
    std::snprintf(hardlink_path, sizeof(hardlink_path), "%s/hard", dir_path);
    std::snprintf(symlink_path, sizeof(symlink_path), "%s/sym", dir_path);

    FsResult result{};
    const bool scheduled = fs_async::start_task<FilesystemOpsTask>(
        dir_path,
        file_path,
        hardlink_path,
        symlink_path,
        &result);
    if (!scheduled) {
        std::cerr << "failed to schedule filesystem task\n";
        fs_async::shutdown();
        return 1;
    }

    fs_async::wait_for_idle();
    fs_async::shutdown();

    static_cast<void>(::unlink(file_path));
    static_cast<void>(::unlink(hardlink_path));
    static_cast<void>(::unlink(symlink_path));
    static_cast<void>(::rmdir(dir_path));

    if (result.error == EINVAL || result.error == EOPNOTSUPP || result.error == ENOSYS) {
        std::cout << "kernel does not support one of the requested io_uring fs opcodes\n";
        return 0;
    }
    if (result.error != 0) {
        std::cerr << "filesystem task failed: " << result.error << "\n";
        return 1;
    }

    std::cout << "filesystem lifecycle complete, statx size=" << result.observed_size << "\n";
    return result.observed_size == 1U ? 0 : 1;
#endif
}
