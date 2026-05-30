#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {

enum class LifecycleThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct LifecycleRuntimeTraits {
    using Thread = LifecycleThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(LifecycleThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(LifecycleThread thread) noexcept {
        return thread == LifecycleThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using lifecycle_async = af::AsyncRuntime<LifecycleRuntimeTraits>;
using LifecycleTaskBase = lifecycle_async::Task;

bool wait_until(std::atomic<int>& value, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

#if defined(__linux__)
class FileLifecycleTask final : public LifecycleTaskBase {
public:
    explicit FileLifecycleTask(LifecycleTaskBase::FactoryToken token)
        : LifecycleTaskBase(token) {}

    bool do_it(
        const char* path,
        const char* renamed_path,
        std::atomic<int>* completed,
        std::atomic<std::uint64_t>* observed_size) {
        if (!copy_path(path_.data(), path_.size(), path) ||
            !copy_path(renamed_path_.data(), renamed_path_.size(), renamed_path)) {
            return false;
        }
        completed_ = completed;
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

    static bool copy_path(char* output, std::size_t output_size, const char* input) {
        const int written = std::snprintf(output, output_size, "%s", input);
        return written >= 0 && static_cast<std::size_t>(written) < output_size;
    }

    af::TaskResult run() override {
        switch (state_) {
        case State::Open:
            return open_file();

        case State::Fallocate:
            return fallocate_file();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::Statx:
            return stat_file();

        case State::Rename:
            return rename_file();

        case State::Unlink:
            return unlink_file();

        case State::Close:
            return close_file();
        }
        return failed();
    }

    af::TaskResult open_file() {
        int fd = -1;
        const af::IoStatus status = af::io_openat(
            *this,
            LifecycleThread::IO_0,
            AT_FDCWD,
            path_.data(),
            O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC,
            0600U,
            &fd,
            open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return failed();
        }
        owned_.reset(fd);
        file_.reset(LifecycleThread::IO_0, owned_.get());
        state_ = State::Fallocate;
        return again();
    }

    af::TaskResult fallocate_file() {
        const af::IoStatus status = af::io_fallocate(
            *this,
            LifecycleThread::IO_0,
            owned_.get(),
            FALLOC_FL_KEEP_SIZE,
            0,
            4096,
            fallocate_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), 0, write_);
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
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_at(*this, &read_, sizeof(read_), 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_) || read_ != value_) {
            return failed();
        }
        state_ = State::Statx;
        return again();
    }

    af::TaskResult stat_file() {
        const af::IoStatus status = af::io_statx(
            *this,
            LifecycleThread::IO_0,
            AT_FDCWD,
            path_.data(),
            0,
            STATX_SIZE,
            &stat_,
            stat_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || stat_.stx_size != sizeof(value_)) {
            return failed();
        }
        observed_size_->store(stat_.stx_size, std::memory_order_release);
        state_ = State::Rename;
        return again();
    }

    af::TaskResult rename_file() {
        const af::IoStatus status = af::io_renameat(
            *this,
            LifecycleThread::IO_0,
            AT_FDCWD,
            path_.data(),
            AT_FDCWD,
            renamed_path_.data(),
            0,
            rename_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Unlink;
        return again();
    }

    af::TaskResult unlink_file() {
        const af::IoStatus status = af::io_unlinkat(
            *this,
            LifecycleThread::IO_0,
            AT_FDCWD,
            renamed_path_.data(),
            0,
            unlink_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Close;
        return again();
    }

    af::TaskResult close_file() {
        const af::IoStatus status = af::io_close(*this, LifecycleThread::IO_0, owned_, close_);
        if (status.pending()) {
            return owned_.get() == -1 ? pending() : failed();
        }
        if (!status.ready() || owned_.get() != -1) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

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
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint64_t>* observed_size_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    lifecycle_async::init();
    if (!lifecycle_async::io_uring_backend_available(LifecycleThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        lifecycle_async::shutdown();
        return 0;
    }

    std::array<char, 160> path{};
    std::array<char, 176> renamed_path{};
    const int path_written = std::snprintf(
        path.data(),
        path.size(),
        "/tmp/asyncflow-lifecycle-%ld",
        static_cast<long>(::getpid()));
    const int rename_written =
        std::snprintf(renamed_path.data(), renamed_path.size(), "%s.renamed", path.data());
    if (path_written < 0 ||
        rename_written < 0 ||
        static_cast<std::size_t>(path_written) >= path.size() ||
        static_cast<std::size_t>(rename_written) >= renamed_path.size()) {
        std::cerr << "path formatting failed\n";
        lifecycle_async::shutdown();
        return 1;
    }
    static_cast<void>(::unlink(path.data()));
    static_cast<void>(::unlink(renamed_path.data()));

    std::atomic<int> completed{0};
    std::atomic<std::uint64_t> observed_size{0};
    const bool started = lifecycle_async::start_task<FileLifecycleTask>(
        path.data(),
        renamed_path.data(),
        &completed,
        &observed_size);
    if (!started || !wait_until(completed, 1)) {
        std::cerr << "io_uring lifecycle task timed out\n";
        static_cast<void>(::unlink(path.data()));
        static_cast<void>(::unlink(renamed_path.data()));
        lifecycle_async::shutdown();
        return 1;
    }

    std::cout << "io_uring lifecycle size="
              << observed_size.load(std::memory_order_acquire) << '\n';
    lifecycle_async::shutdown();
    return 0;
#else
    std::cout << "io_uring lifecycle example is Linux-only\n";
    return 0;
#endif
}
