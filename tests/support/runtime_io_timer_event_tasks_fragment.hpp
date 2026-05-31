#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_io_timer_event_tasks_fragment.hpp is a runtime_io_test_support implementation fragment"
#endif

template <typename TaskBaseT>
class BasicTimerFdTask final : public TaskBaseT {
public:
    explicit BasicTimerFdTask(typename TaskBaseT::FactoryToken token) : TaskBaseT(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<std::uint64_t>* expirations) {
        timer_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        expirations_ = expirations;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        std::uint64_t count = 0;
        const af::IoStatus status = timer_.wait(*this, &count, wait_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return this->pending();
        }
        if (!status.ready() || status.bytes != sizeof(count) || count == 0U) {
            return this->failed();
        }
        expirations_->store(count, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::IoTimer<IoTestThread> timer_{};
    af::IoOpState wait_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint64_t>* expirations_{nullptr};
};

using TimerFdTask = BasicTimerFdTask<IoTaskBase>;
using UringTimerFdTask = BasicTimerFdTask<UringIoTaskBase>;

template <typename TaskBaseT>
class BasicUringTimeoutTask final : public TaskBaseT {
public:
    explicit BasicUringTimeoutTask(typename TaskBaseT::FactoryToken token) : TaskBaseT(token) {}

    bool do_it(
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* error) {
        armed_ = armed;
        completed_ = completed;
        error_ = error;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = af::io_wait_timeout(
            *this,
            IoTestThread::IO_0,
            std::chrono::milliseconds(1),
            wait_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return this->pending();
        }
        error_->store(status.failed() ? status.error : 0, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::IoOpState wait_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

using UringTimeoutTask = BasicUringTimeoutTask<UringIoTaskBase>;

template <typename TaskBaseT>
class BasicEventFdTask final : public TaskBaseT {
public:
    explicit BasicEventFdTask(typename TaskBaseT::FactoryToken token) : TaskBaseT(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<std::uint64_t>* value) {
        event_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        value_ = value;
        return this->schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        std::uint64_t counter = 0;
        const af::IoStatus status = event_.wait(*this, &counter, wait_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return this->pending();
        }
        if (!status.ready() || status.bytes != sizeof(counter) || counter == 0U) {
            return this->failed();
        }
        value_->store(counter, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return this->done();
    }

    af::IoEvent<IoTestThread> event_{};
    af::IoOpState wait_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint64_t>* value_{nullptr};
};

using EventFdTask = BasicEventFdTask<IoTaskBase>;
using UringEventFdTask = BasicEventFdTask<UringIoTaskBase>;

class TimerBoundaryTask final : public IoTaskBase {
public:
    explicit TimerBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoTimer<IoTestThread> timer(IoTestThread::IO_0, -1);
        af::IoOpState null_state{};
        af::IoOpState bad_fd_state{};
        std::uint64_t expirations = 0;
        const af::IoStatus null_status = timer.wait(*this, nullptr, null_state);
        const af::IoStatus bad_fd_status = timer.wait(*this, &expirations, bad_fd_state);
        if (!null_status.failed() || null_status.error != EINVAL ||
            !bad_fd_status.failed() || bad_fd_status.error != EBADF) {
            return failed();
        }
        error_->store(bad_fd_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class TimeoutBoundaryTask final : public IoTaskBase {
public:
    explicit TimeoutBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState invalid_delay{};
        af::IoOpState no_uring{};
        const af::IoStatus invalid_status = af::io_wait_timeout(
            *this,
            IoTestThread::IO_0,
            std::chrono::nanoseconds{0},
            invalid_delay);
        const af::IoStatus no_uring_status = af::io_wait_timeout(
            *this,
            IoTestThread::IO_0,
            std::chrono::milliseconds(1),
            no_uring);
        if (!invalid_status.failed() || invalid_status.error != EINVAL ||
            !no_uring_status.failed() || no_uring_status.error != ENOSYS) {
            return failed();
        }
        error_->store(no_uring_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class EventBoundaryTask final : public IoTaskBase {
public:
    explicit EventBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoEvent<IoTestThread> event(IoTestThread::IO_0, -1);
        af::IoOpState null_state{};
        af::IoOpState bad_fd_state{};
        std::uint64_t value = 0;
        const af::IoStatus null_status = event.wait(*this, nullptr, null_state);
        const af::IoStatus bad_fd_status = event.wait(*this, &value, bad_fd_state);
        if (!null_status.failed() || null_status.error != EINVAL ||
            !bad_fd_status.failed() || bad_fd_status.error != EBADF) {
            return failed();
        }
        error_->store(bad_fd_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

class OpenAtBoundaryTask final : public IoTaskBase {
public:
    explicit OpenAtBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState null_path{};
        af::IoOpState null_output{};
        af::IoOpState no_uring{};
        af::IoOpState close_no_uring{};
        af::IoOpState stat_no_uring{};
        af::IoOpState fallocate_no_uring{};
        af::IoOpState rename_no_uring{};
        af::IoOpState unlink_no_uring{};
        af::IoOpState openat2_no_uring{};
        af::IoOpState mkdir_no_uring{};
        af::IoOpState symlink_no_uring{};
        af::IoOpState link_no_uring{};
        af::IoOpState ftruncate_no_uring{};
        af::IoOpState stat_null_path{};
        af::IoOpState stat_null_output{};
        af::IoOpState fallocate_bad_fd{};
        af::IoOpState rename_null_old{};
        af::IoOpState rename_null_new{};
        af::IoOpState unlink_null_path{};
        af::IoOpState openat2_null_path{};
        af::IoOpState openat2_null_how{};
        af::IoOpState openat2_null_output{};
        af::IoOpState mkdir_null_path{};
        af::IoOpState symlink_null_target{};
        af::IoOpState symlink_null_path{};
        af::IoOpState link_null_old{};
        af::IoOpState link_null_new{};
        af::IoOpState ftruncate_bad_fd{};
        struct statx stat{};
        struct open_how how{};
        how.flags = O_RDONLY | O_CLOEXEC;
        int opened = -1;
        const af::IoStatus null_path_status = af::io_openat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            O_RDONLY | O_CLOEXEC,
            0,
            &opened,
            null_path);
        const af::IoStatus null_output_status = af::io_openat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            nullptr,
            null_output);
        const af::IoStatus no_uring_status = af::io_openat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            &opened,
            no_uring);
        af::UniqueFd event = af::make_eventfd();
        if (!event) {
            return failed();
        }
        const af::IoStatus close_no_uring_status =
            af::io_close(*this, IoTestThread::IO_0, event, close_no_uring);
        const af::IoStatus stat_no_uring_status = af::io_statx(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary",
            0,
            STATX_SIZE,
            &stat,
            stat_no_uring);
        const af::IoStatus fallocate_no_uring_status = af::io_fallocate(
            *this,
            IoTestThread::IO_0,
            event.get(),
            FALLOC_FL_KEEP_SIZE,
            0,
            4096,
            fallocate_no_uring);
        const af::IoStatus rename_no_uring_status = af::io_renameat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary-old",
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary-new",
            0,
            rename_no_uring);
        const af::IoStatus unlink_no_uring_status = af::io_unlinkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary",
            0,
            unlink_no_uring);
        const af::IoStatus openat2_no_uring_status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat2-boundary",
            &how,
            &opened,
            openat2_no_uring);
        const af::IoStatus mkdir_no_uring_status = af::io_mkdirat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-mkdirat-boundary",
            0700U,
            mkdir_no_uring);
        const af::IoStatus symlink_no_uring_status = af::io_symlinkat(
            *this,
            IoTestThread::IO_0,
            "/tmp/asyncflow-symlinkat-target",
            AT_FDCWD,
            "/tmp/asyncflow-symlinkat-boundary",
            symlink_no_uring);
        const af::IoStatus link_no_uring_status = af::io_linkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-linkat-old",
            AT_FDCWD,
            "/tmp/asyncflow-linkat-new",
            0,
            link_no_uring);
        const af::IoStatus ftruncate_no_uring_status =
            af::io_ftruncate(*this, IoTestThread::IO_0, event.get(), 0, ftruncate_no_uring);
        const af::IoStatus stat_null_path_status = af::io_statx(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            0,
            STATX_SIZE,
            &stat,
            stat_null_path);
        const af::IoStatus stat_null_output_status = af::io_statx(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary",
            0,
            STATX_SIZE,
            nullptr,
            stat_null_output);
        const af::IoStatus fallocate_bad_fd_status = af::io_fallocate(
            *this,
            IoTestThread::IO_0,
            -1,
            FALLOC_FL_KEEP_SIZE,
            0,
            4096,
            fallocate_bad_fd);
        const af::IoStatus rename_null_old_status = af::io_renameat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary-new",
            0,
            rename_null_old);
        const af::IoStatus rename_null_new_status = af::io_renameat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-boundary-old",
            AT_FDCWD,
            nullptr,
            0,
            rename_null_new);
        const af::IoStatus unlink_null_path_status = af::io_unlinkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            0,
            unlink_null_path);
        const af::IoStatus openat2_null_path_status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            &how,
            &opened,
            openat2_null_path);
        const af::IoStatus openat2_null_how_status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat2-boundary",
            nullptr,
            &opened,
            openat2_null_how);
        const af::IoStatus openat2_null_output_status = af::io_openat2(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat2-boundary",
            &how,
            nullptr,
            openat2_null_output);
        const af::IoStatus mkdir_null_path_status = af::io_mkdirat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            0700U,
            mkdir_null_path);
        const af::IoStatus symlink_null_target_status = af::io_symlinkat(
            *this,
            IoTestThread::IO_0,
            nullptr,
            AT_FDCWD,
            "/tmp/asyncflow-symlinkat-boundary",
            symlink_null_target);
        const af::IoStatus symlink_null_path_status = af::io_symlinkat(
            *this,
            IoTestThread::IO_0,
            "/tmp/asyncflow-symlinkat-target",
            AT_FDCWD,
            nullptr,
            symlink_null_path);
        const af::IoStatus link_null_old_status = af::io_linkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            AT_FDCWD,
            "/tmp/asyncflow-linkat-new",
            0,
            link_null_old);
        const af::IoStatus link_null_new_status = af::io_linkat(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-linkat-old",
            AT_FDCWD,
            nullptr,
            0,
            link_null_new);
        const af::IoStatus ftruncate_bad_fd_status =
            af::io_ftruncate(*this, IoTestThread::IO_0, -1, 0, ftruncate_bad_fd);
        if (!null_path_status.failed() || null_path_status.error != EINVAL ||
            !null_output_status.failed() || null_output_status.error != EINVAL ||
            !no_uring_status.failed() || no_uring_status.error != ENOSYS ||
            !close_no_uring_status.failed() || close_no_uring_status.error != ENOSYS ||
            event.get() < 0 ||
            !stat_no_uring_status.failed() || stat_no_uring_status.error != ENOSYS ||
            !fallocate_no_uring_status.failed() || fallocate_no_uring_status.error != ENOSYS ||
            !rename_no_uring_status.failed() || rename_no_uring_status.error != ENOSYS ||
            !unlink_no_uring_status.failed() || unlink_no_uring_status.error != ENOSYS ||
            !openat2_no_uring_status.failed() || openat2_no_uring_status.error != ENOSYS ||
            !mkdir_no_uring_status.failed() || mkdir_no_uring_status.error != ENOSYS ||
            !symlink_no_uring_status.failed() || symlink_no_uring_status.error != ENOSYS ||
            !link_no_uring_status.failed() || link_no_uring_status.error != ENOSYS ||
            !ftruncate_no_uring_status.failed() || ftruncate_no_uring_status.error != ENOSYS ||
            !stat_null_path_status.failed() || stat_null_path_status.error != EINVAL ||
            !stat_null_output_status.failed() || stat_null_output_status.error != EINVAL ||
            !fallocate_bad_fd_status.failed() || fallocate_bad_fd_status.error != EBADF ||
            !rename_null_old_status.failed() || rename_null_old_status.error != EINVAL ||
            !rename_null_new_status.failed() || rename_null_new_status.error != EINVAL ||
            !unlink_null_path_status.failed() || unlink_null_path_status.error != EINVAL ||
            !openat2_null_path_status.failed() || openat2_null_path_status.error != EINVAL ||
            !openat2_null_how_status.failed() || openat2_null_how_status.error != EINVAL ||
            !openat2_null_output_status.failed() || openat2_null_output_status.error != EINVAL ||
            !mkdir_null_path_status.failed() || mkdir_null_path_status.error != EINVAL ||
            !symlink_null_target_status.failed() || symlink_null_target_status.error != EINVAL ||
            !symlink_null_path_status.failed() || symlink_null_path_status.error != EINVAL ||
            !link_null_old_status.failed() || link_null_old_status.error != EINVAL ||
            !link_null_new_status.failed() || link_null_new_status.error != EINVAL ||
            !ftruncate_bad_fd_status.failed() || ftruncate_bad_fd_status.error != EBADF ||
            opened != -1) {
            return failed();
        }
        error_->store(no_uring_status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

