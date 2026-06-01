#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_io_file_fixed_file_resource_boundary_tasks.hpp is a runtime_io_file_fixed_file_boundary_tasks implementation detail"
#endif

class FixedFileResourceBoundaryTask final : public IoTaskBase {
public:
    explicit FixedFileResourceBoundaryTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        std::atomic<int>* completed,
        std::atomic<int>* register_error,
        std::atomic<int>* unavailable_error,
        std::atomic<int>* invalid_error,
        std::atomic<int>* null_error) {
        completed_ = completed;
        register_error_ = register_error;
        unavailable_error_ = unavailable_error;
        invalid_error_ = invalid_error;
        null_error_ = null_error;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const int fd = -1;
        int register_error = 0;
        const bool registered =
            IoRuntime::io_register_files(IoTestThread::IO_0, &fd, 1, &register_error);
        if (registered || register_error != ENOSYS) {
            return failed();
        }
        int update_error = 0;
        const bool updated =
            IoRuntime::io_update_registered_files(IoTestThread::IO_0, 0, &fd, 1, &update_error);
        if (updated || update_error != ENOSYS) {
            return failed();
        }
        int null_update_error = 0;
        const bool null_update = IoRuntime::io_update_registered_files(
            IoTestThread::IO_0,
            0,
            nullptr,
            1,
            &null_update_error);
        if (null_update || null_update_error != EINVAL) {
            return failed();
        }

        af::IoFixedFile<IoTestThread> missing(IoTestThread::IO_0, 0);
        af::IoFixedFile<IoTestThread> invalid(IoTestThread::IO_0, -1);
        af::IoOpState unavailable{};
        af::IoOpState zero{};
        af::IoOpState bad{};
        af::IoOpState null_data{};
        af::IoOpState fixed_unavailable{};
        af::IoOpState fixed_bad{};
        af::IoOpState fixed_null{};
        af::IoOpState direct_null_path{};
        af::IoOpState direct_null_output{};
        af::IoOpState direct_bad_index{};
        af::IoOpState direct_unavailable{};
        char value = 0;
        af::IoFixedBuffer buffer{&value, sizeof(value), 0};

        const af::IoStatus unavailable_read =
            missing.read_at(*this, &value, sizeof(value), 0, unavailable);
        const af::IoStatus zero_read = invalid.read_at(*this, nullptr, 0, 0, zero);
        const af::IoStatus bad_read = invalid.read_at(*this, &value, sizeof(value), 0, bad);
        const af::IoStatus null_read =
            missing.read_at(*this, nullptr, sizeof(value), 0, null_data);
        const af::IoStatus fixed_unavailable_read =
            missing.read_fixed_at(*this, buffer, 0, fixed_unavailable);
        const af::IoStatus fixed_bad_read =
            invalid.read_fixed_at(*this, buffer, 0, fixed_bad);
        const af::IoStatus fixed_null_read = missing.read_fixed_at(
            *this,
            af::IoFixedBuffer{nullptr, sizeof(value), 0},
            0,
            fixed_null);

        af::IoFixedFile<IoTestThread> direct_file{};
        const af::IoStatus direct_null_path_status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            nullptr,
            O_RDONLY | O_CLOEXEC,
            0,
            0,
            &direct_file,
            direct_null_path);
        const af::IoStatus direct_null_output_status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-direct-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            0,
            nullptr,
            direct_null_output);
        const af::IoStatus direct_bad_index_status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-direct-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            -1,
            &direct_file,
            direct_bad_index);
        const af::IoStatus direct_unavailable_status = af::io_openat_direct(
            *this,
            IoTestThread::IO_0,
            AT_FDCWD,
            "/tmp/asyncflow-openat-direct-boundary",
            O_RDONLY | O_CLOEXEC,
            0,
            0,
            &direct_file,
            direct_unavailable);

        if (!unavailable_read.failed() || unavailable_read.error != ENOSYS ||
            !zero_read.ready() || zero_read.bytes != 0U ||
            !bad_read.failed() || bad_read.error != EBADF ||
            !null_read.failed() || null_read.error != EINVAL ||
            !fixed_unavailable_read.failed() || fixed_unavailable_read.error != ENOSYS ||
            !fixed_bad_read.failed() || fixed_bad_read.error != EBADF ||
            !fixed_null_read.failed() || fixed_null_read.error != EINVAL ||
            !direct_null_path_status.failed() || direct_null_path_status.error != EINVAL ||
            !direct_null_output_status.failed() || direct_null_output_status.error != EINVAL ||
            !direct_bad_index_status.failed() || direct_bad_index_status.error != EBADF ||
            !direct_unavailable_status.failed() || direct_unavailable_status.error != ENOSYS ||
            direct_file.valid()) {
            return failed();
        }

        register_error_->store(register_error, std::memory_order_release);
        unavailable_error_->store(unavailable_read.error, std::memory_order_release);
        invalid_error_->store(bad_read.error, std::memory_order_release);
        null_error_->store(null_read.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* register_error_{nullptr};
    std::atomic<int>* unavailable_error_{nullptr};
    std::atomic<int>* invalid_error_{nullptr};
    std::atomic<int>* null_error_{nullptr};
};
