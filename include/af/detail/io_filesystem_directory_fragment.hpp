#if !defined(AF_IO_FILESYSTEM_FRAGMENT_INCLUDE)
#error "io_filesystem_directory_fragment.hpp is an io_filesystem implementation fragment"
#endif

template <typename ThreadT>
class IoDirectory {
public:
    constexpr IoDirectory() noexcept = default;
    constexpr IoDirectory(ThreadT thread, int fd) noexcept : thread_(thread), fd_(fd) {}

    void reset(ThreadT thread, int fd) noexcept {
        thread_ = thread;
        fd_ = fd;
    }

    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }

    [[nodiscard]] ThreadT thread() const noexcept {
        return thread_;
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus openat2(
        TaskT& task,
        const char* path,
        const struct open_how* how,
        int* opened_fd,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDirectory thread type must match the task runtime thread type");
        return af::io_openat2(task, thread_, fd_, path, how, opened_fd, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus mkdirat(
        TaskT& task,
        const char* path,
        std::uint32_t mode,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDirectory thread type must match the task runtime thread type");
        return af::io_mkdirat(task, thread_, fd_, path, mode, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus symlinkat(
        TaskT& task,
        const char* target,
        const char* link_path,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDirectory thread type must match the task runtime thread type");
        return af::io_symlinkat(task, thread_, target, fd_, link_path, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus linkat(
        TaskT& task,
        int old_dir_fd,
        const char* old_path,
        const char* new_path,
        std::uint32_t flags,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDirectory thread type must match the task runtime thread type");
        return af::io_linkat(task, thread_, old_dir_fd, old_path, fd_, new_path, flags, state);
    }

private:
    ThreadT thread_{};
    int fd_{-1};
};
