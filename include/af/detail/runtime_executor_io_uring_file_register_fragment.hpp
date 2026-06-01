#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_file_register_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool register_io_uring_files(
            const int* files,
            unsigned file_count,
            int* error) noexcept {
            AF_ASSERT(current_thread_index_ == index_ && "io_uring file registration must run on its IO thread");
            if (error != nullptr) {
                *error = 0;
            }
            if (current_thread_index_ != index_ || files == nullptr || file_count == 0U) {
                if (error != nullptr) {
                    *error = EINVAL;
                }
                return false;
            }

#if defined(__linux__)
            if (io_uring_fd_ < 0) {
                if (error != nullptr) {
                    *error = ENOSYS;
                }
                return false;
            }
            if (io_uring_files_registered_) {
                if (error != nullptr) {
                    *error = EALREADY;
                }
                return false;
            }
            if (file_count > static_cast<unsigned>(std::numeric_limits<int>::max())) {
                if (error != nullptr) {
                    *error = EINVAL;
                }
                return false;
            }

            if (detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_REGISTER_FILES,
                    files,
                    file_count) != 0) {
                if (error != nullptr) {
                    *error = errno == 0 ? EIO : errno;
                }
                return false;
            }

            io_uring_files_registered_ = true;
            io_uring_registered_file_count_ = file_count;
            return true;
#else
            if (error != nullptr) {
                *error = ENOSYS;
            }
            return false;
#endif
        }
