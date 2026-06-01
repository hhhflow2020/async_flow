#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_file_unregister_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool unregister_io_uring_files(int* error) noexcept {
            AF_ASSERT(current_thread_index_ == index_ && "io_uring file unregistration must run on its IO thread");
            if (error != nullptr) {
                *error = 0;
            }
            if (current_thread_index_ != index_) {
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
            if (!io_uring_files_registered_) {
                if (error != nullptr) {
                    *error = ENOENT;
                }
                return false;
            }
            if (io_uring_pending_submissions_ != 0U) {
                const int submit_error = flush_io_uring_submissions();
                if (submit_error != 0) {
                    if (error != nullptr) {
                        *error = submit_error;
                    }
                    fail_io_uring_backend(submit_error, nullptr);
                    return false;
                }
            }
            if (io_uring_operations_ != nullptr) {
                if (error != nullptr) {
                    *error = EBUSY;
                }
                return false;
            }

            if (detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_UNREGISTER_FILES,
                    nullptr,
                    0) != 0) {
                if (error != nullptr) {
                    *error = errno == 0 ? EIO : errno;
                }
                return false;
            }

            io_uring_files_registered_ = false;
            io_uring_registered_file_count_ = 0;
            return true;
#else
            if (error != nullptr) {
                *error = ENOSYS;
            }
            return false;
#endif
        }
