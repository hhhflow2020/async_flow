#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_filesystem_namespace_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_io_uring_renameat(
            int old_dir_fd,
            const char* old_path,
            int new_dir_fd,
            const char* new_path,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                IORING_OP_RENAMEAT,
                old_dir_fd,
                io_writable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = old_dir_fd;
                    sqe.addr = reinterpret_cast<std::uint64_t>(old_path);
                    sqe.len = static_cast<unsigned>(new_dir_fd);
                    sqe.off = reinterpret_cast<std::uint64_t>(new_path);
                    sqe.rename_flags = flags;
                });
#else
            static_cast<void>(old_dir_fd);
            static_cast<void>(old_path);
            static_cast<void>(new_dir_fd);
            static_cast<void>(new_path);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_unlinkat(
            int dir_fd,
            const char* path,
            int flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                IORING_OP_UNLINKAT,
                dir_fd,
                io_writable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = dir_fd;
                    sqe.addr = reinterpret_cast<std::uint64_t>(path);
                    sqe.unlink_flags = static_cast<std::uint32_t>(flags);
                });
#else
            static_cast<void>(dir_fd);
            static_cast<void>(path);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_mkdirat(
            int dir_fd,
            const char* path,
            std::uint32_t mode,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                detail::io_uring_op_mkdirat,
                dir_fd,
                io_writable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = dir_fd;
                    sqe.addr = reinterpret_cast<std::uint64_t>(path);
                    sqe.len = mode;
                });
#else
            static_cast<void>(dir_fd);
            static_cast<void>(path);
            static_cast<void>(mode);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_symlinkat(
            const char* target,
            int new_dir_fd,
            const char* link_path,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                detail::io_uring_op_symlinkat,
                new_dir_fd,
                io_writable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = new_dir_fd;
                    sqe.addr = reinterpret_cast<std::uint64_t>(target);
                    sqe.off = reinterpret_cast<std::uint64_t>(link_path);
                });
#else
            static_cast<void>(target);
            static_cast<void>(new_dir_fd);
            static_cast<void>(link_path);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_linkat(
            int old_dir_fd,
            const char* old_path,
            int new_dir_fd,
            const char* new_path,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                detail::io_uring_op_linkat,
                old_dir_fd,
                io_writable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = old_dir_fd;
                    sqe.addr = reinterpret_cast<std::uint64_t>(old_path);
                    sqe.len = static_cast<unsigned>(new_dir_fd);
                    sqe.off = reinterpret_cast<std::uint64_t>(new_path);
                    sqe.rw_flags = flags;
                });
#else
            static_cast<void>(old_dir_fd);
            static_cast<void>(old_path);
            static_cast<void>(new_dir_fd);
            static_cast<void>(new_path);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }
