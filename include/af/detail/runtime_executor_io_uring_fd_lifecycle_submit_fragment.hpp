#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_fd_lifecycle_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_io_uring_openat(
            int dir_fd,
            const char* path,
            int flags,
            std::uint32_t mode,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_OPENAT,
                dir_fd,
                const_cast<char*>(path),
                mode,
                0,
                static_cast<std::uint32_t>(flags),
                io_readable,
                task,
                result);
#else
            static_cast<void>(dir_fd);
            static_cast<void>(path);
            static_cast<void>(flags);
            static_cast<void>(mode);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_openat_direct(
            int dir_fd,
            const char* path,
            int flags,
            std::uint32_t mode,
            int file_index,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_OPENAT,
                dir_fd,
                const_cast<char*>(path),
                mode,
                0,
                static_cast<std::uint32_t>(flags),
                io_readable,
                task,
                result,
                nullptr,
                0,
                nullptr,
                nullptr,
                0,
                nullptr,
                nullptr,
                nullptr,
                0,
                0,
                -1,
                0,
                false,
                false,
                false,
                0,
                false,
                file_index);
#else
            static_cast<void>(dir_fd);
            static_cast<void>(path);
            static_cast<void>(flags);
            static_cast<void>(mode);
            static_cast<void>(file_index);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_openat2(
            int dir_fd,
            const char* path,
            const struct open_how* how,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                detail::io_uring_op_openat2,
                dir_fd,
                io_readable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = dir_fd;
                    sqe.addr = reinterpret_cast<std::uint64_t>(path);
                    sqe.len = static_cast<unsigned>(sizeof(*how));
                    sqe.off = reinterpret_cast<std::uint64_t>(how);
                });
#else
            static_cast<void>(dir_fd);
            static_cast<void>(path);
            static_cast<void>(how);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_close(
            int fd,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_CLOSE,
                fd,
                nullptr,
                0,
                0,
                0,
                io_writable,
                task,
                result);
#else
            static_cast<void>(fd);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_shutdown(
            int fd,
            int how,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_SHUTDOWN,
                fd,
                nullptr,
                static_cast<std::size_t>(how),
                0,
                0,
                io_writable,
                task,
                result);
#else
            static_cast<void>(fd);
            static_cast<void>(how);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_statx(
            int dir_fd,
            const char* path,
            int flags,
            std::uint32_t mask,
            struct statx* output,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                IORING_OP_STATX,
                dir_fd,
                io_readable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = dir_fd;
                    sqe.addr = reinterpret_cast<std::uint64_t>(path);
                    sqe.len = mask;
                    sqe.off = reinterpret_cast<std::uint64_t>(output);
                    sqe.statx_flags = static_cast<std::uint32_t>(flags);
                });
#else
            static_cast<void>(dir_fd);
            static_cast<void>(path);
            static_cast<void>(flags);
            static_cast<void>(mask);
            static_cast<void>(output);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_fallocate(
            int fd,
            int mode,
            std::uint64_t offset,
            std::uint64_t length,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                IORING_OP_FALLOCATE,
                fd,
                io_writable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = fd;
                    sqe.addr = length;
                    sqe.len = static_cast<unsigned>(mode);
                    sqe.off = offset;
                });
#else
            static_cast<void>(fd);
            static_cast<void>(mode);
            static_cast<void>(offset);
            static_cast<void>(length);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

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

        [[nodiscard]] bool submit_io_uring_ftruncate(
            int fd,
            std::uint64_t length,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fast_sqe(
                detail::io_uring_op_ftruncate,
                fd,
                io_writable,
                task,
                result,
                [&](io_uring_sqe& sqe) noexcept {
                    sqe.fd = fd;
                    sqe.off = length;
                });
#else
            static_cast<void>(fd);
            static_cast<void>(length);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_splice(
            int in_fd,
            std::int64_t off_in,
            int out_fd,
            std::int64_t off_out,
            std::size_t count,
            unsigned int flags,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_op(
                IORING_OP_SPLICE,
                out_fd,
                nullptr,
                count,
                static_cast<std::uint64_t>(off_out),
                flags,
                io_writable,
                task,
                result,
                nullptr,
                0,
                nullptr,
                nullptr,
                0,
                nullptr,
                nullptr,
                nullptr,
                0,
                static_cast<std::uint64_t>(off_in),
                in_fd);
#else
            static_cast<void>(in_fd);
            static_cast<void>(off_in);
            static_cast<void>(out_fd);
            static_cast<void>(off_out);
            static_cast<void>(count);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

