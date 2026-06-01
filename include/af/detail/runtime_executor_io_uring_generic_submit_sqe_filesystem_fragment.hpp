#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_generic_submit_sqe_filesystem_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        static void fill_io_uring_generic_fallocate_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = args.extra;
            sqe.len = static_cast<unsigned>(args.size);
            sqe.off = args.offset;
        }

        static void fill_io_uring_generic_splice_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = args.extra;
            sqe.len = static_cast<unsigned>(args.size);
            sqe.off = args.offset;
            sqe.splice_fd_in = args.extra_fd;
            sqe.splice_flags = args.op_flags;
        }

        static void fill_io_uring_generic_openat_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.len = static_cast<unsigned>(args.size);
            sqe.open_flags = args.op_flags;
        }

        static void fill_io_uring_generic_statx_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.len = static_cast<unsigned>(args.size);
            sqe.off = args.offset;
            sqe.statx_flags = args.op_flags;
        }

        static void fill_io_uring_generic_renameat_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.len = static_cast<unsigned>(args.size);
            sqe.off = args.offset;
            sqe.rename_flags = args.op_flags;
        }

        static void fill_io_uring_generic_unlinkat_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.unlink_flags = args.op_flags;
        }
#endif
