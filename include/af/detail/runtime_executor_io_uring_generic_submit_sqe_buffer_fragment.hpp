#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_generic_submit_sqe_buffer_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        static void fill_io_uring_generic_fixed_buffer_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.len = static_cast<unsigned>(args.size);
            sqe.off = args.offset;
            sqe.buf_index = args.fixed_buffer_index;
        }

        static void fill_io_uring_generic_buffer_sqe(
            io_uring_sqe& sqe,
            const IoUringGenericSubmitArgs& args) noexcept {
            sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
            sqe.len = static_cast<unsigned>(args.size);
            sqe.off = args.offset;
        }
#endif
