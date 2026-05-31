#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_socket_create_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool submit_io_uring_socket(
            int domain,
            int type,
            int protocol,
            std::uint32_t flags,
            Task* task,
            IoResult* result) noexcept {
            return submit_io_uring_socket_impl(domain, type, protocol, flags, task, result);
        }
