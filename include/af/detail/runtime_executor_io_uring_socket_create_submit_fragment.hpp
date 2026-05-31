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
#if defined(__linux__)
            return submit_io_uring_socket_impl(domain, type, protocol, flags, task, result);
#else
            static_cast<void>(domain);
            static_cast<void>(type);
            static_cast<void>(protocol);
            static_cast<void>(flags);
            static_cast<void>(task);
            if (result != nullptr) {
                result->fd = -1;
                result->events = io_error;
                result->error = ENOSYS;
            }
            return false;
#endif
        }
