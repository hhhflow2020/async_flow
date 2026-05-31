#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_state_types_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if AF_DETAIL_HAS_NATIVE_IO_WAIT
        struct IoWaitRegistration {
            int fd{-1};
            std::uint32_t events{0};
            Task* task{nullptr};
            IoResult* result{nullptr};
#if defined(__linux__)
            IoUringOperation* poll_operation{nullptr};
#endif
        };

#endif

#if AF_DETAIL_HAS_KQUEUE
        struct KqueueTimeoutRegistration {
            Task* task{nullptr};
            IoResult* result{nullptr};
            KqueueTimeoutRegistration* prev{nullptr};
            KqueueTimeoutRegistration* next{nullptr};
            uintptr_t ident{0};
        };
#endif

#if defined(__linux__)
        struct IoUringOperation {
            Task* task{nullptr};
            IoResult* result{nullptr};
            IoUringOperation* prev{nullptr};
            IoUringOperation* next{nullptr};
            detail::IoUringMessage* msg{nullptr};
            union {
                detail::IoUringSocketAddress* socket_address;
                __kernel_timespec timeout;
            };
            IoWaitRegistration* wait_registration{nullptr};
            std::uint32_t complete_events{0};
            int direct_file_index{-1};
            std::uint8_t opcode{0};
            bool cancel_requested{false};
            bool multishot{false};
            bool poll_wait{false};
            bool zero_copy_send{false};
            bool zero_copy_primary_done{false};
            bool zero_copy_notification_done{false};
        };

        enum class IoUringPollSubmitResult : std::uint8_t {
            Submitted,
            Fallback,
            Failed,
            BackendClosed,
        };
#endif
