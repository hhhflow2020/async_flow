#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_thread_kind_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool io_thread() const noexcept {
            return native_io_thread()
#if defined(__linux__)
                || io_uring_thread()
#endif
                ;
        }

        [[nodiscard]] bool native_io_thread() const noexcept {
#if AF_DETAIL_HAS_EPOLL
            return kind_ == ThreadKind::Io ||
                   kind_ == ThreadKind::Epoll ||
                   kind_ == ThreadKind::IoUring;
#elif AF_DETAIL_HAS_KQUEUE
            return kind_ == ThreadKind::Io || kind_ == ThreadKind::Kqueue;
#else
            return false;
#endif
        }

        [[nodiscard]] bool io_uring_thread() const noexcept {
            return kind_ == ThreadKind::IoUring;
        }
