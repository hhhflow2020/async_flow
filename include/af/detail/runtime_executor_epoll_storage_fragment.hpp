#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_epoll_storage_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        void defer_io_delete(int fd) {
            io_deferred_deletes_.insert(fd);
        }

        void forget_deferred_io_delete(int fd) noexcept {
            io_deferred_deletes_.erase(fd);
        }

        void flush_native_io_backend_deferred_deletes() noexcept {
            if (io_deferred_deletes_.empty()) {
                return;
            }
            for (int fd : io_deferred_deletes_) {
                static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));
            }
            io_deferred_deletes_.clear();
        }

        void clear_io_waits() noexcept {
            for (auto& entry : io_waits_) {
                io_wait_pool_.destroy(entry.second);
            }
            io_waits_.clear();
        }
