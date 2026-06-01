#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_wait_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool register_io_wait(
            int fd,
            std::uint32_t events,
            Task* task,
            IoResult* result,
            bool prefer_rearm = false) noexcept {
            AF_ASSERT(current_thread_index_ == index_ && "io_wait must be called from its IO thread");
            if (current_thread_index_ != index_ || task == nullptr || result == nullptr) {
                if (result != nullptr) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = EINVAL;
                }
                return false;
            }

            return register_native_io_wait(fd, events, task, result, prefer_rearm);
        }

#if defined(__linux__)
        [[nodiscard]] bool cancel_io_completion(IoOpState& state) noexcept {
            if (io_uring_fd_ < 0) {
                state.wait.events = io_error;
                state.wait.error = ENOSYS;
                state.wait.result = -ENOSYS;
                return false;
            }

            auto* operation = static_cast<IoUringOperation*>(state.wait.completion_token);
            if (operation == nullptr || operation->result != &state.wait || operation->poll_wait) {
                state.wait.events = io_error;
                state.wait.error = ENOENT;
                state.wait.result = -ENOENT;
                return false;
            }
            if (operation->opcode == IORING_OP_CLOSE) {
                state.wait.events = io_error;
                state.wait.error = EOPNOTSUPP;
                state.wait.result = -EOPNOTSUPP;
                return false;
            }
            if (operation->cancel_requested) {
                return true;
            }

            const int submit_error = submit_io_uring_cancel(operation);
            if (submit_error != 0) {
                state.wait.events = io_error;
                state.wait.error = submit_error;
                state.wait.result = -submit_error;
                return false;
            }

            operation->cancel_requested = true;
            return true;
        }
#endif

        [[nodiscard]] bool cancel_io(IoOpState& state) noexcept {
            AF_ASSERT(current_thread_index_ == index_ && "cancel_io must be called from its IO thread");
            if (state.waiting && state.wait.error == ECANCELED) {
                return true;
            }
            if (current_thread_index_ != index_) {
                state.wait.events = io_error;
                state.wait.error = EINVAL;
                state.wait.result = -EINVAL;
                return false;
            }
            if (!state.waiting) {
                state.wait.events = io_error;
                state.wait.error = ENOENT;
                state.wait.result = -ENOENT;
                return false;
            }

#if defined(__linux__)
            if (state.wait_kind == IoWaitKind::Readiness) {
                return cancel_native_io_wait(state);
            }
            if (state.wait_kind == IoWaitKind::Completion) {
                return cancel_io_completion(state);
            }
#elif AF_DETAIL_HAS_NATIVE_IO_WAIT
            if (state.wait_kind == IoWaitKind::Readiness) {
                return cancel_native_io_wait(state);
            }
#if AF_DETAIL_HAS_KQUEUE
            if (state.wait_kind == IoWaitKind::Completion) {
                return cancel_kqueue_timeout(state);
            }
#endif
#endif
            state.wait.events = io_error;
            state.wait.error = ENOSYS;
            state.wait.result = -ENOSYS;
            return false;
        }
