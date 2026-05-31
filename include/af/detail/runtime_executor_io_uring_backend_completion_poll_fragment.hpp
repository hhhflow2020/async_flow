#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_completion_poll_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        [[nodiscard]] bool poll_io_uring_completions() noexcept {
            if (io_uring_fd_ < 0 || io_uring_cq_head_ == nullptr || io_uring_cq_tail_ == nullptr) {
                return false;
            }

            bool did_work = false;
            std::uint32_t head = __atomic_load_n(io_uring_cq_head_, __ATOMIC_ACQUIRE);
            const std::uint32_t tail = __atomic_load_n(io_uring_cq_tail_, __ATOMIC_ACQUIRE);
            while (head != tail) {
                io_uring_cqe& cqe = io_uring_cqes_[head & *io_uring_cq_ring_mask_];
                auto* operation = reinterpret_cast<IoUringOperation*>(cqe.user_data);
                if (operation != nullptr) {
                    const bool yield_to_task = complete_io_uring_operation(
                        operation,
                        cqe.res,
                        cqe.flags);
                    did_work = true;
                    ++head;
                    if (yield_to_task) {
                        break;
                    }
                    continue;
                }
                ++head;
            }
            __atomic_store_n(io_uring_cq_head_, head, __ATOMIC_RELEASE);
            return did_work;
        }
#endif
