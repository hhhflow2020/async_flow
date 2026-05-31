#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_local_queue_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool try_push_local(Task* task) noexcept {
            if (local_size_ == local_queue_.size()) {
                return false;
            }

            local_queue_[local_tail_ & (local_queue_.size() - 1U)] = task;
            ++local_tail_;
            ++local_size_;
            return true;
        }

        [[nodiscard]] Task* try_pop_local() noexcept {
            if (local_size_ == 0) {
                return nullptr;
            }

            Task* task = local_queue_[local_head_ & (local_queue_.size() - 1U)];
            ++local_head_;
            --local_size_;
            return task;
        }
