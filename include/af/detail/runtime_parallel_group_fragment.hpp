#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_parallel_group_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    struct ParallelGroup {
        std::atomic<std::uint32_t> pending{0};
        Task* owner{nullptr};
        std::uint16_t resume_thread{invalid_thread_index};
        std::atomic<std::uint32_t> failed{0};

        void init(
            std::uint32_t target_count,
            Task* group_owner,
            std::uint16_t group_resume_thread) noexcept {
            pending.store(target_count, std::memory_order_relaxed);
            owner = group_owner;
            resume_thread = group_resume_thread;
            failed.store(0, std::memory_order_relaxed);
        }

        void complete(bool ok, bool resume_owner = true) noexcept {
            if (!ok) {
                failed.fetch_add(1, std::memory_order_relaxed);
            }
            if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
                if (resume_owner && owner != nullptr && resume_thread < thread_count) {
                    owner->set_last_parallel_failures(failed.load(std::memory_order_acquire));
                    post_blocking(thread_from_index(resume_thread), owner);
                }
                destroy_parallel_group(this);
            }
        }
    };
