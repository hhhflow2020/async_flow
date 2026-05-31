#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_core_state_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        Task* pop_one() noexcept {
            if (Task* task = try_pop_local()) {
                return task;
            }

            for (std::size_t word = 0; word < decltype(ready_sources_)::word_count; ++word) {
                std::uint64_t mask = ready_sources_.load_word(word);
                while (mask != 0U) {
                    const std::uint16_t source = static_cast<std::uint16_t>(
                        decltype(ready_sources_)::word_base(word) + std::countr_zero(mask));
                    const std::uint64_t bit = 1ULL << (source & 63U);
                    mask &= ~bit;
                    if (source == index_) {
                        continue;
                    }
                    if (Task* task = spsc_queue(source, index_).try_pop()) {
                        next_source_ = static_cast<std::uint16_t>((source + 1U) % thread_count);
                        return task;
                    }
                    ready_sources_.clear(source);
                    if (Task* task = spsc_queue(source, index_).try_pop()) {
                        next_source_ = static_cast<std::uint16_t>((source + 1U) % thread_count);
                        mark_ready(source);
                        return task;
                    }
                }
            }

            for (std::uint16_t checked = 0; checked < thread_count; ++checked) {
                const std::uint16_t source =
                    static_cast<std::uint16_t>((next_source_ + checked) % thread_count);
                if (source == index_) {
                    continue;
                }
                if (Task* task = spsc_queue(source, index_).try_pop()) {
                    next_source_ = static_cast<std::uint16_t>((source + 1U) % thread_count);
                    mark_ready(source);
                    return task;
                }
            }

            if (external_ready_.load(std::memory_order_acquire)) {
                if (Task* task = external_queues_[index_]->try_pop()) {
                    return task;
                }

                external_ready_.store(false, std::memory_order_release);
                if (Task* task = external_queues_[index_]->try_pop()) {
                    external_ready_.store(true, std::memory_order_release);
                    return task;
                }
            }

            return external_queues_[index_]->try_pop();
        }

        void finish_done(Task* task) noexcept {
            const std::uint16_t requested = task->take_requested_thread();
            AF_ASSERT(requested == invalid_thread_index);
            task->state_.store(TaskState::Done, std::memory_order_release);
            on_task_finished(task);
            task->release_lifetime_ref();
        }

        void finish_pending(Task* task) noexcept {
            task->state_.store(TaskState::Pending, std::memory_order_release);
            const std::uint16_t requested = task->take_requested_thread();
            if (requested != invalid_thread_index) {
                enqueue_pending_blocking(requested, task);
            }
        }

        void finish_again(Task* task) noexcept {
            const std::uint16_t requested = task->take_requested_thread();
            AF_ASSERT(requested == invalid_thread_index);
            task->state_.store(TaskState::Queued, std::memory_order_release);
            enqueue_ready_blocking_from_runtime_thread(index_, index_, task);
        }

        std::uint16_t index_;
        ThreadKind kind_{ThreadKind::Worker};
        std::uint16_t next_source_{0};
        std::vector<Task*> local_queue_;
        std::size_t local_head_{0};
        std::size_t local_tail_{0};
        std::size_t local_size_{0};
        detail::ReadySourceSet<thread_count> ready_sources_;
        CacheLineAtomic<bool> external_ready_{false};
        CacheLineAtomic<std::uint32_t> wake_epoch_{0};
        CacheLineAtomic<bool> sleeping_{false};
        CacheLineAtomic<bool> stop_requested_{false};
        Task* running_task_{nullptr};
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
        absl::flat_hash_map<int, IoWaitRegistration*> io_waits_;
        detail::ObjectPool<IoWaitRegistration> io_wait_pool_;
#endif
#if AF_DETAIL_HAS_EPOLL
        int io_epoll_fd_{-1};
        int io_wake_fd_{-1};
        absl::flat_hash_set<int> io_deferred_deletes_;
#endif
#if AF_DETAIL_HAS_KQUEUE
        int io_kqueue_fd_{-1};
        KqueueTimeoutRegistration* io_kqueue_timeouts_{nullptr};
        std::uint32_t io_kqueue_timeout_count_{0};
        uintptr_t io_kqueue_next_timeout_ident_{2};
        detail::ObjectPool<KqueueTimeoutRegistration> io_kqueue_timeout_pool_;
#endif
#if defined(__linux__)
        int io_uring_fd_{-1};
        std::byte* io_uring_sq_ring_{nullptr};
        std::byte* io_uring_cq_ring_{nullptr};
        io_uring_sqe* io_uring_sqes_{nullptr};
        std::size_t io_uring_sq_ring_size_{0};
        std::size_t io_uring_cq_ring_size_{0};
        std::size_t io_uring_sqes_size_{0};
        std::uint32_t* io_uring_sq_head_{nullptr};
        std::uint32_t* io_uring_sq_tail_{nullptr};
        std::uint32_t* io_uring_sq_ring_mask_{nullptr};
        std::uint32_t* io_uring_sq_ring_entries_{nullptr};
        std::uint32_t* io_uring_sq_array_{nullptr};
        std::uint32_t* io_uring_cq_head_{nullptr};
        std::uint32_t* io_uring_cq_tail_{nullptr};
        std::uint32_t* io_uring_cq_ring_mask_{nullptr};
        io_uring_cqe* io_uring_cqes_{nullptr};
        unsigned io_uring_pending_submissions_{0};
        bool io_uring_send_zc_available_{false};
        bool io_uring_sendmsg_zc_available_{false};
        bool io_uring_poll_add_available_{false};
        bool io_uring_socket_available_{false};
        bool io_uring_buffers_registered_{false};
        unsigned io_uring_registered_buffer_count_{0};
        std::vector<std::uint16_t> io_uring_provided_buffer_groups_;
        bool io_uring_files_registered_{false};
        unsigned io_uring_registered_file_count_{0};
        IoUringOperation* io_uring_operations_{nullptr};
        detail::ObjectPool<detail::IoUringMessage> io_uring_msg_pool_;
        detail::ObjectPool<detail::IoUringSocketAddress> io_uring_address_pool_;
        detail::ObjectPool<IoUringOperation> io_uring_op_pool_;
#endif
#if AF_DETAIL_HAS_EPOLL || AF_DETAIL_HAS_KQUEUE
        CacheLineAtomic<bool> io_wake_pending_{false};
#endif
        std::thread worker_;
