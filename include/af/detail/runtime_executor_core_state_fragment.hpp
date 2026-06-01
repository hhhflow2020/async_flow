#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_core_state_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        std::uint16_t index_;
        ThreadKind kind_{ThreadKind::Worker};
        std::uint16_t next_source_{0};
        std::uint16_t next_ready_word_{0};
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
        int io_uring_backend_error_{0};
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
