#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_setup_close_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        void close_io_uring_backend() noexcept {
            clear_io_uring_operations();
            if (io_uring_fd_ >= 0 && io_uring_files_registered_) {
                static_cast<void>(detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_UNREGISTER_FILES,
                    nullptr,
                    0));
            }
            if (io_uring_fd_ >= 0 && io_uring_buffers_registered_) {
                static_cast<void>(detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_UNREGISTER_BUFFERS,
                    nullptr,
                    0));
            }
            if (io_uring_sqes_ != nullptr && io_uring_sqes_ != MAP_FAILED) {
                ::munmap(io_uring_sqes_, io_uring_sqes_size_);
            }
            if (io_uring_sq_ring_ != nullptr && io_uring_sq_ring_ != MAP_FAILED) {
                ::munmap(io_uring_sq_ring_, io_uring_sq_ring_size_);
            }
            if (io_uring_cq_ring_ != nullptr &&
                io_uring_cq_ring_ != MAP_FAILED &&
                io_uring_cq_ring_ != io_uring_sq_ring_) {
                ::munmap(io_uring_cq_ring_, io_uring_cq_ring_size_);
            }
            if (io_uring_fd_ >= 0) {
                ::close(io_uring_fd_);
            }

            io_uring_fd_ = -1;
            io_uring_sq_ring_ = nullptr;
            io_uring_cq_ring_ = nullptr;
            io_uring_sqes_ = nullptr;
            io_uring_sq_ring_size_ = 0;
            io_uring_cq_ring_size_ = 0;
            io_uring_sqes_size_ = 0;
            io_uring_sq_head_ = nullptr;
            io_uring_sq_tail_ = nullptr;
            io_uring_sq_ring_mask_ = nullptr;
            io_uring_sq_ring_entries_ = nullptr;
            io_uring_sq_array_ = nullptr;
            io_uring_cq_head_ = nullptr;
            io_uring_cq_tail_ = nullptr;
            io_uring_cq_ring_mask_ = nullptr;
            io_uring_cqes_ = nullptr;
            io_uring_pending_submissions_ = 0;
            io_uring_send_zc_available_ = false;
            io_uring_sendmsg_zc_available_ = false;
            io_uring_poll_add_available_ = false;
            io_uring_socket_available_ = false;
            io_uring_buffers_registered_ = false;
            io_uring_registered_buffer_count_ = 0;
            io_uring_provided_buffer_groups_.clear();
            io_uring_files_registered_ = false;
            io_uring_registered_file_count_ = 0;
        }
#endif
