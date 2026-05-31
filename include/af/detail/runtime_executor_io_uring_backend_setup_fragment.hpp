#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_setup_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        void init_io_uring_backend() noexcept {
            if (io_uring_fd_ >= 0 || io_wake_fd_ < 0) {
                return;
            }

            io_uring_params params{};
            unsigned requested_setup_flags = io_uring_setup_flags;
            if constexpr (io_uring_setup_sqpoll || io_uring_sqpoll_cpu >= 0) {
                requested_setup_flags |= IORING_SETUP_SQPOLL;
            }
            if constexpr (io_uring_setup_submit_all) {
                requested_setup_flags |= IORING_SETUP_SUBMIT_ALL;
            }
            if constexpr (io_uring_setup_coop_taskrun) {
                requested_setup_flags |= IORING_SETUP_COOP_TASKRUN;
            }
            if constexpr (io_uring_setup_single_issuer || io_uring_setup_defer_taskrun) {
                requested_setup_flags |= IORING_SETUP_SINGLE_ISSUER;
            }
            if constexpr (io_uring_setup_defer_taskrun) {
                requested_setup_flags |= IORING_SETUP_DEFER_TASKRUN;
            }
            detail::configure_io_uring_params(
                params,
                detail::IoUringSetupRequest{
                    requested_setup_flags,
                    io_uring_cq_entries,
                    io_uring_sqpoll_idle_ms,
                    io_uring_sqpoll_cpu});
            io_uring_fd_ = detail::sys_io_uring_setup(io_uring_entries, &params);
            if (io_uring_fd_ < 0) {
                return;
            }

            const std::size_t sq_ring_size =
                params.sq_off.array + static_cast<std::size_t>(params.sq_entries) * sizeof(std::uint32_t);
            const std::size_t cq_ring_size =
                params.cq_off.cqes + static_cast<std::size_t>(params.cq_entries) * sizeof(io_uring_cqe);

            if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0U) {
                io_uring_sq_ring_size_ = std::max(sq_ring_size, cq_ring_size);
                io_uring_cq_ring_size_ = io_uring_sq_ring_size_;
                io_uring_sq_ring_ = static_cast<std::byte*>(::mmap(
                    nullptr,
                    io_uring_sq_ring_size_,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE,
                    io_uring_fd_,
                    IORING_OFF_SQ_RING));
                io_uring_cq_ring_ = io_uring_sq_ring_;
            } else {
                io_uring_sq_ring_size_ = sq_ring_size;
                io_uring_cq_ring_size_ = cq_ring_size;
                io_uring_sq_ring_ = static_cast<std::byte*>(::mmap(
                    nullptr,
                    io_uring_sq_ring_size_,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE,
                    io_uring_fd_,
                    IORING_OFF_SQ_RING));
                io_uring_cq_ring_ = static_cast<std::byte*>(::mmap(
                    nullptr,
                    io_uring_cq_ring_size_,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE,
                    io_uring_fd_,
                    IORING_OFF_CQ_RING));
            }

            io_uring_sqes_size_ = static_cast<std::size_t>(params.sq_entries) * sizeof(io_uring_sqe);
            io_uring_sqes_ = static_cast<io_uring_sqe*>(::mmap(
                nullptr,
                io_uring_sqes_size_,
                PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_POPULATE,
                io_uring_fd_,
                IORING_OFF_SQES));

            if (io_uring_sq_ring_ == MAP_FAILED ||
                io_uring_cq_ring_ == MAP_FAILED ||
                io_uring_sqes_ == MAP_FAILED) {
                close_io_uring_backend();
                return;
            }

            io_uring_sq_head_ = ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.head);
            io_uring_sq_tail_ = ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.tail);
            io_uring_sq_ring_mask_ = ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.ring_mask);
            io_uring_sq_ring_entries_ = ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.ring_entries);
            io_uring_sq_array_ = ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.array);
            io_uring_cq_head_ = ptr_at<std::uint32_t>(io_uring_cq_ring_, params.cq_off.head);
            io_uring_cq_tail_ = ptr_at<std::uint32_t>(io_uring_cq_ring_, params.cq_off.tail);
            io_uring_cq_ring_mask_ = ptr_at<std::uint32_t>(io_uring_cq_ring_, params.cq_off.ring_mask);
            io_uring_cqes_ = ptr_at<io_uring_cqe>(io_uring_cq_ring_, params.cq_off.cqes);
            detect_io_uring_features();

            if (detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_REGISTER_EVENTFD,
                    &io_wake_fd_,
                    1) != 0) {
                close_io_uring_backend();
            }
        }

        template <typename T>
        [[nodiscard]] static T* ptr_at(std::byte* base, std::uint32_t offset) noexcept {
            return reinterpret_cast<T*>(base + offset);
        }

        void detect_io_uring_features() noexcept {
            io_uring_send_zc_available_ = false;
            io_uring_sendmsg_zc_available_ = false;
            io_uring_poll_add_available_ = false;
            io_uring_socket_available_ = false;

            constexpr unsigned probe_count = 64;
            std::array<
                std::byte,
                sizeof(io_uring_probe) + probe_count * sizeof(io_uring_probe_op)>
                storage{};
            auto* probe = reinterpret_cast<io_uring_probe*>(storage.data());
            if (detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_REGISTER_PROBE,
                    probe,
                    probe_count) != 0) {
                return;
            }

            const auto* ops = reinterpret_cast<const io_uring_probe_op*>(
                storage.data() + sizeof(io_uring_probe));
            const unsigned op_count = std::min<unsigned>(probe->ops_len, probe_count);
            for (unsigned i = 0; i < op_count; ++i) {
                if (ops[i].op == detail::io_uring_op_send_zc &&
                    (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
                    io_uring_send_zc_available_ = true;
                } else if (ops[i].op == detail::io_uring_op_sendmsg_zc &&
                           (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
                    io_uring_sendmsg_zc_available_ = true;
                } else if (ops[i].op == IORING_OP_POLL_ADD &&
                           (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
                    io_uring_poll_add_available_ = true;
                } else if (ops[i].op == detail::io_uring_op_socket &&
                           (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
                    io_uring_socket_available_ = true;
                }
            }
        }

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

        void reserve_io_backend_storage() noexcept {
            try {
                if constexpr (io_wait_reserve != 0U) {
                    io_waits_.reserve(io_wait_reserve);
                }
                if constexpr (io_deferred_delete_reserve != 0U) {
                    io_deferred_deletes_.reserve(io_deferred_delete_reserve);
                }
                if constexpr (io_uring_provided_buffer_group_reserve != 0U) {
                    io_uring_provided_buffer_groups_.reserve(
                        io_uring_provided_buffer_group_reserve);
                }
            } catch (...) {
            }
        }
#endif
