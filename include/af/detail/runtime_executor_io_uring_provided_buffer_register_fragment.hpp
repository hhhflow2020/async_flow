#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_provided_buffer_register_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        [[nodiscard]] bool register_io_uring_provided_buffer_ring(
            void* ring,
            unsigned ring_entries,
            std::uint16_t buffer_group,
            int* error) noexcept {
            AF_ASSERT(current_thread_index_ == index_ && "io_uring provided buffer ring registration must run on its IO thread");
            if (error != nullptr) {
                *error = 0;
            }
            if (current_thread_index_ != index_ ||
                ring == nullptr ||
                ring_entries == 0U ||
                (ring_entries & (ring_entries - 1U)) != 0U) {
                if (error != nullptr) {
                    *error = EINVAL;
                }
                return false;
            }

#if defined(__linux__)
            if (io_uring_fd_ < 0) {
                if (error != nullptr) {
                    *error = ENOSYS;
                }
                return false;
            }
            if (provided_buffer_group_registered(buffer_group)) {
                if (error != nullptr) {
                    *error = EALREADY;
                }
                return false;
            }

            detail::IoUringBufferRingRegistration registration{};
            registration.ring_addr = reinterpret_cast<std::uint64_t>(ring);
            registration.ring_entries = ring_entries;
            registration.bgid = buffer_group;
            if (detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_REGISTER_PBUF_RING,
                    &registration,
                    1) != 0) {
                if (error != nullptr) {
                    *error = errno == 0 ? EIO : errno;
                }
                return false;
            }

            try {
                io_uring_provided_buffer_groups_.push_back(buffer_group);
            } catch (...) {
                detail::IoUringBufferRingRegistration undo{};
                undo.bgid = buffer_group;
                static_cast<void>(detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_UNREGISTER_PBUF_RING,
                    &undo,
                    1));
                if (error != nullptr) {
                    *error = ENOMEM;
                }
                return false;
            }
            return true;
#else
            if (error != nullptr) {
                *error = ENOSYS;
            }
            return false;
#endif
        }
