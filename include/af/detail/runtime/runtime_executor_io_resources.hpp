#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_io_resources.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::io_backend_available() const noexcept {
    return native_io_backend_available();
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::io_uring_backend_available() const noexcept {
#if defined(__linux__)
    return io_uring_fd_ >= 0;
#else
    return false;
#endif
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] int Executor<RuntimeT, TraitsT>::io_uring_backend_error() const noexcept {
#if defined(__linux__)
    return io_uring_fd_ >= 0 ? 0
                             : (io_uring_backend_error_ == 0 ? ENODEV : io_uring_backend_error_);
#else
    return ENOSYS;
#endif
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::io_uring_poll_available() const noexcept {
#if defined(__linux__)
    return io_uring_fd_ >= 0 && io_uring_poll_add_available_;
#else
    return false;
#endif
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::register_io_uring_buffers(const iovec *buffers,
                                                                          unsigned buffer_count,
                                                                          int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring buffer registration must run on its IO thread");
    if (error != nullptr) {
        *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_ || buffers == nullptr || buffer_count == 0U) {
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
    if (io_uring_buffers_registered_) {
        if (error != nullptr) {
            *error = EALREADY;
        }
        return false;
    }
    if (buffer_count > static_cast<unsigned>(std::numeric_limits<std::uint16_t>::max())) {
        if (error != nullptr) {
            *error = EINVAL;
        }
        return false;
    }

    if (detail::sys_io_uring_register(io_uring_fd_, IORING_REGISTER_BUFFERS, buffers,
                                      buffer_count) != 0) {
        if (error != nullptr) {
            *error = errno == 0 ? EIO : errno;
        }
        return false;
    }

    io_uring_buffers_registered_ = true;
    io_uring_registered_buffer_count_ = buffer_count;
    return true;
#else
    if (error != nullptr) {
        *error = ENOSYS;
    }
    return false;
#endif
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::unregister_io_uring_buffers(int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring buffer unregistration must run on its IO thread");
    if (error != nullptr) {
        *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_) {
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
    if (!io_uring_buffers_registered_) {
        if (error != nullptr) {
            *error = ENOENT;
        }
        return false;
    }
    if (io_uring_pending_submissions_ != 0U) {
        const int submit_error = flush_io_uring_submissions();
        if (submit_error != 0) {
            if (error != nullptr) {
                *error = submit_error;
            }
            fail_io_uring_backend(submit_error, nullptr);
            return false;
        }
    }
    if (io_uring_operations_ != nullptr) {
        if (error != nullptr) {
            *error = EBUSY;
        }
        return false;
    }

    if (detail::sys_io_uring_register(io_uring_fd_, IORING_UNREGISTER_BUFFERS, nullptr, 0) != 0) {
        if (error != nullptr) {
            *error = errno == 0 ? EIO : errno;
        }
        return false;
    }

    io_uring_buffers_registered_ = false;
    io_uring_registered_buffer_count_ = 0;
    return true;
#else
    if (error != nullptr) {
        *error = ENOSYS;
    }
    return false;
#endif
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::register_io_uring_provided_buffer_ring(
    void *ring, unsigned ring_entries, std::uint16_t buffer_group, int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring provided buffer ring registration must run on its IO thread");
    if (error != nullptr) {
        *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_ || ring == nullptr || ring_entries == 0U ||
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
    if (detail::sys_io_uring_register(io_uring_fd_, IORING_REGISTER_PBUF_RING, &registration, 1) !=
        0) {
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
        static_cast<void>(
            detail::sys_io_uring_register(io_uring_fd_, IORING_UNREGISTER_PBUF_RING, &undo, 1));
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

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::unregister_io_uring_provided_buffer_ring(std::uint16_t buffer_group,
                                                                      int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring provided buffer ring unregistration must run on its IO "
              "thread");
    if (error != nullptr) {
        *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_) {
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
    auto it = std::find(io_uring_provided_buffer_groups_.begin(),
                        io_uring_provided_buffer_groups_.end(), buffer_group);
    if (it == io_uring_provided_buffer_groups_.end()) {
        if (error != nullptr) {
            *error = ENOENT;
        }
        return false;
    }
    if (io_uring_pending_submissions_ != 0U) {
        const int submit_error = flush_io_uring_submissions();
        if (submit_error != 0) {
            if (error != nullptr) {
                *error = submit_error;
            }
            fail_io_uring_backend(submit_error, nullptr);
            return false;
        }
    }
    if (io_uring_operations_ != nullptr) {
        if (error != nullptr) {
            *error = EBUSY;
        }
        return false;
    }

    detail::IoUringBufferRingRegistration registration{};
    registration.bgid = buffer_group;
    if (detail::sys_io_uring_register(io_uring_fd_, IORING_UNREGISTER_PBUF_RING, &registration,
                                      1) != 0) {
        if (error != nullptr) {
            *error = errno == 0 ? EIO : errno;
        }
        return false;
    }

    io_uring_provided_buffer_groups_.erase(it);
    return true;
#else
    if (error != nullptr) {
        *error = ENOSYS;
    }
    return false;
#endif
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::register_io_uring_files(const int *files,
                                                                        unsigned file_count,
                                                                        int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring file registration must run on its IO thread");
    if (error != nullptr) {
        *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_ || files == nullptr || file_count == 0U) {
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
    if (io_uring_files_registered_) {
        if (error != nullptr) {
            *error = EALREADY;
        }
        return false;
    }
    if (file_count > static_cast<unsigned>(std::numeric_limits<int>::max())) {
        if (error != nullptr) {
            *error = EINVAL;
        }
        return false;
    }

    if (detail::sys_io_uring_register(io_uring_fd_, IORING_REGISTER_FILES, files, file_count) !=
        0) {
        if (error != nullptr) {
            *error = errno == 0 ? EIO : errno;
        }
        return false;
    }

    io_uring_files_registered_ = true;
    io_uring_registered_file_count_ = file_count;
    return true;
#else
    if (error != nullptr) {
        *error = ENOSYS;
    }
    return false;
#endif
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::unregister_io_uring_files(int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring file unregistration must run on its IO thread");
    if (error != nullptr) {
        *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_) {
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
    if (!io_uring_files_registered_) {
        if (error != nullptr) {
            *error = ENOENT;
        }
        return false;
    }
    if (io_uring_pending_submissions_ != 0U) {
        const int submit_error = flush_io_uring_submissions();
        if (submit_error != 0) {
            if (error != nullptr) {
                *error = submit_error;
            }
            fail_io_uring_backend(submit_error, nullptr);
            return false;
        }
    }
    if (io_uring_operations_ != nullptr) {
        if (error != nullptr) {
            *error = EBUSY;
        }
        return false;
    }

    if (detail::sys_io_uring_register(io_uring_fd_, IORING_UNREGISTER_FILES, nullptr, 0) != 0) {
        if (error != nullptr) {
            *error = errno == 0 ? EIO : errno;
        }
        return false;
    }

    io_uring_files_registered_ = false;
    io_uring_registered_file_count_ = 0;
    return true;
#else
    if (error != nullptr) {
        *error = ENOSYS;
    }
    return false;
#endif
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::update_io_uring_files(unsigned offset, const int *files,
                                                   unsigned file_count, int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring file update must run on its IO thread");
    if (error != nullptr) {
        *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_ || files == nullptr || file_count == 0U) {
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
    if (!io_uring_files_registered_) {
        if (error != nullptr) {
            *error = ENOENT;
        }
        return false;
    }
    if (offset > io_uring_registered_file_count_ ||
        file_count > io_uring_registered_file_count_ - offset) {
        if (error != nullptr) {
            *error = EINVAL;
        }
        return false;
    }
    if (io_uring_pending_submissions_ != 0U) {
        const int submit_error = flush_io_uring_submissions();
        if (submit_error != 0) {
            if (error != nullptr) {
                *error = submit_error;
            }
            fail_io_uring_backend(submit_error, nullptr);
            return false;
        }
    }
    if (io_uring_operations_ != nullptr) {
        if (error != nullptr) {
            *error = EBUSY;
        }
        return false;
    }

    io_uring_files_update update{};
    update.offset = offset;
    update.fds = reinterpret_cast<std::uint64_t>(files);
    const int updated = detail::sys_io_uring_register(io_uring_fd_, IORING_REGISTER_FILES_UPDATE,
                                                      &update, file_count);
    if (updated < 0) {
        if (error != nullptr) {
            *error = errno == 0 ? EIO : errno;
        }
        return false;
    }
    if (static_cast<unsigned>(updated) != file_count) {
        if (error != nullptr) {
            *error = EIO;
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

} // namespace af::detail
