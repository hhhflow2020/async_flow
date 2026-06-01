#pragma once

template <typename TaskT>
[[nodiscard]] inline bool io_on_target_thread(typename TaskT::Thread thread) noexcept {
    return TaskT::Runtime::is_runtime_thread() &&
           TaskT::Runtime::current_thread_index() == TaskT::Runtime::thread_index(thread);
}

template <typename TaskT, typename NameFn>
[[nodiscard]] IoStatus io_socket_name(TaskT &task, typename TaskT::Thread thread, int fd,
                                      sockaddr *address, socklen_t *address_size,
                                      NameFn name_fn) noexcept {
    static_cast<void>(task);
    if (fd < 0) {
        return IoStatus::failed(EBADF);
    }
    if (address == nullptr || address_size == nullptr) {
        return IoStatus::failed(EINVAL);
    }
    if (!io_on_target_thread<TaskT>(thread)) {
        return IoStatus::failed(EINVAL);
    }
    for (;;) {
        if (name_fn(fd, address, address_size) == 0) {
            return IoStatus::ready(0);
        }
        const int error = errno == 0 ? EIO : errno;
        if (error == EINTR) {
            continue;
        }
        return IoStatus::failed(error);
    }
}
