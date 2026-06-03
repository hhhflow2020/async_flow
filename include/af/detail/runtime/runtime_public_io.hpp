#pragma once

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>

#include <sys/socket.h>
#include <sys/uio.h>

#include "af/detail/io/filesystem/io_filesystem_platform.hpp"

namespace af::detail {

template <typename RuntimeT, typename TraitsT> struct RuntimePublicIo {
    using Thread = typename RuntimeConfig<TraitsT>::Thread;
    using Task = BasicTask<RuntimeT>;

private:
    [[nodiscard]] static bool fail_io_result(IoResult *result, int fd, int error) noexcept {
        if (result != nullptr) {
            detail::set_io_result_error(*result, fd, error);
        }
        return false;
    }

    [[nodiscard]] static bool fail_io_state(IoOpState &state, int fd, int error) noexcept {
        detail::set_io_result_error(state.wait, fd, error);
        return false;
    }

    template <typename Submit>
    [[nodiscard]] static bool submit_to_executor(Thread thread, int fd, IoResult *result,
                                                 Submit submit) noexcept {
        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return submit(*RuntimeT::executors_[index]);
    }

    template <typename Submit>
    [[nodiscard]] static bool submit_linux_only_to_executor(Thread thread, int fd, IoResult *result,
                                                            Submit submit) noexcept {
        if constexpr (detail::platform_linux) {
            return submit_to_executor(thread, fd, result, submit);
        } else {
            static_cast<void>(thread);
            static_cast<void>(submit);
            return fail_io_result(result, fd, ENOSYS);
        }
    }

public:
    [[nodiscard]] static bool io_backend_available(Thread thread) noexcept {
        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return false;
        }
        return RuntimeT::executors_[index]->io_backend_available();
    }

    [[nodiscard]] static bool io_uring_backend_available(Thread thread) noexcept {
        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return false;
        }
        return RuntimeT::executors_[index]->io_uring_backend_available();
    }

    [[nodiscard]] static int io_uring_backend_error(Thread thread) noexcept {
        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return EINVAL;
        }
        return RuntimeT::executors_[index]->io_uring_backend_error();
    }

    [[nodiscard]] static bool io_uring_poll_available(Thread thread) noexcept {
        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return false;
        }
        return RuntimeT::executors_[index]->io_uring_poll_available();
    }

    [[nodiscard]] static bool io_register_buffers(Thread thread, const iovec *buffers,
                                                  unsigned buffer_count,
                                                  int *error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }
        if (buffers == nullptr || buffer_count == 0U) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return RuntimeT::executors_[index]->register_io_uring_buffers(buffers, buffer_count, error);
    }

    [[nodiscard]] static bool io_unregister_buffers(Thread thread, int *error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return RuntimeT::executors_[index]->unregister_io_uring_buffers(error);
    }

    [[nodiscard]] static bool io_register_provided_buffer_ring(Thread thread, void *ring,
                                                               unsigned ring_entries,
                                                               std::uint16_t buffer_group,
                                                               int *error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }
        if (ring == nullptr || ring_entries == 0U) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return RuntimeT::executors_[index]->register_io_uring_provided_buffer_ring(
            ring, ring_entries, buffer_group, error);
    }

    [[nodiscard]] static bool io_unregister_provided_buffer_ring(Thread thread,
                                                                 std::uint16_t buffer_group,
                                                                 int *error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return RuntimeT::executors_[index]->unregister_io_uring_provided_buffer_ring(buffer_group,
                                                                                     error);
    }

    [[nodiscard]] static bool io_register_files(Thread thread, const int *files,
                                                unsigned file_count,
                                                int *error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }
        if (files == nullptr || file_count == 0U) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return RuntimeT::executors_[index]->register_io_uring_files(files, file_count, error);
    }

    [[nodiscard]] static bool io_update_registered_files(Thread thread, unsigned offset,
                                                         const int *files, unsigned file_count,
                                                         int *error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }
        if (files == nullptr || file_count == 0U) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return RuntimeT::executors_[index]->update_io_uring_files(offset, files, file_count, error);
    }

    [[nodiscard]] static bool io_unregister_files(Thread thread, int *error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return RuntimeT::executors_[index]->unregister_io_uring_files(error);
    }

    [[nodiscard]] static bool io_wait(Thread thread, int fd, std::uint32_t events, Task *task,
                                      IoResult *result, bool prefer_rearm = false) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || events == 0U) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->register_io_wait(fd, events, task, result,
                                                             prefer_rearm);
    }

    [[nodiscard]] static bool cancel_io(Thread thread, IoOpState &state) noexcept {
        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_state(state, -1, EINVAL);
        }
        return RuntimeT::executors_[index]->cancel_io(state);
    }

    [[nodiscard]] static bool io_submit_timeout(Thread thread, std::chrono::nanoseconds timeout,
                                                Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || timeout.count() <= 0) {
            return fail_io_result(result, -1, EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, -1, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_timeout(timeout, task, result);
    }

    [[nodiscard]] static bool io_submit_read_at(Thread thread, int fd, void *data, std::size_t size,
                                                std::uint64_t offset, Task *task,
                                                IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_read(fd, data, size, offset, task,
                                                                 result);
    }

    [[nodiscard]] static bool io_submit_write_at(Thread thread, int fd, const void *data,
                                                 std::size_t size, std::uint64_t offset, Task *task,
                                                 IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_write(fd, data, size, offset, task,
                                                                  result);
    }

    [[nodiscard]] static bool io_submit_fsync(Thread thread, int fd, std::uint32_t flags,
                                              Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_fsync(fd, flags, task, result);
    }

    [[nodiscard]] static bool io_submit_read_fixed_file_at(Thread thread, int file_index,
                                                           void *data, std::size_t size,
                                                           std::uint64_t offset,
                                                           std::uint16_t buffer_index, Task *task,
                                                           IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            return fail_io_result(result, file_index, file_index < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, file_index, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_read_fixed_file(
            file_index, data, size, offset, buffer_index, task, result);
    }

    [[nodiscard]] static bool io_submit_write_fixed_file_at(Thread thread, int file_index,
                                                            const void *data, std::size_t size,
                                                            std::uint64_t offset,
                                                            std::uint16_t buffer_index, Task *task,
                                                            IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            return fail_io_result(result, file_index, file_index < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, file_index, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_write_fixed_file(
            file_index, data, size, offset, buffer_index, task, result);
    }

    [[nodiscard]] static bool io_submit_read_fixed_at(Thread thread, int fd, void *data,
                                                      std::size_t size, std::uint64_t offset,
                                                      std::uint16_t buffer_index, Task *task,
                                                      IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_read_fixed(fd, data, size, offset,
                                                                       buffer_index, task, result);
    }

    [[nodiscard]] static bool io_submit_write_fixed_at(Thread thread, int fd, const void *data,
                                                       std::size_t size, std::uint64_t offset,
                                                       std::uint16_t buffer_index, Task *task,
                                                       IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_write_fixed(fd, data, size, offset,
                                                                        buffer_index, task, result);
    }

    [[nodiscard]] static bool io_submit_read_fixed_file_at(Thread thread, int file_index,
                                                           void *data, std::size_t size,
                                                           std::uint64_t offset, Task *task,
                                                           IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            return fail_io_result(result, file_index, file_index < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, file_index, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_read_fixed_file(file_index, data, size,
                                                                            offset, task, result);
    }

    [[nodiscard]] static bool io_submit_write_fixed_file_at(Thread thread, int file_index,
                                                            const void *data, std::size_t size,
                                                            std::uint64_t offset, Task *task,
                                                            IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            return fail_io_result(result, file_index, file_index < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, file_index, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_write_fixed_file(file_index, data, size,
                                                                             offset, task, result);
    }

    [[nodiscard]] static bool io_submit_fsync_fixed_file(Thread thread, int file_index,
                                                         std::uint32_t flags, Task *task,
                                                         IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0) {
            return fail_io_result(result, file_index, file_index < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, file_index, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_fsync_fixed_file(file_index, flags,
                                                                             task, result);
    }

    [[nodiscard]] static bool io_submit_readv_fixed_file_at(Thread thread, int file_index,
                                                            const iovec *iov, int iov_count,
                                                            std::uint64_t offset, Task *task,
                                                            IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || iov == nullptr ||
            iov_count <= 0) {
            return fail_io_result(result, file_index, file_index < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, file_index, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_readv_fixed_file(
            file_index, iov, iov_count, offset, task, result);
    }

    [[nodiscard]] static bool io_submit_writev_fixed_file_at(Thread thread, int file_index,
                                                             const iovec *iov, int iov_count,
                                                             std::uint64_t offset, Task *task,
                                                             IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || iov == nullptr ||
            iov_count <= 0) {
            return fail_io_result(result, file_index, file_index < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, file_index, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_writev_fixed_file(
            file_index, iov, iov_count, offset, task, result);
    }

    [[nodiscard]] static bool io_submit_readv_at(Thread thread, int fd, const iovec *iov,
                                                 int iov_count, std::uint64_t offset, Task *task,
                                                 IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || iov == nullptr || iov_count <= 0) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_readv(fd, iov, iov_count, offset, task,
                                                                  result);
    }

    [[nodiscard]] static bool io_submit_writev_at(Thread thread, int fd, const iovec *iov,
                                                  int iov_count, std::uint64_t offset, Task *task,
                                                  IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || iov == nullptr || iov_count <= 0) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_writev(fd, iov, iov_count, offset, task,
                                                                   result);
    }

    [[nodiscard]] static bool io_submit_openat(Thread thread, int dir_fd, const char *path,
                                               int flags, std::uint32_t mode, Task *task,
                                               IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr) {
            return fail_io_result(result, dir_fd, EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, dir_fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_openat(dir_fd, path, flags, mode, task,
                                                                   result);
    }

    [[nodiscard]] static bool io_submit_socket(Thread thread, int domain, int type, int protocol,
                                               std::uint32_t flags, Task *task,
                                               IoResult *result) noexcept {
        if (task == nullptr || result == nullptr) {
            return fail_io_result(result, -1, EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, -1, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_socket(domain, type, protocol, flags,
                                                                   task, result);
    }

    [[nodiscard]] static bool io_submit_openat_direct(Thread thread, int dir_fd, const char *path,
                                                      int flags, std::uint32_t mode, int file_index,
                                                      Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr || file_index < 0) {
            return fail_io_result(result, file_index, file_index < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, file_index, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_openat_direct(dir_fd, path, flags, mode,
                                                                          file_index, task, result);
    }

    [[nodiscard]] static bool io_submit_openat2(Thread thread, int dir_fd, const char *path,
                                                const struct open_how *how, Task *task,
                                                IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr || how == nullptr) {
            return fail_io_result(result, dir_fd, EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, dir_fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_openat2(dir_fd, path, how, task,
                                                                    result);
    }

    [[nodiscard]] static bool io_submit_close(Thread thread, int fd, Task *task,
                                              IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_close(fd, task, result);
    }

    [[nodiscard]] static bool io_submit_shutdown(Thread thread, int fd, int how, Task *task,
                                                 IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_shutdown(fd, how, task, result);
    }

    [[nodiscard]] static bool io_submit_fallocate(Thread thread, int fd, int mode,
                                                  std::uint64_t offset, std::uint64_t length,
                                                  Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_fallocate(fd, mode, offset, length,
                                                                      task, result);
    }

    [[nodiscard]] static bool io_submit_ftruncate(Thread thread, int fd, std::uint64_t length,
                                                  Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_ftruncate(fd, length, task, result);
    }

    [[nodiscard]] static bool io_submit_statx(Thread thread, int dir_fd, const char *path,
                                              int flags, std::uint32_t mask, struct statx *output,
                                              Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr || output == nullptr) {
            return fail_io_result(result, dir_fd, EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, dir_fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_statx(dir_fd, path, flags, mask, output,
                                                                  task, result);
    }

    [[nodiscard]] static bool io_submit_renameat(Thread thread, int old_dir_fd,
                                                 const char *old_path, int new_dir_fd,
                                                 const char *new_path, std::uint32_t flags,
                                                 Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || old_path == nullptr || new_path == nullptr) {
            return fail_io_result(result, old_dir_fd, EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, old_dir_fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_renameat(
            old_dir_fd, old_path, new_dir_fd, new_path, flags, task, result);
    }

    [[nodiscard]] static bool io_submit_unlinkat(Thread thread, int dir_fd, const char *path,
                                                 int flags, Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr) {
            return fail_io_result(result, dir_fd, EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, dir_fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_unlinkat(dir_fd, path, flags, task,
                                                                     result);
    }

    [[nodiscard]] static bool io_submit_mkdirat(Thread thread, int dir_fd, const char *path,
                                                std::uint32_t mode, Task *task,
                                                IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr) {
            return fail_io_result(result, dir_fd, EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, dir_fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_mkdirat(dir_fd, path, mode, task,
                                                                    result);
    }

    [[nodiscard]] static bool io_submit_symlinkat(Thread thread, const char *target, int new_dir_fd,
                                                  const char *link_path, Task *task,
                                                  IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || target == nullptr || link_path == nullptr) {
            return fail_io_result(result, new_dir_fd, EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, new_dir_fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_symlinkat(target, new_dir_fd, link_path,
                                                                      task, result);
    }

    [[nodiscard]] static bool io_submit_linkat(Thread thread, int old_dir_fd, const char *old_path,
                                               int new_dir_fd, const char *new_path,
                                               std::uint32_t flags, Task *task,
                                               IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || old_path == nullptr || new_path == nullptr) {
            return fail_io_result(result, old_dir_fd, EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, old_dir_fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_linkat(old_dir_fd, old_path, new_dir_fd,
                                                                   new_path, flags, task, result);
    }

    [[nodiscard]] static bool io_submit_splice(Thread thread, int in_fd, std::int64_t off_in,
                                               int out_fd, std::int64_t off_out, std::size_t count,
                                               unsigned int flags, Task *task,
                                               IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || in_fd < 0 || out_fd < 0) {
            return fail_io_result(result, out_fd, in_fd < 0 || out_fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, out_fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_splice(in_fd, off_in, out_fd, off_out,
                                                                   count, flags, task, result);
    }

    [[nodiscard]] static bool io_submit_recv(Thread thread, int fd, void *data, std::size_t size,
                                             std::uint32_t flags, Task *task,
                                             IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_recv(fd, data, size, flags, task,
                                                                 result);
    }

    [[nodiscard]] static bool io_submit_recv_fixed_file(Thread thread, int file_index, void *data,
                                                        std::size_t size, std::uint32_t flags,
                                                        Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            return fail_io_result(result, file_index, file_index < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, file_index, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_recv_fixed_file(file_index, data, size,
                                                                            flags, task, result);
    }

    [[nodiscard]] static bool io_submit_send(Thread thread, int fd, const void *data,
                                             std::size_t size, std::uint32_t flags, Task *task,
                                             IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_send(fd, data, size, flags, task,
                                                                 result);
    }

    [[nodiscard]] static bool io_submit_send_fixed_file(Thread thread, int file_index,
                                                        const void *data, std::size_t size,
                                                        std::uint32_t flags, Task *task,
                                                        IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            return fail_io_result(result, file_index, file_index < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, file_index, EINVAL);
        }
        return RuntimeT::executors_[index]->submit_io_uring_send_fixed_file(file_index, data, size,
                                                                            flags, task, result);
    }

    [[nodiscard]] static bool io_submit_accept(Thread thread, int fd, sockaddr *address,
                                               socklen_t *address_size, int flags, Task *task,
                                               IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 ||
            ((address == nullptr) != (address_size == nullptr))) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(thread, fd, result, [&](auto &executor) noexcept {
            return executor.submit_io_uring_accept(fd, address, address_size, flags, task, result);
        });
    }

    [[nodiscard]] static bool io_submit_accept_direct(Thread thread, int fd, sockaddr *address,
                                                      socklen_t *address_size, int flags,
                                                      int file_index, Task *task,
                                                      IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || file_index < 0 ||
            ((address == nullptr) != (address_size == nullptr))) {
            return fail_io_result(result, file_index, fd < 0 || file_index < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(
            thread, file_index, result, [&](auto &executor) noexcept {
                return executor.submit_io_uring_accept_direct(fd, address, address_size, flags,
                                                              file_index, task, result);
            });
    }

    [[nodiscard]] static bool io_submit_accept_multishot(Thread thread, int fd, sockaddr *address,
                                                         socklen_t *address_size, int flags,
                                                         Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || address != nullptr ||
            address_size != nullptr) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(thread, fd, result, [&](auto &executor) noexcept {
            return executor.submit_io_uring_accept_multishot(fd, address, address_size, flags, task,
                                                             result);
        });
    }

    [[nodiscard]] static bool io_submit_connect(Thread thread, int fd, const sockaddr *address,
                                                socklen_t address_size, Task *task,
                                                IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || address == nullptr ||
            address_size == 0U) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(thread, fd, result, [&](auto &executor) noexcept {
            return executor.submit_io_uring_connect(fd, address, address_size, task, result);
        });
    }

    [[nodiscard]] static bool io_submit_recv_multishot(Thread thread, int fd,
                                                       std::uint16_t buffer_group,
                                                       std::uint32_t flags, Task *task,
                                                       IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(thread, fd, result, [&](auto &executor) noexcept {
            return executor.submit_io_uring_recv_multishot(fd, buffer_group, flags, task, result);
        });
    }

    [[nodiscard]] static bool
    io_submit_recvmsg_multishot(Thread thread, int fd, std::uint16_t buffer_group,
                                socklen_t name_capacity, std::size_t control_capacity,
                                std::uint32_t flags, Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(thread, fd, result, [&](auto &executor) noexcept {
            return executor.submit_io_uring_recvmsg_multishot(
                fd, buffer_group, name_capacity, control_capacity, flags, task, result);
        });
    }

    [[nodiscard]] static bool io_submit_send_zc(Thread thread, int fd, const void *data,
                                                std::size_t size, std::uint32_t flags, Task *task,
                                                IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(thread, fd, result, [&](auto &executor) noexcept {
            return executor.submit_io_uring_send_zc(fd, data, size, flags, task, result);
        });
    }

    [[nodiscard]] static bool io_submit_sendmsg_zc(Thread thread, int fd, const void *data,
                                                   std::size_t size, const sockaddr *address,
                                                   socklen_t address_size, std::uint32_t flags,
                                                   Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(thread, fd, result, [&](auto &executor) noexcept {
            return executor.submit_io_uring_sendmsg_zc(fd, data, size, address, address_size, flags,
                                                       task, result);
        });
    }

    [[nodiscard]] static bool io_submit_sendmsg_zc_iov(Thread thread, int fd, const iovec *iov,
                                                       int iov_count, const sockaddr *address,
                                                       socklen_t address_size, std::uint32_t flags,
                                                       Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || iov == nullptr || iov_count <= 0) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(thread, fd, result, [&](auto &executor) noexcept {
            return executor.submit_io_uring_sendmsg_zc_iov(fd, iov, iov_count, address,
                                                           address_size, flags, task, result);
        });
    }

    [[nodiscard]] static bool io_submit_recvmsg_fixed_file_iov(Thread thread, int file_index,
                                                               const iovec *iov, int iov_count,
                                                               std::uint32_t flags, Task *task,
                                                               IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || iov == nullptr ||
            iov_count <= 0) {
            return fail_io_result(result, file_index, file_index < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(
            thread, file_index, result, [&](auto &executor) noexcept {
                return executor.submit_io_uring_recvmsg_fixed_file_iov(file_index, iov, iov_count,
                                                                       flags, task, result);
            });
    }

    [[nodiscard]] static bool io_submit_recvmsg_iov(Thread thread, int fd, const iovec *iov,
                                                    int iov_count, sockaddr *address,
                                                    socklen_t *address_size, std::uint32_t flags,
                                                    Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || iov == nullptr || iov_count <= 0) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(thread, fd, result, [&](auto &executor) noexcept {
            return executor.submit_io_uring_recvmsg_iov(fd, iov, iov_count, address, address_size,
                                                        flags, task, result);
        });
    }

    [[nodiscard]] static bool io_submit_recvmsg(Thread thread, int fd, void *data, std::size_t size,
                                                sockaddr *address, socklen_t *address_size,
                                                std::uint32_t flags, Task *task,
                                                IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(thread, fd, result, [&](auto &executor) noexcept {
            return executor.submit_io_uring_recvmsg(fd, data, size, address, address_size, flags,
                                                    task, result);
        });
    }

    [[nodiscard]] static bool io_submit_sendmsg_fixed_file_iov(Thread thread, int file_index,
                                                               const iovec *iov, int iov_count,
                                                               std::uint32_t flags, Task *task,
                                                               IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || iov == nullptr ||
            iov_count <= 0) {
            return fail_io_result(result, file_index, file_index < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(
            thread, file_index, result, [&](auto &executor) noexcept {
                return executor.submit_io_uring_sendmsg_fixed_file_iov(file_index, iov, iov_count,
                                                                       flags, task, result);
            });
    }

    [[nodiscard]] static bool io_submit_sendmsg_iov(Thread thread, int fd, const iovec *iov,
                                                    int iov_count, const sockaddr *address,
                                                    socklen_t address_size, std::uint32_t flags,
                                                    Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || iov == nullptr || iov_count <= 0) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(thread, fd, result, [&](auto &executor) noexcept {
            return executor.submit_io_uring_sendmsg_iov(fd, iov, iov_count, address, address_size,
                                                        flags, task, result);
        });
    }

    [[nodiscard]] static bool io_submit_sendmsg(Thread thread, int fd, const void *data,
                                                std::size_t size, const sockaddr *address,
                                                socklen_t address_size, std::uint32_t flags,
                                                Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        return submit_linux_only_to_executor(thread, fd, result, [&](auto &executor) noexcept {
            return executor.submit_io_uring_sendmsg(fd, data, size, address, address_size, flags,
                                                    task, result);
        });
    }
};

} // namespace af::detail
