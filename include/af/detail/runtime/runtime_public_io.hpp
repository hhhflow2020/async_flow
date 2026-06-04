#pragma once

#include <cerrno>
#include <chrono>
#include <cstdint>

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

public:
    [[nodiscard]] static bool io_backend_available(Thread thread) noexcept {
        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return false;
        }
        return RuntimeT::executors_[index]->io_backend_available();
    }

    [[nodiscard]] static bool io_wait(Thread thread, int fd, std::uint32_t events, Task *task,
                                      IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || events == 0U) {
            return fail_io_result(result, fd, fd < 0 ? EBADF : EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, fd, EINVAL);
        }
        return RuntimeT::executors_[index]->register_io_wait(fd, events, task, result);
    }

    [[nodiscard]] static bool cancel_io(Thread thread, IoOpState &state) noexcept {
        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_state(state, -1, EINVAL);
        }
        return RuntimeT::executors_[index]->cancel_io(state);
    }

    [[nodiscard]] static bool io_timer_wait(Thread thread, std::chrono::nanoseconds timeout,
                                            Task *task, IoResult *result) noexcept {
        if (task == nullptr || result == nullptr || timeout.count() <= 0) {
            return fail_io_result(result, -1, EINVAL);
        }

        const std::uint16_t index = RuntimeT::thread_index(thread);
        if (index >= RuntimeT::executors_.size()) {
            return fail_io_result(result, -1, EINVAL);
        }
        return RuntimeT::executors_[index]->register_timer_wait(timeout, task, result);
    }
};

} // namespace af::detail
