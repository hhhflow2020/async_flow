#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <fcntl.h>
#include <thread>
#include <unistd.h>

#include <gtest/gtest.h>

#include "af/runtime.hpp"

namespace {

struct SelectReadinessState {
    int read_fd{-1};
    int write_fd{-1};
    af::fd_event_source source;
    af::runtime *runtime{nullptr};
    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> events{0};
    std::atomic<int> error{0};
    std::atomic<std::uint16_t> callback_thread{af::runtime_invalid_thread_index};
};

void close_fd(int &fd) noexcept {
    if (fd >= 0) {
        static_cast<void>(::close(fd));
        fd = -1;
    }
}

void set_fd_nonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    ASSERT_GE(flags, 0);
    ASSERT_EQ(::fcntl(fd, F_SETFL, flags | O_NONBLOCK), 0);
}

[[nodiscard]] bool wait_until_at_least(const std::atomic<int> &value, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

class SelectReadinessArmTask final : public af::runtime_task {
public:
    SelectReadinessArmTask(factory_token token, af::runtime &owner, SelectReadinessState &state)
        : runtime_task(token, owner), state_(state) {}

    [[nodiscard]] bool do_it(af::thread_ref thread) noexcept {
        return schedule_to(thread);
    }

private:
    static void on_event(void *owner, af::fd_event_source &source, std::uint32_t events) noexcept {
        auto &state = *static_cast<SelectReadinessState *>(owner);
        std::array<char, 16> buffer{};
        const ssize_t n = ::read(state.read_fd, buffer.data(), buffer.size());
        if (state.runtime != nullptr) {
            static_cast<void>(state.runtime->unregister_reactor_source(
                af::runtime::current_thread_index(), &source));
        }

        state.callback_thread.store(af::runtime::current_thread_index(), std::memory_order_release);
        state.events.store(static_cast<int>(events), std::memory_order_release);
        if (n != 1 || buffer[0] != 'x') {
            state.error.store(errno == 0 ? EIO : errno, std::memory_order_release);
        }
        state.completed.fetch_add(1, std::memory_order_release);
    }

    af::task_result run_task() noexcept override {
        if (af::runtime::current_reactor() == nullptr) {
            state_.error.store(ENOSYS, std::memory_order_release);
            return failed();
        }

        state_.runtime = &owner();
        state_.source.fd = state_.read_fd;
        state_.source.interests = af::reactor_readable;
        state_.source.owner = &state_;
        state_.source.on_event = &SelectReadinessArmTask::on_event;
        if (!owner().register_reactor_source(af::runtime::current_thread_index(), &state_.source)) {
            state_.error.store(errno == 0 ? EIO : errno, std::memory_order_release);
            return failed();
        }

        state_.armed.fetch_add(1, std::memory_order_release);
        return done();
    }

    SelectReadinessState &state_;
};

} // namespace

TEST(SelectBackendTests, AutoBackendFallsBackToSelectAndDispatchesReadiness) {
    static_assert(af::detail::supports_select);
    static_assert(!af::detail::supports_epoll);
    static_assert(!af::detail::supports_kqueue);

    int pipe_fds[2]{-1, -1};
    ASSERT_EQ(::pipe(pipe_fds), 0);
    SelectReadinessState state;
    state.read_fd = pipe_fds[0];
    state.write_fd = pipe_fds[1];
    set_fd_nonblocking(state.read_fd);
    set_fd_nonblocking(state.write_fd);

    af::runtime_config config;
    config.threads = {af::io_threads("select-io", 1)};
    config.reactor.backend = af::reactor_backend::auto_select;
    config.logger.consumer_thread = af::thread_selector::any_io();
    af::runtime runtime(config);
    ASSERT_TRUE(runtime.start());

    const af::thread_group_ref io_threads = runtime.io_threads();
    ASSERT_EQ(io_threads.size(), 1U);
    auto task = af::make_task<SelectReadinessArmTask>(runtime, state);
    ASSERT_TRUE(task->do_it(io_threads.front()));
    task.reset();
    ASSERT_TRUE(wait_until_at_least(state.armed, 1));

    const char byte = 'x';
    ASSERT_EQ(::write(state.write_fd, &byte, sizeof(byte)), 1);
    ASSERT_TRUE(wait_until_at_least(state.completed, 1));
    runtime.stop();

    EXPECT_EQ(state.error.load(std::memory_order_acquire), 0);
    EXPECT_NE(static_cast<std::uint32_t>(state.events.load(std::memory_order_acquire)) &
                  af::reactor_readable,
              0U);
    EXPECT_EQ(state.callback_thread.load(std::memory_order_acquire), io_threads.front().index);

    close_fd(state.read_fd);
    close_fd(state.write_fd);
}
