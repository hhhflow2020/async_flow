#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace io_vectored_example {

enum class VectoredThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct VectoredRuntimeTraits {
    using Thread = VectoredThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(VectoredThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(VectoredThread thread) noexcept {
        return thread == VectoredThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using vectored_async = af::AsyncRuntime<VectoredRuntimeTraits>;
using VectoredTask = vectored_async::Task;

inline bool wait_until(std::atomic<int> &value, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

} // namespace io_vectored_example
