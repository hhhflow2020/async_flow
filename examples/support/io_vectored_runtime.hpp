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

struct VectoredIoThreadTag;

struct VectoredRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<VectoredIoThreadTag, 1, af::ThreadKind::IoUring, "vectored-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using vectored_async = af::AsyncRuntime<VectoredRuntimeTraits>;
using VectoredTask = vectored_async::Task;
using VectoredThread = vectored_async::Thread;

struct VectoredThreads {
    static constexpr VectoredThread IO_0 =
        vectored_async::thread_group<VectoredIoThreadTag>().template at<0>();
};

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
