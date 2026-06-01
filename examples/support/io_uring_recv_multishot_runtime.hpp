#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace io_uring_recv_multishot_example {

struct RecvIoThreadTag;

struct RecvRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<RecvIoThreadTag, 1, af::ThreadKind::IoUring, "recv-shot">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using recv_async = af::AsyncRuntime<RecvRuntimeTraits>;
using RecvTaskBase = recv_async::Task;
using RecvThread = recv_async::Thread;

struct RecvThreads {
    static constexpr RecvThread IO_0 = recv_async::thread_group<RecvIoThreadTag>().template at<0>();
};

inline bool wait_until_armed_or_error(std::atomic<int> &armed, std::atomic<int> &error) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (armed.load(std::memory_order_acquire) == 0 &&
           error.load(std::memory_order_acquire) == 0) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

} // namespace io_uring_recv_multishot_example
