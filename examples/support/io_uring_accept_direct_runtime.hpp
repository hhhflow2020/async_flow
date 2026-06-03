#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace io_uring_accept_direct_example {

struct DirectAcceptLogicThreadTag;
struct DirectAcceptIoThreadTag;

struct DirectAcceptRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<DirectAcceptLogicThreadTag, 1, af::ThreadKind::Worker, "accept-cpu">(),
        af::thread_group<DirectAcceptIoThreadTag, 1, af::preferred_io_thread_kind, "accept-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using direct_accept_async = af::AsyncRuntime<DirectAcceptRuntimeTraits>;
using DirectAcceptTask = direct_accept_async::Task;
using DirectAcceptThread = direct_accept_async::Thread;

struct DirectAcceptThreads {
    static constexpr DirectAcceptThread Logic_0 =
        direct_accept_async::thread_group<DirectAcceptLogicThreadTag>().template at<0>();
    static constexpr DirectAcceptThread IO_0 =
        direct_accept_async::thread_group<DirectAcceptIoThreadTag>().template at<0>();
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

} // namespace io_uring_accept_direct_example
