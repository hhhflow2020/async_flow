#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace io_uring_udp_recv_multishot_example {

enum class UdpRecvThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct UdpRecvRuntimeTraits {
    using Thread = UdpRecvThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(UdpRecvThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(UdpRecvThread thread) noexcept {
        return thread == UdpRecvThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using udp_recv_async = af::AsyncRuntime<UdpRecvRuntimeTraits>;
using UdpRecvTaskBase = udp_recv_async::Task;

inline bool wait_until_armed_or_error(std::atomic<int>& armed, std::atomic<int>& error) {
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

} // namespace io_uring_udp_recv_multishot_example
