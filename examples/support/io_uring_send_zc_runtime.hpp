#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_uring_send_zc_example {

struct SendZcIoThreadTag;

struct SendZcRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<SendZcIoThreadTag, 1, af::preferred_io_thread_kind, "send-zc">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using send_zc_async = af::AsyncRuntime<SendZcRuntimeTraits>;
using SendZcTaskBase = send_zc_async::Task;
using SendZcThread = send_zc_async::Thread;

struct SendZcThreads {
    static constexpr SendZcThread IO_0 =
        send_zc_async::thread_group<SendZcIoThreadTag>().template at<0>();
};

inline constexpr char send_zc_payload[] =
    "asyncflow io_uring send_zc keeps socket writes on the IO thread\n";
inline constexpr std::size_t send_zc_payload_size = sizeof(send_zc_payload) - 1U;
using SendZcPayloadBuffer = std::array<char, send_zc_payload_size>;

struct SendZcServerResult {
    bool ok{false};
    int error{0};
    std::size_t bytes_sent{0};
};

struct SendZcClientResult {
    bool ok{false};
    int error{0};
    bool payload_match{false};
    std::size_t bytes_read{0};
    SendZcPayloadBuffer received{};
};

} // namespace io_uring_send_zc_example
