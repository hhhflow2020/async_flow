#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_uring_send_zc_example {

enum class SendZcThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct SendZcRuntimeTraits {
    using Thread = SendZcThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(SendZcThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(SendZcThread thread) noexcept {
        return thread == SendZcThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using send_zc_async = af::AsyncRuntime<SendZcRuntimeTraits>;
using SendZcTaskBase = send_zc_async::Task;

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
