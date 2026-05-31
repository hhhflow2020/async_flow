#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_sendfile_static_example {

enum class SendfileThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct SendfileRuntimeTraits {
    using Thread = SendfileThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(SendfileThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(SendfileThread thread) noexcept {
        return thread == SendfileThread::IO_0 ? af::ThreadKind::Epoll : af::ThreadKind::Worker;
    }
};

using sendfile_async = af::AsyncRuntime<SendfileRuntimeTraits>;
using SendfileTaskBase = sendfile_async::Task;

inline constexpr char sendfile_payload[] =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 20\r\n"
    "\r\n"
    "asyncflow sendfile\n";
inline constexpr std::size_t sendfile_payload_size = sizeof(sendfile_payload) - 1U;
using SendfilePayloadBuffer = std::array<char, sendfile_payload_size>;

struct SendfileServerResult {
    bool ok{false};
    int error{0};
    std::size_t bytes_sent{0};
};

struct SendfileClientResult {
    bool ok{false};
    int error{0};
    bool payload_match{false};
    std::size_t bytes_read{0};
    SendfilePayloadBuffer received{};
};

} // namespace io_sendfile_static_example
