#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_sendfile_static_example {

struct SendfileLogicThreadTag;
struct SendfileIoThreadTag;

struct SendfileRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<SendfileLogicThreadTag, 1, af::ThreadKind::Worker, "sendfile-cpu">(),
        af::thread_group<SendfileIoThreadTag, 1, af::native_io_thread_kind, "sendfile-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using sendfile_async = af::AsyncRuntime<SendfileRuntimeTraits>;
using SendfileTaskBase = sendfile_async::Task;
using SendfileThread = sendfile_async::Thread;

struct SendfileThreads {
    static constexpr SendfileThread Logic_0 =
        sendfile_async::thread_group<SendfileLogicThreadTag>().template at<0>();
    static constexpr SendfileThread IO_0 =
        sendfile_async::thread_group<SendfileIoThreadTag>().template at<0>();
};

inline constexpr char sendfile_payload[] = "HTTP/1.1 200 OK\r\n"
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
