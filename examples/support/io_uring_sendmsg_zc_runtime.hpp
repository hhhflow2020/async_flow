#pragma once

#include <cstddef>

#include "af/async_flow.hpp"

namespace io_uring_sendmsg_zc_example {

struct SendmsgZcIoThreadTag;

struct SendmsgZcRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<SendmsgZcIoThreadTag, 1, af::preferred_io_thread_kind, "sendmsg-zc">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using sendmsg_zc_async = af::AsyncRuntime<SendmsgZcRuntimeTraits>;
using SendmsgZcTaskBase = sendmsg_zc_async::Task;
using SendmsgZcThread = sendmsg_zc_async::Thread;

struct SendmsgZcThreads {
    static constexpr SendmsgZcThread IO_0 =
        sendmsg_zc_async::thread_group<SendmsgZcIoThreadTag>().template at<0>();
};

inline constexpr char sendmsg_zc_first[] = "asyncflow sendmsg_zc ";
inline constexpr char sendmsg_zc_second[] = "uses vectored zero-copy when the kernel supports it\n";
inline constexpr std::size_t sendmsg_zc_first_size = sizeof(sendmsg_zc_first) - 1U;
inline constexpr std::size_t sendmsg_zc_second_size = sizeof(sendmsg_zc_second) - 1U;
inline constexpr std::size_t sendmsg_zc_payload_size =
    sendmsg_zc_first_size + sendmsg_zc_second_size;

} // namespace io_uring_sendmsg_zc_example
