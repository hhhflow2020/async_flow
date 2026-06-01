#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_pollable_client_example {

struct ClientIoThreadTag;

struct ClientRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<ClientIoThreadTag, 1, af::ThreadKind::Epoll, "client-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using client_async = af::AsyncRuntime<ClientRuntimeTraits>;
using PollableTaskBase = client_async::Task;
using ClientThread = client_async::Thread;

struct ClientThreads {
    static constexpr ClientThread IO_0 =
        client_async::thread_group<ClientIoThreadTag>().template at<0>();
};

} // namespace io_pollable_client_example
