#pragma once

#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_pollable_client_example {

enum class ClientThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct ClientRuntimeTraits {
    using Thread = ClientThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(ClientThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(ClientThread thread) noexcept {
        return thread == ClientThread::IO_0 ? af::ThreadKind::Epoll : af::ThreadKind::Worker;
    }
};

using client_async = af::AsyncRuntime<ClientRuntimeTraits>;
using PollableTaskBase = client_async::Task;

} // namespace io_pollable_client_example
