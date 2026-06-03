#pragma once

#include <cstddef>

#include "af/async_flow.hpp"

namespace io_adapters_example {

struct AdapterIoThreadTag;

struct AdapterRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<AdapterIoThreadTag, 1, af::preferred_io_thread_kind, "adapter-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using async = af::AsyncRuntime<AdapterRuntimeTraits>;
using Task = async::Task;
using AppThread = async::Thread;

struct AppThreads {
    static constexpr AppThread IO_0 = async::thread_group<AdapterIoThreadTag>().template at<0>();
};

} // namespace io_adapters_example
