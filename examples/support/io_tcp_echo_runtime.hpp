#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_tcp_echo_example {

struct EchoIoThreadTag;
struct EchoComputeThreadTag;

struct EchoRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<EchoIoThreadTag, 2, af::preferred_io_thread_kind, "echo-io">(),
        af::thread_group<EchoComputeThreadTag, 1, af::ThreadKind::Worker, "echo-cpu">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
};

using echo_async = af::AsyncRuntime<EchoRuntimeTraits>;
using EchoTask = echo_async::Task;
using EchoThread = echo_async::Thread;

struct EchoThreads {
    static constexpr EchoThread IO_0 = echo_async::thread_group<EchoIoThreadTag>().template at<0>();
    static constexpr EchoThread IO_1 = echo_async::thread_group<EchoIoThreadTag>().template at<1>();
    static constexpr EchoThread Compute_0 =
        echo_async::thread_group<EchoComputeThreadTag>().template at<0>();
};

inline constexpr std::size_t echo_session_buffer_size = 4096;
inline constexpr std::size_t echo_payload_size = 8;
inline constexpr std::size_t echo_self_test_client_count = 2;
using EchoPayload = std::array<char, echo_payload_size>;

struct EchoClientResult {
    std::atomic<bool> completed{false};
    bool ok{false};
    int error{0};
    EchoThread io_thread{EchoThreads::IO_0};
    EchoPayload response{};
    std::size_t received{0};
};

[[nodiscard]] inline EchoThread echo_io_thread(std::size_t index) noexcept {
    return echo_async::thread_group<EchoIoThreadTag>().at(static_cast<std::uint16_t>(index & 1U));
}

[[nodiscard]] inline const char *echo_backend_name(EchoThread thread) noexcept {
    return af::runtime_io_backend_name<echo_async>(thread);
}

[[nodiscard]] inline std::size_t echo_lowercase_ascii(char *data, std::size_t size) noexcept {
    if (data == nullptr) {
        return 0;
    }

    std::size_t converted = 0;
    for (std::size_t index = 0; index < size; ++index) {
        char &ch = data[index];
        if (ch >= 'A' && ch <= 'Z') {
            ch = static_cast<char>(ch - 'A' + 'a');
            ++converted;
        }
    }
    return converted;
}

} // namespace io_tcp_echo_example
