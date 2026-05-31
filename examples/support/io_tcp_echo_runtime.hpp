#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "af/async_flow.hpp"

namespace io_tcp_echo_example {

enum class EchoThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    IO_1,
    Compute_0,
    enum_thread_index_end,
};

struct EchoRuntimeTraits {
    using Thread = EchoThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(EchoThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(EchoThread thread) noexcept {
        switch (thread) {
        case EchoThread::IO_0:
        case EchoThread::IO_1:
#if defined(__linux__)
            return af::ThreadKind::IoUring;
#else
            return af::ThreadKind::Io;
#endif
        case EchoThread::Compute_0:
            return af::ThreadKind::Worker;
        case EchoThread::enum_thread_index_start:
        case EchoThread::enum_thread_index_end:
            break;
        }
        return af::ThreadKind::Worker;
    }
};

using echo_async = af::AsyncRuntime<EchoRuntimeTraits>;
using EchoTask = echo_async::Task;

inline constexpr std::size_t echo_payload_size = 8;
inline constexpr std::size_t echo_client_count = 2;
using EchoPayload = std::array<char, echo_payload_size>;

struct EchoSessionResult {
    bool ok{false};
    int error{0};
    EchoThread io_thread{EchoThread::IO_0};
};

struct EchoClientResult {
    bool ok{false};
    int error{0};
    EchoThread io_thread{EchoThread::IO_0};
    EchoPayload response{};
    std::size_t received{0};
};

[[nodiscard]] inline EchoThread echo_io_thread(std::size_t index) noexcept {
    return (index & 1U) == 0U ? EchoThread::IO_0 : EchoThread::IO_1;
}

[[nodiscard]] inline const char* echo_backend_name(EchoThread thread) noexcept {
    static_cast<void>(thread);
#if defined(__linux__)
    return echo_async::io_uring_backend_available(thread) ? "io_uring" : "epoll-fallback";
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    return "kqueue";
#else
    return "native-readiness";
#endif
}

inline void lowercase_ascii(EchoPayload& payload) noexcept {
    for (char& ch : payload) {
        if (ch >= 'A' && ch <= 'Z') {
            ch = static_cast<char>(ch - 'A' + 'a');
        }
    }
}

} // namespace io_tcp_echo_example
