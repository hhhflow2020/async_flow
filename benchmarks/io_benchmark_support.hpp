#pragma once

#include <cstddef>
#include <cstdint>
#include <chrono>

#include <benchmark/benchmark.h>

#include "af/io.hpp"

namespace {

enum class BenchIoThread : std::int16_t {
    IO_0,
};

struct FakeRuntime {
    static constexpr std::uint16_t invalid_thread_index = 1;

    static bool is_runtime_thread() noexcept {
        return false;
    }

    static std::uint16_t current_thread_index() noexcept {
        return invalid_thread_index;
    }

    static constexpr std::uint16_t thread_index(BenchIoThread thread) noexcept {
        return static_cast<std::uint16_t>(thread);
    }

    static bool io_uring_backend_available(BenchIoThread) noexcept {
        return false;
    }

    static bool io_backend_available(BenchIoThread) noexcept {
        return false;
    }

    static bool io_submit_socket(
        BenchIoThread,
        int,
        int,
        int,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_wait(
        BenchIoThread,
        int,
        std::uint32_t,
        void*,
        af::IoResult*,
        bool = false) noexcept {
        return false;
    }

    static bool io_submit_recv(
        BenchIoThread,
        int,
        void*,
        std::size_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_timeout(
        BenchIoThread,
        std::chrono::nanoseconds,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_send(
        BenchIoThread,
        int,
        const void*,
        std::size_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_recv_fixed_file(
        BenchIoThread,
        int,
        void*,
        std::size_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_send_fixed_file(
        BenchIoThread,
        int,
        const void*,
        std::size_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_read_at(
        BenchIoThread,
        int,
        void*,
        std::size_t,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_write_at(
        BenchIoThread,
        int,
        const void*,
        std::size_t,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

#define AF_IO_BENCHMARK_RUNTIME_DETAIL_INCLUDE 1
#include "detail/io_benchmark_runtime_linux_socket.hpp"
#include "detail/io_benchmark_runtime_posix_message.hpp"
#include "detail/io_benchmark_runtime_posix_file.hpp"
#include "detail/io_benchmark_runtime_posix_accept.hpp"
#include "detail/io_benchmark_runtime_filesystem.hpp"
#undef AF_IO_BENCHMARK_RUNTIME_DETAIL_INCLUDE
};

struct FakeTask {
    using Runtime = FakeRuntime;
    using Thread = BenchIoThread;
};

} // namespace
