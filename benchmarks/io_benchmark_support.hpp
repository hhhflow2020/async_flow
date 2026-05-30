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

#if defined(__linux__)
    static bool io_submit_send_zc(
        BenchIoThread,
        int,
        const void*,
        std::size_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_sendmsg_zc(
        BenchIoThread,
        int,
        const void*,
        std::size_t,
        const sockaddr*,
        socklen_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_sendmsg_zc_iov(
        BenchIoThread,
        int,
        const iovec*,
        int,
        const sockaddr*,
        socklen_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_recv_multishot(
        BenchIoThread,
        int,
        std::uint16_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_recvmsg_multishot(
        BenchIoThread,
        int,
        std::uint16_t,
        socklen_t,
        std::size_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }
#endif

#if !defined(_WIN32)
    static bool io_submit_recvmsg(
        BenchIoThread,
        int,
        void*,
        std::size_t,
        sockaddr*,
        socklen_t*,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_sendmsg(
        BenchIoThread,
        int,
        const void*,
        std::size_t,
        const sockaddr*,
        socklen_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_recvmsg_iov(
        BenchIoThread,
        int,
        const iovec*,
        int,
        sockaddr*,
        socklen_t*,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_recvmsg_fixed_file_iov(
        BenchIoThread,
        int,
        const iovec*,
        int,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_sendmsg_iov(
        BenchIoThread,
        int,
        const iovec*,
        int,
        const sockaddr*,
        socklen_t,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_sendmsg_fixed_file_iov(
        BenchIoThread,
        int,
        const iovec*,
        int,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_read_fixed_at(
        BenchIoThread,
        int,
        void*,
        std::size_t,
        std::uint64_t,
        std::uint16_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_write_fixed_at(
        BenchIoThread,
        int,
        const void*,
        std::size_t,
        std::uint64_t,
        std::uint16_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_read_fixed_file_at(
        BenchIoThread,
        int,
        void*,
        std::size_t,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_write_fixed_file_at(
        BenchIoThread,
        int,
        const void*,
        std::size_t,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_readv_fixed_file_at(
        BenchIoThread,
        int,
        const iovec*,
        int,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_writev_fixed_file_at(
        BenchIoThread,
        int,
        const iovec*,
        int,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_read_fixed_file_at(
        BenchIoThread,
        int,
        void*,
        std::size_t,
        std::uint64_t,
        std::uint16_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_write_fixed_file_at(
        BenchIoThread,
        int,
        const void*,
        std::size_t,
        std::uint64_t,
        std::uint16_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_fsync_fixed_file(
        BenchIoThread,
        int,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_readv_at(
        BenchIoThread,
        int,
        const iovec*,
        int,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_writev_at(
        BenchIoThread,
        int,
        const iovec*,
        int,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_accept(
        BenchIoThread,
        int,
        sockaddr*,
        socklen_t*,
        int,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_accept_direct(
        BenchIoThread,
        int,
        sockaddr*,
        socklen_t*,
        int,
        int,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_accept_multishot(
        BenchIoThread,
        int,
        sockaddr*,
        socklen_t*,
        int,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_connect(
        BenchIoThread,
        int,
        const sockaddr*,
        socklen_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }
#endif

    static bool io_submit_openat(
        BenchIoThread,
        int,
        const char*,
        int,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_openat_direct(
        BenchIoThread,
        int,
        const char*,
        int,
        std::uint32_t,
        int,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_openat2(
        BenchIoThread,
        int,
        const char*,
        const struct open_how*,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_close(
        BenchIoThread,
        int,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_shutdown(
        BenchIoThread,
        int,
        int,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_statx(
        BenchIoThread,
        int,
        const char*,
        int,
        std::uint32_t,
        struct statx*,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_fallocate(
        BenchIoThread,
        int,
        int,
        std::uint64_t,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_renameat(
        BenchIoThread,
        int,
        const char*,
        int,
        const char*,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_unlinkat(
        BenchIoThread,
        int,
        const char*,
        int,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_mkdirat(
        BenchIoThread,
        int,
        const char*,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_symlinkat(
        BenchIoThread,
        const char*,
        int,
        const char*,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_linkat(
        BenchIoThread,
        int,
        const char*,
        int,
        const char*,
        std::uint32_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_ftruncate(
        BenchIoThread,
        int,
        std::uint64_t,
        void*,
        af::IoResult*) noexcept {
        return false;
    }

    static bool io_submit_splice(
        BenchIoThread,
        int,
        std::int64_t,
        int,
        std::int64_t,
        std::size_t,
        unsigned int,
        void*,
        af::IoResult*) noexcept {
        return false;
    }
};

struct FakeTask {
    using Runtime = FakeRuntime;
    using Thread = BenchIoThread;
};

} // namespace
