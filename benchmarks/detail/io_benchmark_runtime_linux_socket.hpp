#pragma once

struct FakeRuntimeLinuxSocketOps {
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
};
