#if !defined(AF_IO_BENCHMARK_RUNTIME_DETAIL_INCLUDE)
#error "io_benchmark_runtime_posix_message.hpp is a FakeRuntime implementation detail"
#endif

struct FakeRuntimePosixMessageOps {
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
#endif
};
