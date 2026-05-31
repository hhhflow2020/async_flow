#if !defined(AF_IO_BENCHMARK_RUNTIME_FRAGMENT_INCLUDE)
#error "io_benchmark_runtime_posix_accept_fragment.hpp is a FakeRuntime implementation fragment"
#endif

#if !defined(_WIN32)
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
