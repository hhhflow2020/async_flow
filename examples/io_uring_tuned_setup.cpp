#include <cstddef>
#include <cstdint>
#include <iostream>
 
#include "af/async_flow.hpp"
 
namespace {
 
enum class TunedSetupThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};
 
struct TunedSetupRuntimeTraits {
    using Thread = TunedSetupThread;
 
    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(TunedSetupThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
 
    static constexpr unsigned io_uring_entries = 1024;
    static constexpr unsigned io_uring_cq_entries = 2048;
    static constexpr unsigned io_uring_submit_batch_threshold = 256;
 
    // These flags are optional and kernel-dependent. If the kernel rejects them,
    // the io_uring backend will be unavailable and the example will print a hint.
    static constexpr bool io_uring_setup_coop_taskrun = true;
    static constexpr bool io_uring_setup_single_issuer = true;
    static constexpr bool io_uring_setup_defer_taskrun = true;
 
    static constexpr af::ThreadKind thread_kind(TunedSetupThread thread) noexcept {
        return thread == TunedSetupThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};
 
struct FallbackSetupRuntimeTraits {
    using Thread = TunedSetupThread;
 
    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(TunedSetupThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
 
    static constexpr unsigned io_uring_entries = 1024;
    static constexpr unsigned io_uring_cq_entries = 2048;
    static constexpr unsigned io_uring_submit_batch_threshold = 256;
 
    static constexpr af::ThreadKind thread_kind(TunedSetupThread thread) noexcept {
        return thread == TunedSetupThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};
 
} // namespace
 
int main() {
#if defined(__linux__)
    using tuned_async = af::AsyncRuntime<TunedSetupRuntimeTraits>;
 
    tuned_async::init();
    if (tuned_async::io_uring_backend_available(TunedSetupThread::IO_0)) {
        std::cout << "io_uring tuned setup backend available\n";
        tuned_async::shutdown();
        return 0;
    }
    tuned_async::shutdown();
 
    std::cout << "io_uring tuned setup backend unavailable; retrying with fallback traits\n";
    using fallback_async = af::AsyncRuntime<FallbackSetupRuntimeTraits>;
    fallback_async::init();
    if (!fallback_async::io_uring_backend_available(TunedSetupThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        fallback_async::shutdown();
        return 0;
    }
 
    std::cout << "io_uring fallback backend available\n";
    fallback_async::shutdown();
    return 0;
#else
    std::cout << "io_uring tuned setup example is Linux-only\n";
    return 0;
#endif
}
