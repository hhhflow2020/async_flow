#include <cstddef>
#include <cstdint>
#include <iostream>

#include "af/async_flow.hpp"

namespace {

struct TunedSetupIoThreadTag;

struct TunedSetupRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<TunedSetupIoThreadTag, 1, af::ThreadKind::IoUring, "tuned-io">());
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
};

struct FallbackSetupRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<TunedSetupIoThreadTag, 1, af::ThreadKind::IoUring, "fallback-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr unsigned io_uring_entries = 1024;
    static constexpr unsigned io_uring_cq_entries = 2048;
    static constexpr unsigned io_uring_submit_batch_threshold = 256;
};

} // namespace

int main() {
    if constexpr (!af::supports_io_uring) {
        std::cout << "io_uring tuned setup example is Linux-only\n";
        return 0;
    }

    using tuned_async = af::AsyncRuntime<TunedSetupRuntimeTraits>;
    constexpr auto tuned_io = tuned_async::thread_group<TunedSetupIoThreadTag>().template at<0>();

    tuned_async::init();
    if (tuned_async::io_uring_backend_available(tuned_io)) {
        std::cout << "io_uring tuned setup backend available\n";
        tuned_async::shutdown();
        return 0;
    }
    tuned_async::shutdown();

    std::cout << "io_uring tuned setup backend unavailable; retrying with fallback traits\n";
    using fallback_async = af::AsyncRuntime<FallbackSetupRuntimeTraits>;
    constexpr auto fallback_io =
        fallback_async::thread_group<TunedSetupIoThreadTag>().template at<0>();
    fallback_async::init();
    if (!fallback_async::io_uring_backend_available(fallback_io)) {
        std::cout << "io_uring backend unavailable\n";
        fallback_async::shutdown();
        return 0;
    }

    std::cout << "io_uring fallback backend available\n";
    fallback_async::shutdown();
    return 0;
}
