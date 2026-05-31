#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "af/detail/bounded_queues.hpp"
#include "af/detail/config.hpp"
#include "af/detail/io_uring_support.hpp"
#include "af/detail/object_pool.hpp"
#include "af/detail/runtime_ready_source_set.hpp"
#include "af/detail/runtime_traits.hpp"
#include "af/task.hpp"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"

#if !defined(_WIN32)
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#endif

#if AF_DETAIL_HAS_EPOLL
#include <algorithm>
#include <linux/openat2.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <unistd.h>
#endif

#if AF_DETAIL_HAS_KQUEUE
#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>
#endif

#if !defined(__linux__)
struct open_how;
struct statx;
#endif

namespace af {

enum class ParallelMode : std::uint8_t {
    NonEmptyOnly,
    AllShards,
};

enum class OrderedBatchReplayPolicy : std::uint8_t {
    Strict,
    SkipAlreadyApplied,
};

struct OrderedBatchOptions {
    OrderedBatchReplayPolicy replay_policy{OrderedBatchReplayPolicy::Strict};
};

inline constexpr OrderedBatchOptions retryable_ordered_batch_options{
    OrderedBatchReplayPolicy::SkipAlreadyApplied};

template <typename Op>
struct ShardedOps {
    std::vector<std::vector<Op>> shards;

    explicit ShardedOps(std::uint16_t shard_count = 0) : shards(shard_count) {}

    [[nodiscard]] std::uint16_t shard_count() const noexcept {
        return static_cast<std::uint16_t>(shards.size());
    }
};

template <typename TraitsT>
class AsyncRuntime {
public:
    using Traits = TraitsT;
    using Thread = typename Traits::Thread;
    using Task = BasicTask<AsyncRuntime<Traits>>;
    using TraitConfig = detail::RuntimeTraitsConfig<Traits>;

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_task_handle_fragment.hpp"
#include "af/detail/runtime_public_config_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE

    AsyncRuntime() = delete;

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_lifecycle_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_parallel_api_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_io_resource_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_io_file_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_io_socket_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE

private:
    // These fragments are included inside AsyncRuntime to keep templates visible and inlineable.
#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_common_fragment.hpp"
#include "af/detail/runtime_parallel_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE

    class alignas(detail::hardware_cache_line_size) Executor {
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
        struct IoWaitRegistration;
#endif
#if AF_DETAIL_HAS_KQUEUE
        struct KqueueTimeoutRegistration;
#endif
#if defined(__linux__)
        struct IoUringOperation;
#endif

    public:
#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_control_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

        [[nodiscard]] bool io_backend_available() const noexcept {
            return native_io_backend_available();
        }

        [[nodiscard]] bool io_uring_backend_available() const noexcept {
#if defined(__linux__)
            return io_uring_fd_ >= 0;
#else
            return false;
#endif
        }

        [[nodiscard]] bool io_uring_poll_available() const noexcept {
#if defined(__linux__)
            return io_uring_fd_ >= 0 && io_uring_poll_add_available_;
#else
            return false;
#endif
        }

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_uring_resource_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_wait_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_uring_file_data_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_uring_fd_lifecycle_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_uring_socket_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_task_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

    private:
#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_state_types_fragment.hpp"
#include "af/detail/runtime_executor_thread_kind_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_uring_submit_core_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_native_io_backend_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_backend_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_uring_backend_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_poll_helpers_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_core_state_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE
    };

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_dispatch_fragment.hpp"
#include "af/detail/runtime_lifecycle_fragment.hpp"
#include "af/detail/runtime_state_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE
};

} // namespace af
