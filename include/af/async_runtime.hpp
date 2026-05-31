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

    template <typename TaskT>
    class [[nodiscard]] TaskHandle {
    public:
        TaskHandle() noexcept = default;
        explicit TaskHandle(TaskT* task) noexcept : task_(task) {}

        TaskHandle(const TaskHandle&) = delete;
        TaskHandle& operator=(const TaskHandle&) = delete;

        TaskHandle(TaskHandle&& other) noexcept : task_(std::exchange(other.task_, nullptr)) {}

        TaskHandle& operator=(TaskHandle&& other) noexcept {
            if (this != &other) {
                reset();
                task_ = std::exchange(other.task_, nullptr);
            }
            return *this;
        }

        ~TaskHandle() {
            reset();
        }

        [[nodiscard]] TaskT* get() const noexcept {
            return task_;
        }

        [[nodiscard]] TaskT& operator*() const noexcept {
            AF_ASSERT(task_ != nullptr);
            return *task_;
        }

        [[nodiscard]] TaskT* operator->() const noexcept {
            AF_ASSERT(task_ != nullptr);
            return task_;
        }

        [[nodiscard]] explicit operator bool() const noexcept {
            return task_ != nullptr;
        }

        [[nodiscard]] bool scheduled() const noexcept {
            return task_ != nullptr && !AsyncRuntime::is_task_created(task_);
        }

        void reset() noexcept {
            if (task_ != nullptr) {
                AsyncRuntime::release_task_handle(task_);
                task_ = nullptr;
            }
        }

    private:
        TaskT* task_{nullptr};
    };

    static constexpr std::uint16_t thread_count = Traits::thread_count;
    static constexpr std::uint16_t invalid_thread_index = thread_count;
    static_assert(thread_count > 0, "AsyncRuntime requires at least one fixed thread");

    static constexpr std::size_t spsc_queue_capacity = TraitConfig::spsc_queue_capacity;
    static constexpr std::size_t external_queue_capacity = TraitConfig::external_queue_capacity;
    static constexpr QueueFullPolicy queue_full_policy = TraitConfig::queue_full_policy;
    static constexpr ShutdownPolicy shutdown_policy = TraitConfig::shutdown_policy;
    static constexpr bool task_registry_enabled = TraitConfig::task_registry_enabled;
    static constexpr unsigned io_uring_entries = TraitConfig::io_uring_entries;
    static constexpr unsigned io_uring_submit_batch_threshold =
        TraitConfig::io_uring_submit_batch_threshold;
    static constexpr unsigned io_uring_cq_entries = TraitConfig::io_uring_cq_entries;
    static constexpr unsigned io_uring_setup_flags = TraitConfig::io_uring_setup_flags;
    static constexpr bool io_uring_setup_sqpoll = TraitConfig::io_uring_setup_sqpoll;
    static constexpr unsigned io_uring_sqpoll_idle_ms =
        TraitConfig::io_uring_sqpoll_idle_ms;
    static constexpr int io_uring_sqpoll_cpu = TraitConfig::io_uring_sqpoll_cpu;
    static constexpr bool io_uring_setup_submit_all = TraitConfig::io_uring_setup_submit_all;
    static constexpr bool io_uring_setup_coop_taskrun =
        TraitConfig::io_uring_setup_coop_taskrun;
    static constexpr bool io_uring_setup_single_issuer =
        TraitConfig::io_uring_setup_single_issuer;
    static constexpr bool io_uring_setup_defer_taskrun =
        TraitConfig::io_uring_setup_defer_taskrun;
    static constexpr std::size_t io_wait_reserve = TraitConfig::io_wait_reserve;
    static constexpr std::size_t io_deferred_delete_reserve =
        TraitConfig::io_deferred_delete_reserve;
    static constexpr std::size_t io_uring_provided_buffer_group_reserve =
        TraitConfig::io_uring_provided_buffer_group_reserve;
    static_assert(spsc_queue_capacity > 0, "spsc_queue_capacity must be greater than zero");
    static_assert(external_queue_capacity > 0, "external_queue_capacity must be greater than zero");
    static_assert(io_uring_entries > 0, "io_uring_entries must be greater than zero");
    static_assert(
        std::has_single_bit(io_uring_entries),
        "io_uring_entries must be a power of two");
    static_assert(
        io_uring_submit_batch_threshold > 0,
        "io_uring_submit_batch_threshold must be greater than zero");
    static_assert(
        io_uring_submit_batch_threshold <= io_uring_entries,
        "io_uring_submit_batch_threshold must not exceed io_uring_entries");
    static_assert(
        io_uring_cq_entries == 0U || std::has_single_bit(io_uring_cq_entries),
        "io_uring_cq_entries must be zero or a power of two");
    static_assert(
        io_uring_cq_entries == 0U || io_uring_cq_entries >= io_uring_entries,
        "io_uring_cq_entries must be zero or not less than io_uring_entries");
    static_assert(
        !(io_uring_setup_sqpoll || io_uring_sqpoll_cpu >= 0) || io_uring_sqpoll_idle_ms > 0U,
        "io_uring_sqpoll_idle_ms must be greater than zero when SQPOLL is enabled");

    [[nodiscard]] static constexpr ThreadKind thread_kind(Thread thread) noexcept {
        if constexpr (requires { Traits::thread_kind(thread); }) {
            return Traits::thread_kind(thread);
        } else {
            static_cast<void>(thread);
            return ThreadKind::Worker;
        }
    }

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
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
        struct IoWaitRegistration {
            int fd{-1};
            std::uint32_t events{0};
            Task* task{nullptr};
            IoResult* result{nullptr};
#if defined(__linux__)
            IoUringOperation* poll_operation{nullptr};
#endif
        };

#endif

#if AF_DETAIL_HAS_KQUEUE
        struct KqueueTimeoutRegistration {
            Task* task{nullptr};
            IoResult* result{nullptr};
            KqueueTimeoutRegistration* prev{nullptr};
            KqueueTimeoutRegistration* next{nullptr};
            uintptr_t ident{0};
        };
#endif

#if defined(__linux__)
        struct IoUringOperation {
            Task* task{nullptr};
            IoResult* result{nullptr};
            IoUringOperation* prev{nullptr};
            IoUringOperation* next{nullptr};
            detail::IoUringMessage* msg{nullptr};
            union {
                detail::IoUringSocketAddress* socket_address;
                __kernel_timespec timeout;
            };
            IoWaitRegistration* wait_registration{nullptr};
            std::uint32_t complete_events{0};
            int direct_file_index{-1};
            std::uint8_t opcode{0};
            bool cancel_requested{false};
            bool multishot{false};
            bool poll_wait{false};
            bool zero_copy_send{false};
            bool zero_copy_primary_done{false};
            bool zero_copy_notification_done{false};
        };

        enum class IoUringPollSubmitResult : std::uint8_t {
            Submitted,
            Fallback,
            Failed,
            BackendClosed,
        };
#endif

        [[nodiscard]] bool io_thread() const noexcept {
            return native_io_thread()
#if defined(__linux__)
                || io_uring_thread()
#endif
                ;
        }

        [[nodiscard]] bool native_io_thread() const noexcept {
#if AF_DETAIL_HAS_EPOLL
            return kind_ == ThreadKind::Io ||
                   kind_ == ThreadKind::Epoll ||
                   kind_ == ThreadKind::IoUring;
#elif AF_DETAIL_HAS_KQUEUE
            return kind_ == ThreadKind::Io || kind_ == ThreadKind::Kqueue;
#else
            return false;
#endif
        }

        [[nodiscard]] bool io_uring_thread() const noexcept {
            return kind_ == ThreadKind::IoUring;
        }

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
