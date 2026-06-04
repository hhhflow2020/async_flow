#pragma once

#include "af/async_flow.hpp"

struct AppLogicThreadTag;
struct AppDbThreadTag;
struct AppIoThreadTag;

struct AppRuntimeTraits {
    static constexpr auto threads =
        af::thread_layout(af::thread_group<AppLogicThreadTag, 4, af::thread_kind::cpu>("logic"),
                          af::thread_group<AppDbThreadTag, 1, af::thread_kind::cpu>("db"),
                          af::thread_group<AppIoThreadTag, 1, af::thread_kind::io>("io"));
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using async = af::AsyncRuntime<AppRuntimeTraits>;
using Task = async::Task;
using AppThread = async::Thread;

inline constexpr auto player_logic_threads = async::thread_group<AppLogicThreadTag>();
inline constexpr auto app_db_threads = async::thread_group<AppDbThreadTag>();
inline constexpr auto app_io_threads = async::thread_group<AppIoThreadTag>();
inline constexpr AppThread player_logic_begin = player_logic_threads.begin();
inline constexpr std::uint16_t player_logic_shard_count = player_logic_threads.count;

struct AppThreads {
    static constexpr AppThread Logic_0 = player_logic_threads.template at<0>();
    static constexpr AppThread Logic_1 = player_logic_threads.template at<1>();
    static constexpr AppThread Logic_2 = player_logic_threads.template at<2>();
    static constexpr AppThread Logic_3 = player_logic_threads.template at<3>();
    static constexpr AppThread DB_0 = app_db_threads.template at<0>();
    static constexpr AppThread IO_0 = app_io_threads.template at<0>();
};

inline AppThread player_thread(std::uint64_t player_id) noexcept {
    return player_logic_threads.shard(player_id);
}
