#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <thread>

#include "af/async_flow.hpp"

namespace {

struct RuntimeFileLogicThreadTag;
struct RuntimeFileIoThreadTag;

inline constexpr std::uint32_t runtime_file_records_per_task = 128;

#if defined(__linux__)
inline constexpr af::ThreadKind runtime_file_io_kind = af::ThreadKind::IoUring;
#else
inline constexpr af::ThreadKind runtime_file_io_kind = af::ThreadKind::Io;
#endif

struct RuntimeFileTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<RuntimeFileLogicThreadTag, 2, af::ThreadKind::Worker, "file-log-cpu">(),
        af::thread_group<RuntimeFileIoThreadTag, 1, runtime_file_io_kind, "file-log-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using runtime_file_async = af::AsyncRuntime<RuntimeFileTraits>;
using RuntimeFileTaskBase = runtime_file_async::Task;

struct RuntimeFileThreads {
    static constexpr auto logic = runtime_file_async::thread_group<RuntimeFileLogicThreadTag>();
    static constexpr auto IO_0 =
        runtime_file_async::thread_group<RuntimeFileIoThreadTag>().template at<0>();
};

class RuntimeFileLogTask final : public RuntimeFileTaskBase {
public:
    explicit RuntimeFileLogTask(RuntimeFileTaskBase::FactoryToken token)
        : RuntimeFileTaskBase(token) {}

    bool do_it(std::uint32_t task_id, std::atomic<int> *completed) {
        task_id_ = task_id;
        completed_ = completed;
        return schedule(RuntimeFileThreads::logic.shard(task_id));
    }

private:
    af::TaskResult run() override {
        for (std::uint32_t i = 0; i < runtime_file_records_per_task; ++i) {
            LOG(INFO) << "runtime file log task=" << task_id_ << " seq=" << i
                      << " thread=" << runtime_file_async::current_thread_index();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::uint32_t task_id_{0};
    std::atomic<int> *completed_{nullptr};
};

bool wait_for_completion(std::atomic<int> &completed, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (completed.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

std::string read_file(const std::filesystem::path &path) {
    std::ifstream input(path);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

} // namespace

int main(int argc, char **argv) {
    using namespace std::chrono_literals;

    const std::filesystem::path log_path =
        argc > 1 ? argv[1] : "asyncflow-runtime-file-logging-example.log";
    std::filesystem::remove(log_path);

    runtime_file_async::init();

    auto backend = std::make_unique<af::RuntimeFileLogBackend<runtime_file_async>>(
        af::RuntimeFileLogBackendConfig<runtime_file_async>{
            .thread = RuntimeFileThreads::IO_0,
            .path = log_path,
            .append = false,
            .fsync_on_flush = true,
            .batch_queue_capacity = 128,
            .max_batch_records = 256,
            .max_batches_per_run = 64,
        });
    auto *runtime_file_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 1U << 15U;
    config.runtime_queue_capacity = 1U << 15U;
    config.max_batch_size = 512;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.flush_poll_interval = 1ms;
    config.consumer_thread_name = "log";
    config.backends.push_back(std::move(backend));

    auto logging = af::start_async_logging_for_runtime<runtime_file_async>(std::move(config));

    constexpr int task_count = 4;
    constexpr int expected_records =
        task_count * static_cast<int>(runtime_file_records_per_task) + 1;
    std::atomic<int> completed{0};
    bool started = true;
    for (std::uint32_t i = 0; i < static_cast<std::uint32_t>(task_count); ++i) {
        started = runtime_file_async::start_task<RuntimeFileLogTask>(i, &completed) && started;
    }

    const bool completed_all = wait_for_completion(completed, task_count);
    LOG(INFO) << "external runtime file log after runtime tasks";

    const bool flushed = logging->flush(5s);
    const af::AsyncLogStats stats = logging->stats();
    const af::RuntimeFileLogBackendStats backend_stats = runtime_file_backend->stats();
    logging->stop();
    runtime_file_async::shutdown();

    const std::string contents = read_file(log_path);
    const int file_records = static_cast<int>(std::count(contents.begin(), contents.end(), '\n'));

    std::cout << "started=" << started << " completed=" << completed.load(std::memory_order_acquire)
              << " accepted=" << stats.accepted << " dropped=" << stats.dropped
              << " flushed=" << flushed << " written=" << backend_stats.written_records
              << " file_records=" << file_records << " file=" << log_path << '\n';
    return started && completed_all && flushed && stats.dropped == 0U &&
                   backend_stats.dropped_records == 0U && file_records == expected_records
               ? 0
               : 1;
}
