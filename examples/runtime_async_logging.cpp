#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <thread>
#include <utility>

#include "af/async_runtime.hpp"
#include "af/log.hpp"

namespace {

struct RuntimeLogThreadTag;

struct RuntimeLogTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<RuntimeLogThreadTag, 2, af::thread_kind::cpu, "logic">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
};

using runtime_async = af::AsyncRuntime<RuntimeLogTraits>;
using RuntimeLogTaskBase = runtime_async::Task;

struct RuntimeLogThreads {
    static constexpr auto logic = runtime_async::thread_group<RuntimeLogThreadTag>();
};

class RuntimeLogTask final : public RuntimeLogTaskBase {
public:
    explicit RuntimeLogTask(RuntimeLogTaskBase::FactoryToken token) : RuntimeLogTaskBase(token) {}

    bool do_it(std::uint32_t task_id, std::atomic<int> *completed) {
        task_id_ = task_id;
        completed_ = completed;
        return schedule(RuntimeLogThreads::logic.shard(task_id));
    }

private:
    af::TaskResult run() override {
        for (std::uint32_t i = 0; i < records_per_task; ++i) {
            LOG(INFO) << "runtime log task=" << task_id_ << " seq=" << i
                      << " thread=" << runtime_async::current_thread_index();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    static constexpr std::uint32_t records_per_task = 512;

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

} // namespace

int main(int argc, char **argv) {
    using namespace std::chrono_literals;

    af::AsyncLogConfig config = af::AsyncLogConfig::relaxed(runtime_async::thread_count);
    config.queue_capacity = 1U << 15U;
    config.runtime_lane_capacity = 1U << 15U;
    config.max_batch_size = 512;
    config.overflow_spin_count = 128;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.flush_poll_interval = 1ms;

    const std::filesystem::path log_path =
        argc > 1 ? argv[1] : "asyncflow-runtime-logging-example.log";
    config.backends.push_back(af::make_file_log_backend({.path = log_path, .append = false}));

    runtime_async::init();
    auto logging = af::start_async_logging_for_runtime<runtime_async>(
        std::move(config), RuntimeLogThreads::logic.at<0>());

    constexpr int task_count = 4;
    std::atomic<int> completed{0};
    bool started = true;
    for (std::uint32_t i = 0; i < static_cast<std::uint32_t>(task_count); ++i) {
        started = runtime_async::start_task<RuntimeLogTask>(i, &completed) && started;
    }

    const bool completed_all = wait_for_completion(completed, task_count);
    LOG(INFO) << "external log before runtime shutdown";

    const bool flushed = logging->flush(5s);
    const af::AsyncLogStats stats = logging->stats();
    logging->stop();
    runtime_async::shutdown();

    std::cout << "started=" << started << " completed=" << completed.load(std::memory_order_acquire)
              << " accepted=" << stats.accepted << " dropped=" << stats.dropped
              << " flushed=" << flushed << " file=" << log_path << '\n';
    return started && completed_all && flushed && stats.dropped == 0U ? 0 : 1;
}
