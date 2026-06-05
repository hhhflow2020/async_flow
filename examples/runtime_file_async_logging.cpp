#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <thread>

#include "af/log.hpp"
#include "af/runtime.hpp"

namespace {

inline constexpr std::uint32_t runtime_file_records_per_task = 128;

class RuntimeFileLogTask final : public af::runtime_task {
public:
    RuntimeFileLogTask(af::runtime_task::factory_token token, af::runtime &owner,
                       std::uint32_t task_id, std::atomic<int> &completed)
        : af::runtime_task(token, owner), task_id_(task_id), completed_(completed) {}

    [[nodiscard]] bool do_it(af::thread_ref thread) noexcept {
        return schedule_to(thread);
    }

private:
    af::task_result run_task() noexcept override {
        for (std::uint32_t i = 0; i < runtime_file_records_per_task; ++i) {
            LOG(INFO) << "runtime file log task=" << task_id_ << " seq=" << i
                      << " thread=" << af::runtime::current_thread_index();
        }
        completed_.fetch_add(1, std::memory_order_release);
        return done();
    }

    std::uint32_t task_id_{0};
    std::atomic<int> &completed_;
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

    af::runtime_config config;
    config.threads = {
        af::cpu_threads("file-log-cpu", 2),
    };
    config.logger = af::log_config::ordered();
    config.logger.consumer_thread = af::thread_selector::cpu(0);
    config.logger.queue_capacity = 1U << 15U;
    config.logger.max_batch_records = 512;
    config.logger.max_batch_delay = 1000us;
    config.logger.backends = {
        af::file_log_backend_config{log_path.string(), false, true, 64},
    };
    config.shutdown.log_flush_timeout = 5s;

    af::runtime runtime(config);
    if (!runtime.start()) {
        std::cerr << "failed to start runtime\n";
        return 1;
    }

    constexpr int task_count = 4;
    constexpr int expected_records =
        task_count * static_cast<int>(runtime_file_records_per_task) + 1;
    const af::thread_group_ref cpu_threads = runtime.cpu_threads();
    if (cpu_threads.empty()) {
        std::cerr << "runtime has no cpu threads\n";
        runtime.stop();
        return 1;
    }
    std::atomic<int> completed{0};
    bool started = true;
    for (std::uint32_t i = 0; i < static_cast<std::uint32_t>(task_count); ++i) {
        auto task = af::make_task<RuntimeFileLogTask>(runtime, i, completed);
        started = task->do_it(cpu_threads.shard(i)) && started;
    }

    const bool completed_all = wait_for_completion(completed, task_count);
    LOG(INFO) << "external runtime file log after runtime tasks";

    const bool flushed = runtime.flush_logger(5s);
    runtime.stop();

    const std::string contents = read_file(log_path);
    const int file_records = static_cast<int>(std::count(contents.begin(), contents.end(), '\n'));

    std::cout << "started=" << started << " completed=" << completed.load(std::memory_order_acquire)
              << " flushed=" << flushed << " file_records=" << file_records << " file=" << log_path
              << '\n';
    return started && completed_all && flushed && file_records == expected_records ? 0 : 1;
}
