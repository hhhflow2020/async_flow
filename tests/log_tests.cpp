#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "af/log.hpp"

namespace {

[[nodiscard]] std::string read_file(const std::filesystem::path &path) {
    std::ifstream input(path);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

class BlockingLogBackend final : public af::LogBackend {
public:
    void write_batch(std::span<af::detail::LogRecord *const> records) noexcept override {
        static_cast<void>(records);
        std::unique_lock lock(mutex_);
        entered_ = true;
        entered_cv_.notify_all();
        release_cv_.wait(lock, [this] { return released_; });
    }

    [[nodiscard]] bool wait_until_entered(std::chrono::milliseconds timeout) {
        std::unique_lock lock(mutex_);
        return entered_cv_.wait_for(lock, timeout, [this] { return entered_; });
    }

    void release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        release_cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable entered_cv_;
    std::condition_variable release_cv_;
    bool entered_{false};
    bool released_{false};
};

} // namespace

TEST(LogTests, AsyncFileBackendWritesAbslFormattedMessages) {
    const auto path = std::filesystem::path(::testing::TempDir()) / "asyncflow-log-file.log";
    std::filesystem::remove(path);

    af::AsyncLogConfig config;
    config.backends.push_back(af::make_file_log_backend({.path = path, .append = false}));
    auto logging = af::start_async_logging(std::move(config));

    LOG(INFO) << "af async file backend test";

    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
    logging.reset();

    const std::string contents = read_file(path);
    EXPECT_NE(contents.find("af async file backend test"), std::string::npos);
}

TEST(LogTests, QueueOverflowDropsNewestWithoutBlockingProducer) {
    auto backend = std::make_unique<BlockingLogBackend>();
    auto *blocking_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 2;
    config.max_batch_size = 1;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.backends.push_back(std::move(backend));
    auto logging = af::start_async_logging(std::move(config));

    LOG(INFO) << "block log worker";
    ASSERT_TRUE(blocking_backend->wait_until_entered(std::chrono::seconds(2)));

    for (int i = 0; i < 256; ++i) {
        LOG(INFO) << "overflow candidate " << i;
    }

    EXPECT_GT(logging->stats().dropped, 0U);

    blocking_backend->release();
    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
}
