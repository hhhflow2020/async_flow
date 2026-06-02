#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include <gtest/gtest.h>

#include "af/log.hpp"

#if !defined(_WIN32)
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

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

class CountingLogBackend final : public af::LogBackend {
public:
    void write_batch(std::span<af::detail::LogRecord *const> records) noexcept override {
        record_count_.fetch_add(records.size(), std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t record_count() const noexcept {
        return record_count_.load(std::memory_order_acquire);
    }

private:
    std::atomic<std::size_t> record_count_{0};
};

#if !defined(_WIN32)
void close_fd(int &fd) noexcept {
    if (fd >= 0) {
        static_cast<void>(::close(fd));
        fd = -1;
    }
}

void set_fd_nonblocking(int fd) noexcept {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        static_cast<void>(::fcntl(fd, F_SETFL, flags | O_NONBLOCK));
    }
}

[[nodiscard]] bool transient_socket_error() noexcept {
    return errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK;
}

[[nodiscard]] int make_loopback_tcp_listener(std::uint16_t &port) noexcept {
    int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) {
        return -1;
    }

    const int reuse = 1;
    static_cast<void>(::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 ||
        ::listen(listener, 16) != 0) {
        close_fd(listener);
        return -1;
    }

    socklen_t address_size = sizeof(address);
    if (::getsockname(listener, reinterpret_cast<sockaddr *>(&address), &address_size) != 0) {
        close_fd(listener);
        return -1;
    }
    port = ntohs(address.sin_port);
    set_fd_nonblocking(listener);
    return listener;
}

[[nodiscard]] int accept_until(int listener,
                               std::chrono::steady_clock::time_point deadline) noexcept {
    while (std::chrono::steady_clock::now() < deadline) {
        int accepted = ::accept(listener, nullptr, nullptr);
        if (accepted >= 0) {
            set_fd_nonblocking(accepted);
            return accepted;
        }
        if (!transient_socket_error()) {
            return -1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return -1;
}

[[nodiscard]] std::string recv_until(int fd, std::string_view marker,
                                     std::chrono::steady_clock::time_point deadline) {
    std::string received;
    std::array<char, 256> buffer{};
    while (std::chrono::steady_clock::now() < deadline &&
           received.find(marker) == std::string::npos) {
        const auto n = ::recv(fd, buffer.data(), buffer.size(), 0);
        if (n > 0) {
            received.append(buffer.data(), static_cast<std::size_t>(n));
            continue;
        }
        if (n == 0) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        break;
    }
    return received;
}
#endif

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

TEST(LogTests, BlockOverflowWaitsForQueueCapacity) {
    auto backend = std::make_unique<BlockingLogBackend>();
    auto *blocking_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 1;
    config.queue_shard_count = 1;
    config.max_batch_size = 1;
    config.overflow_policy = af::LogOverflowPolicy::Block;
    config.backends.push_back(std::move(backend));

    af::AsyncLogger logger(std::move(config));
    logger.start();

    ASSERT_TRUE(logger.try_log("block log worker\n"));
    ASSERT_TRUE(blocking_backend->wait_until_entered(std::chrono::seconds(2)));
    ASSERT_TRUE(logger.try_log("queued log one\n"));
    ASSERT_TRUE(logger.try_log("queued log two\n"));

    std::atomic<bool> accepted{false};
    std::atomic<bool> finished{false};
    std::thread producer([&] {
        accepted.store(logger.try_log("wait for queue capacity\n"), std::memory_order_release);
        finished.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(finished.load(std::memory_order_acquire));

    blocking_backend->release();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!finished.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!finished.load(std::memory_order_acquire)) {
        logger.shutdown();
    }
    producer.join();

    EXPECT_TRUE(finished.load(std::memory_order_acquire));
    EXPECT_TRUE(accepted.load(std::memory_order_acquire));
    ASSERT_TRUE(logger.flush(std::chrono::seconds(2)));
    logger.shutdown();
}

TEST(LogTests, ShardedQueuesAvoidSingleQueueProducerContention) {
    auto backend = std::make_unique<BlockingLogBackend>();
    auto *blocking_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 1;
    config.queue_shard_count = 4;
    config.max_batch_size = 1;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.backends.push_back(std::move(backend));

    af::AsyncLogger logger(std::move(config));
    logger.start();

    ASSERT_TRUE(logger.try_log("block log worker\n"));
    ASSERT_TRUE(blocking_backend->wait_until_entered(std::chrono::seconds(2)));

    std::array<std::thread, 4> producers;
    std::atomic<int> accepted{0};
    for (std::size_t i = 0; i < producers.size(); ++i) {
        producers[i] = std::thread([&logger, &accepted] {
            if (logger.try_log("producer shard log\n")) {
                accepted.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto &producer : producers) {
        producer.join();
    }

    EXPECT_EQ(accepted.load(std::memory_order_acquire), 4);
    EXPECT_EQ(logger.stats().dropped, 0U);

    blocking_backend->release();
    ASSERT_TRUE(logger.flush(std::chrono::seconds(2)));
    logger.shutdown();
}

TEST(LogTests, ShardedQueuesDrainConcurrentProducers) {
    auto backend = std::make_unique<CountingLogBackend>();
    auto *counting_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 4096;
    config.queue_shard_count = 8;
    config.max_batch_size = 64;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.backends.push_back(std::move(backend));

    af::AsyncLogger logger(std::move(config));
    logger.start();

    constexpr int producer_count = 8;
    constexpr int records_per_producer = 128;
    constexpr int expected_records = producer_count * records_per_producer;

    std::array<std::thread, producer_count> producers;
    std::atomic<int> accepted{0};
    for (int producer = 0; producer < producer_count; ++producer) {
        producers[producer] = std::thread([&logger, &accepted] {
            for (int i = 0; i < records_per_producer; ++i) {
                if (logger.try_log("concurrent sharded log\n")) {
                    accepted.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto &producer : producers) {
        producer.join();
    }

    ASSERT_TRUE(logger.flush(std::chrono::seconds(2)));
    const af::AsyncLogStats stats = logger.stats();
    EXPECT_EQ(accepted.load(std::memory_order_acquire), expected_records);
    EXPECT_EQ(stats.accepted, static_cast<std::uint64_t>(expected_records));
    EXPECT_EQ(stats.dropped, 0U);
    EXPECT_EQ(counting_backend->record_count(), static_cast<std::size_t>(expected_records));
    logger.shutdown();
}

TEST(LogTests, RecordPoolReusesSlotsAcrossFlushes) {
    auto backend = std::make_unique<CountingLogBackend>();
    auto *counting_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 1;
    config.queue_shard_count = 1;
    config.max_batch_size = 1;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.backends.push_back(std::move(backend));

    af::AsyncLogger logger(std::move(config));
    logger.start();

    constexpr int record_count = 32;
    for (int i = 0; i < record_count; ++i) {
        ASSERT_TRUE(logger.try_log("reused pooled log record\n"));
        ASSERT_TRUE(logger.flush(std::chrono::seconds(2)));
    }

    const af::AsyncLogStats stats = logger.stats();
    EXPECT_EQ(stats.accepted, static_cast<std::uint64_t>(record_count));
    EXPECT_EQ(stats.dropped, 0U);
    EXPECT_EQ(counting_backend->record_count(), static_cast<std::size_t>(record_count));
    logger.shutdown();
}

TEST(LogTests, TcpBackendWritesBatchedRecordsToLoopbackStream) {
#if defined(_WIN32)
    GTEST_SKIP() << "tcp log backend loopback test is POSIX-only";
#else
    std::uint16_t port = 0;
    int listener = make_loopback_tcp_listener(port);
    ASSERT_GE(listener, 0) << std::strerror(errno);

    std::string received;
    std::atomic<bool> server_done{false};
    std::thread server([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        int accepted = accept_until(listener, deadline);
        if (accepted < 0) {
            server_done.store(true, std::memory_order_release);
            return;
        }
        received = recv_until(accepted, "tcp backend four\n", deadline);
        close_fd(accepted);
        server_done.store(true, std::memory_order_release);
    });

    af::TcpLogBackend backend({
        .host = "127.0.0.1",
        .port = port,
        .reconnect_interval = std::chrono::milliseconds(1),
    });
    std::array<af::detail::LogRecord, 4> records;
    records[0].reset("tcp backend one\n");
    records[1].reset("tcp backend two\n");
    records[2].reset("tcp backend three\n");
    records[3].reset("tcp backend four\n");
    std::array<af::detail::LogRecord *, 4> record_ptrs{
        &records[0],
        &records[1],
        &records[2],
        &records[3],
    };

    for (int attempt = 0; attempt < 250 && !server_done.load(std::memory_order_acquire);
         ++attempt) {
        backend.write_batch(
            std::span<af::detail::LogRecord *const>(record_ptrs.data(), record_ptrs.size()));
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    server.join();
    close_fd(listener);

    EXPECT_NE(received.find("tcp backend one\n"), std::string::npos);
    EXPECT_NE(received.find("tcp backend two\n"), std::string::npos);
    EXPECT_NE(received.find("tcp backend three\n"), std::string::npos);
    EXPECT_NE(received.find("tcp backend four\n"), std::string::npos);
#endif
}
