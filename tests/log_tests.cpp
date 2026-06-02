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
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include <gtest/gtest.h>

#include "af/async_runtime.hpp"
#include "af/log.hpp"

#if !defined(_WIN32)
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
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

template <typename T> bool wait_until_at_least(std::atomic<T> &value, T expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

#if defined(__linux__) || defined(__APPLE__)
class ThreadNameLogBackend final : public af::LogBackend {
public:
    void write_batch(std::span<af::detail::LogRecord *const> records) noexcept override {
        static_cast<void>(records);
        std::array<char, 16> name{};
        if (::pthread_getname_np(::pthread_self(), name.data(), name.size()) != 0) {
            return;
        }

        std::lock_guard lock(mutex_);
        thread_name_ = name.data();
    }

    [[nodiscard]] std::string thread_name() const {
        std::lock_guard lock(mutex_);
        return thread_name_;
    }

private:
    mutable std::mutex mutex_;
    std::string thread_name_;
};
#endif

struct LogTestRuntimeThreadTag;

struct LogTestRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<LogTestRuntimeThreadTag, 1, af::ThreadKind::Worker, "log-src">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
};

using LogTestRuntime = af::AsyncRuntime<LogTestRuntimeTraits>;
using LogTestTaskBase = LogTestRuntime::Task;

struct LogTestThreads {
    static constexpr auto Runtime_0 =
        LogTestRuntime::thread_group<LogTestRuntimeThreadTag>().template at<0>();
};

class RuntimeLogTask final : public LogTestTaskBase {
public:
    explicit RuntimeLogTask(LogTestTaskBase::FactoryToken token) : LogTestTaskBase(token) {}

    bool do_it(std::atomic<int> *completed) {
        completed_ = completed;
        return schedule(LogTestThreads::Runtime_0);
    }

private:
    af::TaskResult run() override {
        LOG(INFO) << "runtime spsc lane log";
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
};

class RuntimeTaskIdLogTask final : public LogTestTaskBase {
public:
    explicit RuntimeTaskIdLogTask(LogTestTaskBase::FactoryToken token) : LogTestTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<LogTestTaskBase::TaskId> *observed_task_id,
               std::atomic<LogTestTaskBase::TaskId> *observed_current_task_id) {
        completed_ = completed;
        observed_task_id_ = observed_task_id;
        observed_current_task_id_ = observed_current_task_id;
        return schedule(LogTestThreads::Runtime_0);
    }

private:
    af::TaskResult run() override {
        observed_task_id_->store(task_id(), std::memory_order_release);
        observed_current_task_id_->store(LogTestRuntime::current_task_id(),
                                         std::memory_order_release);
        LOG(INFO) << "runtime task id log";
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<LogTestTaskBase::TaskId> *observed_task_id_{nullptr};
    std::atomic<LogTestTaskBase::TaskId> *observed_current_task_id_{nullptr};
};

struct LogUdpIoThreadTag;

#if defined(__linux__)
inline constexpr af::ThreadKind log_udp_io_thread_kind = af::ThreadKind::IoUring;
#else
inline constexpr af::ThreadKind log_udp_io_thread_kind = af::ThreadKind::Io;
#endif

struct LogUdpIoRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<LogUdpIoThreadTag, 1, log_udp_io_thread_kind, "log-udp-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using LogUdpIoRuntime = af::AsyncRuntime<LogUdpIoRuntimeTraits>;

struct LogUdpIoThreads {
    static constexpr auto IO_0 =
        LogUdpIoRuntime::thread_group<LogUdpIoThreadTag>().template at<0>();
};

class LogUdpIoRuntimeGuard {
public:
    LogUdpIoRuntimeGuard() {
        LogUdpIoRuntime::init();
    }

    ~LogUdpIoRuntimeGuard() {
        LogUdpIoRuntime::shutdown();
    }
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

[[nodiscard]] int make_loopback_udp_socket(std::uint16_t &port) noexcept {
    int socket_fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd < 0) {
        return -1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(socket_fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        close_fd(socket_fd);
        return -1;
    }

    socklen_t address_size = sizeof(address);
    if (::getsockname(socket_fd, reinterpret_cast<sockaddr *>(&address), &address_size) != 0) {
        close_fd(socket_fd);
        return -1;
    }
    port = ntohs(address.sin_port);
    set_fd_nonblocking(socket_fd);
    return socket_fd;
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

template <std::size_t Count>
[[nodiscard]] std::size_t recv_datagrams_until(int fd, std::array<std::string, Count> &received,
                                               std::chrono::steady_clock::time_point deadline) {
    std::size_t count = 0;
    std::array<char, 256> buffer{};
    while (std::chrono::steady_clock::now() < deadline && count < received.size()) {
        const auto n = ::recv(fd, buffer.data(), buffer.size(), 0);
        if (n > 0) {
            received[count].assign(buffer.data(), static_cast<std::size_t>(n));
            ++count;
            continue;
        }
        if (n == 0) {
            continue;
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
    return count;
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

TEST(LogTests, RuntimeFileBackendWritesBatchesOnIoThread) {
    const auto path =
        std::filesystem::path(::testing::TempDir()) / "asyncflow-runtime-log-file.log";
    std::filesystem::remove(path);

    LogUdpIoRuntimeGuard runtime_guard;
    af::RuntimeFileLogBackend<LogUdpIoRuntime> backend({
        .thread = LogUdpIoThreads::IO_0,
        .path = path,
        .append = false,
        .fsync_on_flush = true,
        .batch_queue_capacity = 8,
        .max_batch_records = 4,
        .max_batches_per_run = 8,
    });

    std::array<af::detail::LogRecord, 4> records;
    records[0].reset("runtime file backend one\n");
    records[1].reset("runtime file backend two\n");
    records[2].reset("runtime file backend three\n");
    records[3].reset("runtime file backend four\n");
    std::array<af::detail::LogRecord *, 4> record_ptrs{
        &records[0],
        &records[1],
        &records[2],
        &records[3],
    };

    backend.write_batch(
        std::span<af::detail::LogRecord *const>(record_ptrs.data(), record_ptrs.size()));
    const bool flushed = backend.flush(std::chrono::seconds(2));
    const af::RuntimeFileLogBackendStats stats = backend.stats();
    backend.shutdown();

    const std::string contents = read_file(path);
    EXPECT_TRUE(flushed);
    EXPECT_EQ(stats.queued_records, record_ptrs.size());
    EXPECT_EQ(stats.written_records, record_ptrs.size())
        << "last_error=" << stats.last_error << " stage=" << stats.last_error_stage;
    EXPECT_EQ(stats.dropped_records, 0U)
        << "last_error=" << stats.last_error << " stage=" << stats.last_error_stage;
    EXPECT_GE(stats.flushes, 1U);
    EXPECT_NE(contents.find("runtime file backend one\n"), std::string::npos);
    EXPECT_NE(contents.find("runtime file backend two\n"), std::string::npos);
    EXPECT_NE(contents.find("runtime file backend three\n"), std::string::npos);
    EXPECT_NE(contents.find("runtime file backend four\n"), std::string::npos);
}

TEST(LogTests, RuntimeFileAsyncLoggerBackendWritesOnIoThread) {
    const auto path =
        std::filesystem::path(::testing::TempDir()) / "asyncflow-runtime-async-log-file.log";
    std::filesystem::remove(path);

    LogUdpIoRuntimeGuard runtime_guard;
    auto backend = std::make_unique<af::RuntimeFileLogBackend<LogUdpIoRuntime>>(
        af::RuntimeFileLogBackendConfig<LogUdpIoRuntime>{
            .thread = LogUdpIoThreads::IO_0,
            .path = path,
            .append = false,
            .fsync_on_flush = true,
            .batch_queue_capacity = 8,
            .max_batch_records = 8,
            .max_batches_per_run = 8,
        });
    auto *runtime_file_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 64;
    config.runtime_queue_capacity = 64;
    config.max_batch_size = 8;
    config.flush_poll_interval = std::chrono::milliseconds(1);
    config.backends.push_back(std::move(backend));
    auto logging = af::start_async_logging_for_runtime<LogUdpIoRuntime>(std::move(config));

    LOG(INFO) << "runtime file async logger one";
    LOG(INFO) << "runtime file async logger two";
    LOG(INFO) << "runtime file async logger three";
    LOG(INFO) << "runtime file async logger four";

    const bool flushed = logging->flush(std::chrono::seconds(2));
    const af::RuntimeFileLogBackendStats stats = runtime_file_backend->stats();
    logging->stop();

    const std::string contents = read_file(path);
    EXPECT_TRUE(flushed);
    EXPECT_EQ(stats.queued_records, 4U);
    EXPECT_EQ(stats.written_records, 4U)
        << "last_error=" << stats.last_error << " stage=" << stats.last_error_stage;
    EXPECT_EQ(stats.dropped_records, 0U)
        << "last_error=" << stats.last_error << " stage=" << stats.last_error_stage;
    EXPECT_GE(stats.flushes, 1U);
    EXPECT_NE(contents.find("runtime file async logger one"), std::string::npos);
    EXPECT_NE(contents.find("runtime file async logger two"), std::string::npos);
    EXPECT_NE(contents.find("runtime file async logger three"), std::string::npos);
    EXPECT_NE(contents.find("runtime file async logger four"), std::string::npos);
}

#if defined(__linux__) || defined(__APPLE__)
TEST(LogTests, AsyncLoggerNamesConsumerThread) {
    auto backend = std::make_unique<ThreadNameLogBackend>();
    auto *thread_name_backend = backend.get();

    af::AsyncLogConfig config;
    config.consumer_thread_name = "log";
    config.backends.push_back(std::move(backend));

    af::AsyncLogger logger(std::move(config));
    logger.start();
    ASSERT_TRUE(logger.try_log("named consumer thread\n"));
    ASSERT_TRUE(logger.flush(std::chrono::seconds(2)));

    EXPECT_EQ(thread_name_backend->thread_name(), "af-log-0");
    logger.shutdown();
}
#endif

TEST(LogTests, ProducerShardCacheRefreshesWhenLoggerReusesAddress) {
    alignas(af::AsyncLogger) unsigned char storage[sizeof(af::AsyncLogger)];

    auto make_config = [](std::size_t shard_count, CountingLogBackend *&counter) {
        auto backend = std::make_unique<CountingLogBackend>();
        counter = backend.get();

        af::AsyncLogConfig config;
        config.queue_capacity = 8;
        config.queue_shard_count = shard_count;
        config.max_batch_size = 4;
        config.backends.push_back(std::move(backend));
        return config;
    };

    CountingLogBackend *first_counter = nullptr;
    af::AsyncLogger *first =
        ::new (static_cast<void *>(storage)) af::AsyncLogger(make_config(1, first_counter));
    first->start();
    ASSERT_TRUE(first->try_log("first logger record\n"));
    ASSERT_TRUE(first->flush(std::chrono::seconds(2)));
    EXPECT_EQ(first_counter->record_count(), 1U);
    first->~AsyncLogger();

    CountingLogBackend *second_counter = nullptr;
    af::AsyncLogger *second =
        ::new (static_cast<void *>(storage)) af::AsyncLogger(make_config(8, second_counter));
    second->start();
    ASSERT_TRUE(second->try_log("second logger record\n"));
    ASSERT_TRUE(second->flush(std::chrono::seconds(2)));
    EXPECT_EQ(second_counter->record_count(), 1U);
    second->~AsyncLogger();
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

TEST(LogTests, RuntimeAwareSinkUsesSpscLaneWhenExternalMpscIsFull) {
    auto backend = std::make_unique<BlockingLogBackend>();
    auto *blocking_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 1;
    config.queue_shard_count = 1;
    config.runtime_queue_capacity = 2;
    config.max_batch_size = 1;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.backends.push_back(std::move(backend));
    auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(std::move(config));

    LOG(INFO) << "block log worker";
    ASSERT_TRUE(blocking_backend->wait_until_entered(std::chrono::seconds(2)));

    LOG(INFO) << "external mpsc fill one";
    LOG(INFO) << "external mpsc fill two";
    const af::AsyncLogStats filled = logging->stats();

    LOG(INFO) << "external mpsc overflow";
    const af::AsyncLogStats overflowed = logging->stats();
    ASSERT_GT(overflowed.dropped, filled.dropped);

    LogTestRuntime::init();
    std::atomic<int> runtime_completed{0};
    ASSERT_TRUE(LogTestRuntime::start_task<RuntimeLogTask>(&runtime_completed));
    ASSERT_TRUE(wait_until_at_least(runtime_completed, 1));
    LogTestRuntime::shutdown();

    const af::AsyncLogStats after_runtime = logging->stats();
    EXPECT_EQ(after_runtime.accepted, overflowed.accepted + 1U);

    blocking_backend->release();
    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
}

TEST(LogTests, RuntimeLaneRecordPoolReusesSlotsAcrossFlushes) {
    auto backend = std::make_unique<CountingLogBackend>();
    auto *counting_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 1;
    config.queue_shard_count = 1;
    config.runtime_thread_count = 1;
    config.runtime_queue_capacity = 1;
    config.max_batch_size = 1;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.backends.push_back(std::move(backend));

    af::AsyncLogger logger(std::move(config));
    logger.start();

    constexpr int record_count = 32;
    for (int i = 0; i < record_count; ++i) {
        ASSERT_TRUE(logger.try_log_from_runtime_thread(0, "runtime pooled log record\n"));
        ASSERT_TRUE(logger.flush(std::chrono::seconds(2)));
    }

    const af::AsyncLogStats stats = logger.stats();
    EXPECT_EQ(stats.accepted, static_cast<std::uint64_t>(record_count));
    EXPECT_EQ(stats.dropped, 0U);
    EXPECT_EQ(counting_backend->record_count(), static_cast<std::size_t>(record_count));
    logger.shutdown();
}

TEST(LogTests, RuntimeAwareSinkPrefixesRuntimeTaskId) {
    const auto path = std::filesystem::temp_directory_path() / "async_flow_task_id_log.txt";
    std::filesystem::remove(path);

    af::AsyncLogConfig config;
    config.queue_capacity = 16;
    config.runtime_queue_capacity = 16;
    config.backends.push_back(af::make_file_log_backend({.path = path, .append = false}));
    auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(std::move(config));

    LogTestRuntime::init();
    std::atomic<int> runtime_completed{0};
    std::atomic<LogTestTaskBase::TaskId> observed_task_id{LogTestTaskBase::invalid_task_id};
    std::atomic<LogTestTaskBase::TaskId> observed_current_task_id{LogTestTaskBase::invalid_task_id};
    ASSERT_TRUE(LogTestRuntime::start_task<RuntimeTaskIdLogTask>(
        &runtime_completed, &observed_task_id, &observed_current_task_id));
    ASSERT_TRUE(wait_until_at_least(runtime_completed, 1));
    LogTestRuntime::shutdown();

    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
    logging->stop();

    const auto task_id = observed_task_id.load(std::memory_order_acquire);
    ASSERT_NE(task_id, LogTestTaskBase::invalid_task_id);
    EXPECT_EQ(observed_current_task_id.load(std::memory_order_acquire), task_id);

    const std::string contents = read_file(path);
    EXPECT_NE(contents.find("[task=" + std::to_string(task_id) + "] "), std::string::npos);
    EXPECT_NE(contents.find("runtime task id log"), std::string::npos);
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

TEST(LogTests, UdpBackendWritesBatchedRecordsToLoopbackDatagrams) {
#if defined(_WIN32)
    GTEST_SKIP() << "udp log backend loopback test is POSIX-only";
#else
    std::uint16_t port = 0;
    int socket_fd = make_loopback_udp_socket(port);
    ASSERT_GE(socket_fd, 0) << std::strerror(errno);

    af::UdpLogBackend backend({
        .host = "127.0.0.1",
        .port = port,
    });
    std::array<af::detail::LogRecord, 4> records;
    records[0].reset("udp backend one\n");
    records[1].reset("udp backend two\n");
    records[2].reset("udp backend three\n");
    records[3].reset("udp backend four\n");
    std::array<af::detail::LogRecord *, 4> record_ptrs{
        &records[0],
        &records[1],
        &records[2],
        &records[3],
    };

    backend.write_batch(
        std::span<af::detail::LogRecord *const>(record_ptrs.data(), record_ptrs.size()));

    std::array<std::string, 4> received{};
    const std::size_t received_count = recv_datagrams_until(
        socket_fd, received, std::chrono::steady_clock::now() + std::chrono::seconds(2));
    close_fd(socket_fd);

    std::string combined;
    for (const std::string &message : received) {
        combined.append(message);
    }
    EXPECT_EQ(received_count, record_ptrs.size());
    EXPECT_NE(combined.find("udp backend one\n"), std::string::npos);
    EXPECT_NE(combined.find("udp backend two\n"), std::string::npos);
    EXPECT_NE(combined.find("udp backend three\n"), std::string::npos);
    EXPECT_NE(combined.find("udp backend four\n"), std::string::npos);
#endif
}

TEST(LogTests, RuntimeUdpBackendSendsBatchesOnIoThread) {
#if defined(_WIN32)
    GTEST_SKIP() << "runtime udp log backend loopback test is POSIX-only";
#else
    std::uint16_t port = 0;
    int socket_fd = make_loopback_udp_socket(port);
    ASSERT_GE(socket_fd, 0) << std::strerror(errno);

    LogUdpIoRuntimeGuard runtime_guard;
    af::RuntimeUdpLogBackend<LogUdpIoRuntime> backend({
        .thread = LogUdpIoThreads::IO_0,
        .host = "127.0.0.1",
        .port = port,
        .batch_queue_capacity = 8,
        .max_batch_records = 4,
        .max_datagram_size = 1400,
        .max_batches_per_run = 8,
    });

    std::array<af::detail::LogRecord, 4> records;
    records[0].reset("runtime udp backend one\n");
    records[1].reset("runtime udp backend two\n");
    records[2].reset("runtime udp backend three\n");
    records[3].reset("runtime udp backend four\n");
    std::array<af::detail::LogRecord *, 4> record_ptrs{
        &records[0],
        &records[1],
        &records[2],
        &records[3],
    };

    backend.write_batch(
        std::span<af::detail::LogRecord *const>(record_ptrs.data(), record_ptrs.size()));
    ASSERT_TRUE(backend.flush(std::chrono::seconds(2)));
    const af::RuntimeUdpLogBackendStats stats = backend.stats();
    EXPECT_EQ(stats.queued_records, record_ptrs.size());
    EXPECT_EQ(stats.sent_records, record_ptrs.size());
    EXPECT_EQ(stats.dropped_records, 0U);

    std::array<std::string, 4> received{};
    const std::size_t received_count = recv_datagrams_until(
        socket_fd, received, std::chrono::steady_clock::now() + std::chrono::seconds(2));
    close_fd(socket_fd);
    backend.shutdown();

    std::string combined;
    for (const std::string &message : received) {
        combined.append(message);
    }
    EXPECT_EQ(received_count, record_ptrs.size());
    EXPECT_NE(combined.find("runtime udp backend one\n"), std::string::npos);
    EXPECT_NE(combined.find("runtime udp backend two\n"), std::string::npos);
    EXPECT_NE(combined.find("runtime udp backend three\n"), std::string::npos);
    EXPECT_NE(combined.find("runtime udp backend four\n"), std::string::npos);
#endif
}

TEST(LogTests, RuntimeTcpBackendSendsBatchesOnIoThread) {
#if defined(_WIN32)
    GTEST_SKIP() << "runtime tcp log backend loopback test is POSIX-only";
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
        received = recv_until(accepted, "runtime tcp backend four\n", deadline);
        close_fd(accepted);
        server_done.store(true, std::memory_order_release);
    });

    LogUdpIoRuntimeGuard runtime_guard;
    af::RuntimeTcpLogBackend<LogUdpIoRuntime> backend({
        .thread = LogUdpIoThreads::IO_0,
        .host = "127.0.0.1",
        .port = port,
        .reconnect_interval = std::chrono::milliseconds(1),
        .batch_queue_capacity = 8,
        .max_batch_records = 4,
        .max_batches_per_run = 8,
    });

    std::array<af::detail::LogRecord, 4> records;
    records[0].reset("runtime tcp backend one\n");
    records[1].reset("runtime tcp backend two\n");
    records[2].reset("runtime tcp backend three\n");
    records[3].reset("runtime tcp backend four\n");
    std::array<af::detail::LogRecord *, 4> record_ptrs{
        &records[0],
        &records[1],
        &records[2],
        &records[3],
    };

    backend.write_batch(
        std::span<af::detail::LogRecord *const>(record_ptrs.data(), record_ptrs.size()));
    const bool flushed = backend.flush(std::chrono::seconds(2));
    const af::RuntimeTcpLogBackendStats stats = backend.stats();

    server.join();
    close_fd(listener);
    backend.shutdown();

    EXPECT_TRUE(flushed);
    EXPECT_EQ(stats.queued_records, record_ptrs.size());
    EXPECT_EQ(stats.sent_records, record_ptrs.size())
        << "last_error=" << stats.last_error << " stage=" << stats.last_error_stage;
    EXPECT_EQ(stats.dropped_records, 0U)
        << "last_error=" << stats.last_error << " stage=" << stats.last_error_stage;
    EXPECT_TRUE(server_done.load(std::memory_order_acquire));
    EXPECT_NE(received.find("runtime tcp backend one\n"), std::string::npos);
    EXPECT_NE(received.find("runtime tcp backend two\n"), std::string::npos);
    EXPECT_NE(received.find("runtime tcp backend three\n"), std::string::npos);
    EXPECT_NE(received.find("runtime tcp backend four\n"), std::string::npos);
#endif
}

TEST(LogTests, RuntimeTcpAsyncLoggerBackendSendsOnIoThread) {
#if defined(_WIN32)
    GTEST_SKIP() << "runtime tcp async logger backend loopback test is POSIX-only";
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
        received = recv_until(accepted, "runtime tcp async logger four", deadline);
        close_fd(accepted);
        server_done.store(true, std::memory_order_release);
    });

    LogUdpIoRuntimeGuard runtime_guard;
    auto backend = std::make_unique<af::RuntimeTcpLogBackend<LogUdpIoRuntime>>(
        af::RuntimeTcpLogBackendConfig<LogUdpIoRuntime>{
            .thread = LogUdpIoThreads::IO_0,
            .host = "127.0.0.1",
            .port = port,
            .reconnect_interval = std::chrono::milliseconds(1),
            .batch_queue_capacity = 8,
            .max_batch_records = 8,
            .max_batches_per_run = 8,
        });
    auto *runtime_tcp_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 64;
    config.runtime_queue_capacity = 64;
    config.max_batch_size = 8;
    config.flush_poll_interval = std::chrono::milliseconds(1);
    config.backends.push_back(std::move(backend));
    auto logging = af::start_async_logging_for_runtime<LogUdpIoRuntime>(std::move(config));

    LOG(INFO) << "runtime tcp async logger one";
    LOG(INFO) << "runtime tcp async logger two";
    LOG(INFO) << "runtime tcp async logger three";
    LOG(INFO) << "runtime tcp async logger four";

    const bool flushed = logging->flush(std::chrono::seconds(2));
    const af::RuntimeTcpLogBackendStats stats = runtime_tcp_backend->stats();
    logging->stop();

    server.join();
    close_fd(listener);

    EXPECT_TRUE(flushed);
    EXPECT_EQ(stats.queued_records, 4U);
    EXPECT_EQ(stats.sent_records, 4U)
        << "last_error=" << stats.last_error << " stage=" << stats.last_error_stage;
    EXPECT_EQ(stats.dropped_records, 0U)
        << "last_error=" << stats.last_error << " stage=" << stats.last_error_stage;
    EXPECT_TRUE(server_done.load(std::memory_order_acquire));
    EXPECT_NE(received.find("runtime tcp async logger one"), std::string::npos);
    EXPECT_NE(received.find("runtime tcp async logger two"), std::string::npos);
    EXPECT_NE(received.find("runtime tcp async logger three"), std::string::npos);
    EXPECT_NE(received.find("runtime tcp async logger four"), std::string::npos);
#endif
}
