#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <new>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

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

static_assert(alignof(af::detail::CacheLineAtomic<bool>) == af::detail::hardware_cache_line_size);
static_assert(sizeof(af::detail::CacheLineAtomic<bool>) >= af::detail::hardware_cache_line_size);
static_assert(alignof(af::detail::CacheLineAtomic<std::uint64_t>) ==
              af::detail::hardware_cache_line_size);
static_assert(sizeof(af::detail::CacheLineAtomic<std::uint64_t>) >=
              af::detail::hardware_cache_line_size);

[[nodiscard]] std::string read_file(const std::filesystem::path &path) {
    std::ifstream input(path);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

[[nodiscard]] std::size_t count_substring_occurrences(std::string_view text,
                                                      std::string_view needle) {
    std::size_t count = 0;
    std::size_t position = 0;
    while ((position = text.find(needle, position)) != std::string_view::npos) {
        ++count;
        position += needle.size();
    }
    return count;
}

TEST(LogTests, TaskIdTagIsInsertedAtFirstUserLogField) {
    const std::string message = "I0603 10:31:40.550430 123 log_tests.cpp:42] user first\nsecond\n";
    const std::string user_message = "user first\nsecond\n";
    const std::string tagged =
        af::detail::task_id_tagged_user_log_message(message, user_message, 1025);

    EXPECT_EQ(tagged,
              "I0603 10:31:40.550430 123 log_tests.cpp:42] [task=1025] user first\nsecond\n");
    EXPECT_EQ(count_substring_occurrences(tagged, "[task=1025] "), 1U);
    EXPECT_EQ(tagged.find("[task=1025] "), message.size() - user_message.size());
}

TEST(LogTests, TaskIdTagKeepsOriginalMessageWhenUserLogFieldCannotBeLocated) {
    const std::string message = "I0603 10:31:40.550430 123 log_tests.cpp:42] user first\n";
    const std::string tagged =
        af::detail::task_id_tagged_user_log_message(message, "different user field\n", 1025);

    EXPECT_EQ(tagged, message);
}

#if !defined(_WIN32)
TEST(LogTests, LogSocketNonblockingHelperReportsInvalidFd) {
    EXPECT_FALSE(af::detail::set_log_socket_nonblocking(-1));
}
#endif

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

class CapturingLogBackend final : public af::LogBackend {
public:
    void write_batch(std::span<af::detail::LogRecord *const> records) noexcept override {
        std::lock_guard lock(mutex_);
        for (af::detail::LogRecord *record : records) {
            messages_.emplace_back(record->message());
        }
    }

    [[nodiscard]] std::vector<std::string> messages() const {
        std::lock_guard lock(mutex_);
        return messages_;
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::string> messages_;
};

template <typename RuntimeT> class RuntimeThreadObservingLogBackend final : public af::LogBackend {
public:
    explicit RuntimeThreadObservingLogBackend(typename RuntimeT::Thread expected_thread)
        : expected_thread_index_(RuntimeT::thread_index(expected_thread)) {}

    void write_batch(std::span<af::detail::LogRecord *const> records) noexcept override {
        record_count_.fetch_add(records.size(), std::memory_order_relaxed);
        const bool on_runtime_thread = RuntimeT::is_runtime_thread();
        ran_on_runtime_thread_.store(on_runtime_thread, std::memory_order_release);
        if (on_runtime_thread) {
            observed_thread_index_.store(RuntimeT::current_thread_index(),
                                         std::memory_order_release);
        }
    }

    [[nodiscard]] std::size_t record_count() const noexcept {
        return record_count_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool ran_on_runtime_thread() const noexcept {
        return ran_on_runtime_thread_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint16_t observed_thread_index() const noexcept {
        return observed_thread_index_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint16_t expected_thread_index() const noexcept {
        return expected_thread_index_;
    }

private:
    const std::uint16_t expected_thread_index_;
    std::atomic<std::size_t> record_count_{0};
    std::atomic<bool> ran_on_runtime_thread_{false};
    std::atomic<std::uint16_t> observed_thread_index_{RuntimeT::invalid_thread_index};
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

[[nodiscard]] std::string extract_absl_prefix_thread_id(std::string_view line) {
    const std::size_t prefix_end = line.find(']');
    if (prefix_end == std::string_view::npos) {
        return {};
    }

    std::istringstream tokens(std::string(line.substr(0, prefix_end)));
    std::string previous;
    std::string current;
    for (std::string token; tokens >> token;) {
        previous = std::move(current);
        current = std::move(token);
    }
    return previous;
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
        af::thread_group<LogTestRuntimeThreadTag, 2, af::ThreadKind::Worker, "log-src">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
};

using LogTestRuntime = af::AsyncRuntime<LogTestRuntimeTraits>;
using LogTestTaskBase = LogTestRuntime::Task;

struct LogTestThreads {
    static constexpr auto Runtime_0 =
        LogTestRuntime::thread_group<LogTestRuntimeThreadTag>().template at<0>();
    static constexpr auto Runtime_1 =
        LogTestRuntime::thread_group<LogTestRuntimeThreadTag>().template at<1>();
};

template <typename RuntimeT> class ScopedRuntimeLogConsumer {
public:
    using Thread = typename RuntimeT::Thread;

    ScopedRuntimeLogConsumer(std::shared_ptr<af::AsyncLogger> logger, Thread thread,
                             std::size_t max_batches_per_run)
        : controller_(std::make_unique<af::detail::RuntimeAsyncLogConsumerController<RuntimeT>>(
              std::move(logger), thread, max_batches_per_run)) {}

    ScopedRuntimeLogConsumer(const ScopedRuntimeLogConsumer &) = delete;
    ScopedRuntimeLogConsumer &operator=(const ScopedRuntimeLogConsumer &) = delete;

    ~ScopedRuntimeLogConsumer() {
        shutdown();
    }

    [[nodiscard]] bool start() noexcept {
        return controller_ != nullptr && controller_->start();
    }

    void shutdown() noexcept {
        if (controller_ != nullptr) {
            controller_->shutdown();
            controller_.reset();
        }
    }

private:
    std::unique_ptr<af::detail::RuntimeAsyncLogConsumerController<RuntimeT>> controller_;
};

class LogTestRuntimeGuard {
public:
    LogTestRuntimeGuard() {
        LogTestRuntime::init();
    }

    ~LogTestRuntimeGuard() {
        LogTestRuntime::shutdown();
    }
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

class RuntimeThreadIdProbeLogTask final : public LogTestTaskBase {
public:
    explicit RuntimeThreadIdProbeLogTask(LogTestTaskBase::FactoryToken token)
        : LogTestTaskBase(token) {}

    bool do_it(LogTestRuntime::Thread target, std::atomic<int> *completed, const char *marker) {
        completed_ = completed;
        marker_ = marker;
        return schedule(target);
    }

private:
    af::TaskResult run() override {
        LOG(INFO) << marker_;
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    const char *marker_{nullptr};
};

class RuntimeTaskIdLogTask final : public LogTestTaskBase {
public:
    explicit RuntimeTaskIdLogTask(LogTestTaskBase::FactoryToken token) : LogTestTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<LogTestTaskBase::TaskId> *observed_task_id,
               std::atomic<LogTestTaskBase::TaskId> *observed_current_task_id,
               const char *message = "runtime task id log") {
        completed_ = completed;
        observed_task_id_ = observed_task_id;
        observed_current_task_id_ = observed_current_task_id;
        message_ = message;
        return schedule(LogTestThreads::Runtime_0);
    }

private:
    af::TaskResult run() override {
        observed_task_id_->store(task_id(), std::memory_order_release);
        observed_current_task_id_->store(LogTestRuntime::current_task_id(),
                                         std::memory_order_release);
        LOG(INFO) << message_;
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<LogTestTaskBase::TaskId> *observed_task_id_{nullptr};
    std::atomic<LogTestTaskBase::TaskId> *observed_current_task_id_{nullptr};
    const char *message_{"runtime task id log"};
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

struct DefaultConsumerLogicThreadTag;
struct DefaultConsumerIoThreadTag;

struct LogDefaultIoRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<DefaultConsumerLogicThreadTag, 1, af::ThreadKind::Worker, "log-def-cpu">(),
        af::thread_group<DefaultConsumerIoThreadTag, 1, af::ThreadKind::Io, "log-def-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
};

using LogDefaultIoRuntime = af::AsyncRuntime<LogDefaultIoRuntimeTraits>;

struct LogDefaultIoThreads {
    static constexpr auto IO_0 =
        LogDefaultIoRuntime::thread_group<DefaultConsumerIoThreadTag>().template at<0>();
};

static_assert(af::default_async_log_consumer_thread<LogDefaultIoRuntime>() ==
              LogDefaultIoThreads::IO_0);

struct DefaultConsumerWorkerThreadTag;
struct DefaultConsumerLogThreadTag;

struct LogDefaultLogRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<DefaultConsumerWorkerThreadTag, 1, af::ThreadKind::Worker,
                         "log-pref-cpu">(),
        af::thread_group<DefaultConsumerIoThreadTag, 1, af::ThreadKind::Io, "log-pref-io">(),
        af::thread_group<DefaultConsumerLogThreadTag, 1, af::ThreadKind::Log, "log-pref-log">());
};

using LogDefaultLogRuntime = af::AsyncRuntime<LogDefaultLogRuntimeTraits>;

struct LogDefaultLogThreads {
    static constexpr auto LOG_0 =
        LogDefaultLogRuntime::thread_group<DefaultConsumerLogThreadTag>().template at<0>();
};

static_assert(af::default_async_log_consumer_thread<LogDefaultLogRuntime>() ==
              LogDefaultLogThreads::LOG_0);

class LogDefaultIoRuntimeGuard {
public:
    LogDefaultIoRuntimeGuard() {
        LogDefaultIoRuntime::init();
    }

    ~LogDefaultIoRuntimeGuard() {
        LogDefaultIoRuntime::shutdown();
    }
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
    LogTestRuntimeGuard runtime_guard;
    auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(std::move(config),
                                                                       LogTestThreads::Runtime_1);

    LOG(INFO) << "af async file backend test";

    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
    logging.reset();

    const std::string contents = read_file(path);
    EXPECT_NE(contents.find("af async file backend test"), std::string::npos);
}

TEST(LogTests, RuntimeAsyncLoggingRejectsInvalidConsumerThread) {
    LogTestRuntimeGuard runtime_guard;

    EXPECT_THROW(
        {
            af::AsyncLogConfig config;
            config.backends.push_back(std::make_unique<CountingLogBackend>());
            auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(
                std::move(config),
                LogTestRuntime::thread_from_index(LogTestRuntime::invalid_thread_index));
            static_cast<void>(logging);
        },
        std::runtime_error);
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

TEST(LogTests, RuntimeFileBackendSkipsEmptyRecordsWithoutLeakingBatch) {
    const auto path =
        std::filesystem::path(::testing::TempDir()) / "asyncflow-runtime-log-empty.log";
    std::filesystem::remove(path);

    LogUdpIoRuntimeGuard runtime_guard;
    af::RuntimeFileLogBackend<LogUdpIoRuntime> backend({
        .thread = LogUdpIoThreads::IO_0,
        .path = path,
        .append = false,
        .batch_queue_capacity = 1,
        .max_batch_records = 1,
        .max_batches_per_run = 1,
    });

    af::detail::LogRecord empty_record;
    empty_record.reset("");
    std::array<af::detail::LogRecord *, 1> empty_ptrs{&empty_record};

    backend.write_batch(
        std::span<af::detail::LogRecord *const>(empty_ptrs.data(), empty_ptrs.size()));
    ASSERT_TRUE(backend.flush(std::chrono::seconds(2)));

    af::RuntimeFileLogBackendStats stats = backend.stats();
    EXPECT_EQ(stats.queued_records, 0U);
    EXPECT_EQ(stats.written_records, 0U);
    EXPECT_EQ(stats.dropped_records, 0U);

    af::detail::LogRecord record;
    record.reset("runtime file backend after empty\n");
    std::array<af::detail::LogRecord *, 1> record_ptrs{&record};

    backend.write_batch(
        std::span<af::detail::LogRecord *const>(record_ptrs.data(), record_ptrs.size()));
    ASSERT_TRUE(backend.flush(std::chrono::seconds(2)));
    backend.shutdown();

    stats = backend.stats();
    EXPECT_EQ(stats.queued_records, 1U);
    EXPECT_EQ(stats.written_records, 1U);
    EXPECT_EQ(stats.dropped_records, 0U);
    EXPECT_NE(read_file(path).find("runtime file backend after empty\n"), std::string::npos);
}

TEST(LogTests, RuntimeLogBackendRejectsInvalidRuntimeThread) {
    LogUdpIoRuntimeGuard runtime_guard;

    EXPECT_THROW(
        {
            const auto path = std::filesystem::path(::testing::TempDir()) /
                              "asyncflow-runtime-log-invalid-thread.log";
            af::RuntimeFileLogBackend<LogUdpIoRuntime> backend({
                .thread = LogUdpIoRuntime::thread_from_index(LogUdpIoRuntime::invalid_thread_index),
                .path = path,
            });
            static_cast<void>(backend);
        },
        std::runtime_error);
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
TEST(LogTests, RuntimeAwareSinkUsesConfiguredRuntimeThreadName) {
    auto backend = std::make_unique<ThreadNameLogBackend>();
    auto *thread_name_backend = backend.get();

    af::AsyncLogConfig config;
    config.backends.push_back(std::move(backend));

    LogTestRuntimeGuard runtime_guard;
    auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(std::move(config),
                                                                       LogTestThreads::Runtime_1);
    LOG(INFO) << "named runtime log consumer thread";
    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
    logging->stop();

    EXPECT_EQ(thread_name_backend->thread_name(), "af-log-src-1");
}
#endif

TEST(LogTests, RuntimeAwareSinkKeepsProducerThreadIdInAbslPrefix) {
    auto backend = std::make_unique<CapturingLogBackend>();
    auto *capturing_backend = backend.get();

    af::AsyncLogConfig config;
    config.backends.push_back(std::move(backend));

    LogTestRuntimeGuard runtime_guard;
    auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(std::move(config),
                                                                       LogTestThreads::Runtime_1);

    std::atomic<int> completed{0};
    ASSERT_TRUE(LogTestRuntime::start_task<RuntimeThreadIdProbeLogTask>(
        LogTestThreads::Runtime_0, &completed, "producer-runtime-zero"));
    ASSERT_TRUE(LogTestRuntime::start_task<RuntimeThreadIdProbeLogTask>(
        LogTestThreads::Runtime_1, &completed, "producer-runtime-one"));
    ASSERT_TRUE(wait_until_at_least(completed, 2));
    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
    logging->stop();

    std::string runtime_zero_tid;
    std::string runtime_one_tid;
    for (const std::string &message : capturing_backend->messages()) {
        if (message.find("producer-runtime-zero") != std::string::npos) {
            runtime_zero_tid = extract_absl_prefix_thread_id(message);
        }
        if (message.find("producer-runtime-one") != std::string::npos) {
            runtime_one_tid = extract_absl_prefix_thread_id(message);
        }
    }

    ASSERT_FALSE(runtime_zero_tid.empty());
    ASSERT_FALSE(runtime_one_tid.empty());
    EXPECT_NE(runtime_zero_tid, runtime_one_tid);
}

TEST(LogTests, ProducerShardCacheRefreshesWhenLoggerReusesAddress) {
    alignas(af::AsyncLogger) unsigned char storage[sizeof(af::AsyncLogger)];
    LogTestRuntimeGuard runtime_guard;

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
    {
        std::shared_ptr<af::AsyncLogger> first_logger(first, [](af::AsyncLogger *) {});
        ScopedRuntimeLogConsumer<LogTestRuntime> first_consumer(first_logger,
                                                                LogTestThreads::Runtime_1, 64);
        ASSERT_TRUE(first_consumer.start());
        ASSERT_TRUE(first->try_log("first logger record\n"));
        ASSERT_TRUE(first->flush(std::chrono::seconds(2)));
        EXPECT_EQ(first_counter->record_count(), 1U);
        first_consumer.shutdown();
    }
    first->~AsyncLogger();

    CountingLogBackend *second_counter = nullptr;
    af::AsyncLogger *second =
        ::new (static_cast<void *>(storage)) af::AsyncLogger(make_config(8, second_counter));
    {
        std::shared_ptr<af::AsyncLogger> second_logger(second, [](af::AsyncLogger *) {});
        ScopedRuntimeLogConsumer<LogTestRuntime> second_consumer(second_logger,
                                                                 LogTestThreads::Runtime_1, 64);
        ASSERT_TRUE(second_consumer.start());
        ASSERT_TRUE(second->try_log("second logger record\n"));
        ASSERT_TRUE(second->flush(std::chrono::seconds(2)));
        EXPECT_EQ(second_counter->record_count(), 1U);
        second_consumer.shutdown();
    }
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
    LogTestRuntimeGuard runtime_guard;
    auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(std::move(config),
                                                                       LogTestThreads::Runtime_1);

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
    LogTestRuntimeGuard runtime_guard;

    auto backend = std::make_unique<BlockingLogBackend>();
    auto *blocking_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 1;
    config.queue_shard_count = 1;
    config.runtime_queue_capacity = 2;
    config.max_batch_size = 1;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.backends.push_back(std::move(backend));
    auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(std::move(config),
                                                                       LogTestThreads::Runtime_1);

    LOG(INFO) << "block log worker";
    ASSERT_TRUE(blocking_backend->wait_until_entered(std::chrono::seconds(2)));

    LOG(INFO) << "external mpsc fill one";
    LOG(INFO) << "external mpsc fill two";
    const af::AsyncLogStats filled = logging->stats();

    LOG(INFO) << "external mpsc overflow";
    const af::AsyncLogStats overflowed = logging->stats();
    ASSERT_GT(overflowed.dropped, filled.dropped);

    std::atomic<int> runtime_completed{0};
    ASSERT_TRUE(LogTestRuntime::start_task<RuntimeLogTask>(&runtime_completed));
    ASSERT_TRUE(wait_until_at_least(runtime_completed, 1));

    const af::AsyncLogStats after_runtime = logging->stats();
    EXPECT_EQ(after_runtime.accepted, overflowed.accepted + 1U);

    blocking_backend->release();
    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
    logging->stop();
}

TEST(LogTests, RuntimeAwareSinkDrainsOnConfiguredRuntimeThread) {
    LogTestRuntimeGuard runtime_guard;

    auto backend = std::make_unique<RuntimeThreadObservingLogBackend<LogTestRuntime>>(
        LogTestThreads::Runtime_1);
    auto *observing_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 16;
    config.runtime_queue_capacity = 16;
    config.max_batch_size = 4;
    config.backends.push_back(std::move(backend));
    auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(std::move(config),
                                                                       LogTestThreads::Runtime_1);

    LOG(INFO) << "runtime-bound consumer external log";

    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
    logging->stop();

    EXPECT_EQ(observing_backend->record_count(), 1U);
    EXPECT_TRUE(observing_backend->ran_on_runtime_thread());
    EXPECT_EQ(observing_backend->observed_thread_index(),
              observing_backend->expected_thread_index());
}

TEST(LogTests, RuntimeAwareSinkDefaultConsumerPrefersIoThread) {
    LogDefaultIoRuntimeGuard runtime_guard;

    auto backend = std::make_unique<RuntimeThreadObservingLogBackend<LogDefaultIoRuntime>>(
        LogDefaultIoThreads::IO_0);
    auto *observing_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 16;
    config.runtime_queue_capacity = 16;
    config.max_batch_size = 4;
    config.backends.push_back(std::move(backend));
    auto logging = af::start_async_logging_for_runtime<LogDefaultIoRuntime>(std::move(config));

    LOG(INFO) << "default runtime-bound consumer external log";

    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
    logging->stop();

    EXPECT_EQ(observing_backend->record_count(), 1U);
    EXPECT_TRUE(observing_backend->ran_on_runtime_thread());
    EXPECT_EQ(observing_backend->observed_thread_index(),
              observing_backend->expected_thread_index());
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

    LogTestRuntimeGuard runtime_guard;
    auto logger = std::make_shared<af::AsyncLogger>(std::move(config));
    ScopedRuntimeLogConsumer<LogTestRuntime> consumer(logger, LogTestThreads::Runtime_1, 64);
    ASSERT_TRUE(consumer.start());

    constexpr int record_count = 32;
    for (int i = 0; i < record_count; ++i) {
        ASSERT_TRUE(logger->try_log_from_runtime_thread(0, "runtime pooled log record\n"));
        ASSERT_TRUE(logger->flush(std::chrono::seconds(2)));
    }

    const af::AsyncLogStats stats = logger->stats();
    EXPECT_EQ(stats.accepted, static_cast<std::uint64_t>(record_count));
    EXPECT_EQ(stats.dropped, 0U);
    EXPECT_EQ(counting_backend->record_count(), static_cast<std::size_t>(record_count));
    consumer.shutdown();
}

TEST(LogTests, SharedRecordPoolBatchReleaseReusesSlots) {
    af::detail::AsyncLogRecordPool pool(4);
    std::array<af::detail::LogRecord *, 4> records{};

    for (std::size_t i = 0; i < records.size(); ++i) {
        records[i] = pool.try_acquire("shared batch release log record\n");
        ASSERT_NE(records[i], nullptr);
    }
    EXPECT_EQ(pool.try_acquire("shared pool should be full\n"), nullptr);

    af::detail::release_async_log_records(
        std::span<af::detail::LogRecord *const>(records.data(), records.size()));

    for (std::size_t i = 0; i < records.size(); ++i) {
        records[i] = pool.try_acquire("shared batch release reused log record\n");
        ASSERT_NE(records[i], nullptr);
    }

    af::detail::release_async_log_records(
        std::span<af::detail::LogRecord *const>(records.data(), records.size()));
}

TEST(LogTests, SpscRecordPoolBatchReleaseReusesSlots) {
    af::detail::AsyncLogSpscRecordPool pool(4);
    std::array<af::detail::LogRecord *, 4> records{};

    for (std::size_t i = 0; i < records.size(); ++i) {
        records[i] = pool.try_acquire("spsc batch release log record\n");
        ASSERT_NE(records[i], nullptr);
    }
    EXPECT_EQ(pool.try_acquire("spsc pool should be full\n"), nullptr);

    af::detail::release_async_log_records(
        std::span<af::detail::LogRecord *const>(records.data(), records.size()));

    for (std::size_t i = 0; i < records.size(); ++i) {
        records[i] = pool.try_acquire("spsc batch release reused log record\n");
        ASSERT_NE(records[i], nullptr);
    }

    af::detail::release_async_log_records(
        std::span<af::detail::LogRecord *const>(records.data(), records.size()));
}

TEST(LogTests, RuntimeAwareSinkTagsFirstUserLogFieldWithRuntimeTaskId) {
    const auto path = std::filesystem::temp_directory_path() / "async_flow_task_id_log.txt";
    std::filesystem::remove(path);

    af::AsyncLogConfig config;
    config.queue_capacity = 16;
    config.runtime_queue_capacity = 16;
    config.backends.push_back(af::make_file_log_backend({.path = path, .append = false}));
    LogTestRuntimeGuard runtime_guard;
    auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(std::move(config),
                                                                       LogTestThreads::Runtime_0);
    std::atomic<int> runtime_completed{0};
    std::atomic<LogTestTaskBase::TaskId> observed_task_id{LogTestTaskBase::invalid_task_id};
    std::atomic<LogTestTaskBase::TaskId> observed_current_task_id{LogTestTaskBase::invalid_task_id};
    ASSERT_TRUE(LogTestRuntime::start_task<RuntimeTaskIdLogTask>(
        &runtime_completed, &observed_task_id, &observed_current_task_id));
    ASSERT_TRUE(wait_until_at_least(runtime_completed, 1));

    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
    logging->stop();

    const auto task_id = observed_task_id.load(std::memory_order_acquire);
    ASSERT_NE(task_id, LogTestTaskBase::invalid_task_id);
    EXPECT_EQ(observed_current_task_id.load(std::memory_order_acquire), task_id);

    const std::string contents = read_file(path);
    const std::string task_tag = "[task=" + std::to_string(task_id) + "] ";
    const std::size_t prefix_end = contents.find("] ");
    const std::size_t task_tag_pos = contents.find(task_tag);
    const std::size_t user_message_pos = contents.find("runtime task id log");
    ASSERT_NE(prefix_end, std::string::npos);
    ASSERT_NE(task_tag_pos, std::string::npos);
    ASSERT_NE(user_message_pos, std::string::npos);
    EXPECT_EQ(contents.find('I'), 0U);
    EXPECT_EQ(task_tag_pos, prefix_end + 2U);
    EXPECT_EQ(user_message_pos, task_tag_pos + task_tag.size());
    EXPECT_NE(contents.find(task_tag + "runtime task id log"), std::string::npos);
}

TEST(LogTests, RuntimeAwareSinkTagsOnlyFirstUserLogLine) {
    const auto path = std::filesystem::temp_directory_path() / "async_flow_multiline_task_log.txt";
    std::filesystem::remove(path);

    af::AsyncLogConfig config;
    config.queue_capacity = 16;
    config.runtime_queue_capacity = 16;
    config.backends.push_back(af::make_file_log_backend({.path = path, .append = false}));
    LogTestRuntimeGuard runtime_guard;
    auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(std::move(config),
                                                                       LogTestThreads::Runtime_0);
    std::atomic<int> runtime_completed{0};
    std::atomic<LogTestTaskBase::TaskId> observed_task_id{LogTestTaskBase::invalid_task_id};
    std::atomic<LogTestTaskBase::TaskId> observed_current_task_id{LogTestTaskBase::invalid_task_id};
    ASSERT_TRUE(LogTestRuntime::start_task<RuntimeTaskIdLogTask>(
        &runtime_completed, &observed_task_id, &observed_current_task_id,
        "runtime task id log first\nruntime task id log second"));
    ASSERT_TRUE(wait_until_at_least(runtime_completed, 1));

    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
    logging->stop();

    const auto task_id = observed_task_id.load(std::memory_order_acquire);
    ASSERT_NE(task_id, LogTestTaskBase::invalid_task_id);

    const std::string contents = read_file(path);
    const std::string task_tag = "[task=" + std::to_string(task_id) + "] ";
    const std::size_t task_tag_pos = contents.find(task_tag);
    const std::size_t first_user_line_pos = contents.find("runtime task id log first");
    ASSERT_NE(task_tag_pos, std::string::npos);
    ASSERT_NE(first_user_line_pos, std::string::npos);
    EXPECT_EQ(first_user_line_pos, task_tag_pos + task_tag.size());
    EXPECT_EQ(count_substring_occurrences(contents, task_tag), 1U);
    EXPECT_NE(contents.find("\nruntime task id log second"), std::string::npos);
    EXPECT_EQ(contents.find("\n" + task_tag + "runtime task id log second"), std::string::npos);
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

    LogTestRuntimeGuard runtime_guard;
    auto logger = std::make_shared<af::AsyncLogger>(std::move(config));
    ScopedRuntimeLogConsumer<LogTestRuntime> consumer(logger, LogTestThreads::Runtime_1, 64);
    ASSERT_TRUE(consumer.start());

    ASSERT_TRUE(logger->try_log("block log worker\n"));
    ASSERT_TRUE(blocking_backend->wait_until_entered(std::chrono::seconds(2)));
    ASSERT_TRUE(logger->try_log("queued log one\n"));
    ASSERT_TRUE(logger->try_log("queued log two\n"));

    std::atomic<bool> accepted{false};
    std::atomic<bool> finished{false};
    std::thread producer([&] {
        accepted.store(logger->try_log("wait for queue capacity\n"), std::memory_order_release);
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
        consumer.shutdown();
    }
    producer.join();

    EXPECT_TRUE(finished.load(std::memory_order_acquire));
    EXPECT_TRUE(accepted.load(std::memory_order_acquire));
    ASSERT_TRUE(logger->flush(std::chrono::seconds(2)));
    consumer.shutdown();
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

    LogTestRuntimeGuard runtime_guard;
    auto logger = std::make_shared<af::AsyncLogger>(std::move(config));
    ScopedRuntimeLogConsumer<LogTestRuntime> consumer(logger, LogTestThreads::Runtime_1, 64);
    ASSERT_TRUE(consumer.start());

    ASSERT_TRUE(logger->try_log("block log worker\n"));
    ASSERT_TRUE(blocking_backend->wait_until_entered(std::chrono::seconds(2)));

    std::array<std::thread, 4> producers;
    std::atomic<int> accepted{0};
    for (std::size_t i = 0; i < producers.size(); ++i) {
        producers[i] = std::thread([&logger, &accepted] {
            if (logger->try_log("producer shard log\n")) {
                accepted.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto &producer : producers) {
        producer.join();
    }

    EXPECT_EQ(accepted.load(std::memory_order_acquire), 4);
    EXPECT_EQ(logger->stats().dropped, 0U);

    blocking_backend->release();
    ASSERT_TRUE(logger->flush(std::chrono::seconds(2)));
    consumer.shutdown();
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

    LogTestRuntimeGuard runtime_guard;
    auto logger = std::make_shared<af::AsyncLogger>(std::move(config));
    ScopedRuntimeLogConsumer<LogTestRuntime> consumer(logger, LogTestThreads::Runtime_1, 64);
    ASSERT_TRUE(consumer.start());

    constexpr int producer_count = 8;
    constexpr int records_per_producer = 128;
    constexpr int expected_records = producer_count * records_per_producer;

    std::array<std::thread, producer_count> producers;
    std::atomic<int> accepted{0};
    for (int producer = 0; producer < producer_count; ++producer) {
        producers[producer] = std::thread([&logger, &accepted] {
            for (int i = 0; i < records_per_producer; ++i) {
                if (logger->try_log("concurrent sharded log\n")) {
                    accepted.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto &producer : producers) {
        producer.join();
    }

    ASSERT_TRUE(logger->flush(std::chrono::seconds(2)));
    const af::AsyncLogStats stats = logger->stats();
    EXPECT_EQ(accepted.load(std::memory_order_acquire), expected_records);
    EXPECT_EQ(stats.accepted, static_cast<std::uint64_t>(expected_records));
    EXPECT_EQ(stats.dropped, 0U);
    EXPECT_EQ(counting_backend->record_count(), static_cast<std::size_t>(expected_records));
    consumer.shutdown();
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

    LogTestRuntimeGuard runtime_guard;
    auto logger = std::make_shared<af::AsyncLogger>(std::move(config));
    ScopedRuntimeLogConsumer<LogTestRuntime> consumer(logger, LogTestThreads::Runtime_1, 64);
    ASSERT_TRUE(consumer.start());

    constexpr int record_count = 32;
    for (int i = 0; i < record_count; ++i) {
        ASSERT_TRUE(logger->try_log("reused pooled log record\n"));
        ASSERT_TRUE(logger->flush(std::chrono::seconds(2)));
    }

    const af::AsyncLogStats stats = logger->stats();
    EXPECT_EQ(stats.accepted, static_cast<std::uint64_t>(record_count));
    EXPECT_EQ(stats.dropped, 0U);
    EXPECT_EQ(counting_backend->record_count(), static_cast<std::size_t>(record_count));
    consumer.shutdown();
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

    EXPECT_TRUE(flushed) << "queued=" << stats.queued_records << " sent=" << stats.sent_records
                         << " dropped=" << stats.dropped_records
                         << " last_error=" << stats.last_error
                         << " stage=" << stats.last_error_stage
                         << " server_done=" << server_done.load(std::memory_order_acquire)
                         << " received_size=" << received.size();
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

    EXPECT_TRUE(flushed) << "queued=" << stats.queued_records << " sent=" << stats.sent_records
                         << " dropped=" << stats.dropped_records
                         << " last_error=" << stats.last_error
                         << " stage=" << stats.last_error_stage
                         << " server_done=" << server_done.load(std::memory_order_acquire)
                         << " received_size=" << received.size();
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
