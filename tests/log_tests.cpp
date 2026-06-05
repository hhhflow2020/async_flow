#include <array>
#include <atomic>
#include <charconv>
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
#include "af/span.hpp"
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "af/async_runtime.hpp"
#include "af/log.hpp"
#include "af/runtime.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

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

[[nodiscard]] bool parse_indexed_log_message(std::string_view message, std::string_view prefix,
                                             int *index) noexcept {
    if (message.size() <= prefix.size() || message.substr(0, prefix.size()) != prefix) {
        return false;
    }

    const std::string_view digits = message.substr(prefix.size());
    int parsed = -1;
    const auto *begin = digits.data();
    const auto *end = digits.data() + digits.size();
    const auto result = std::from_chars(begin, end, parsed);
    if (result.ec != std::errc{} || result.ptr != end || parsed < 0) {
        return false;
    }

    *index = parsed;
    return true;
}

void expect_exact_indexed_log_set(const std::vector<std::string> &messages, std::string_view prefix,
                                  int expected_count) {
    ASSERT_EQ(messages.size(), static_cast<std::size_t>(expected_count));

    std::vector<int> seen(static_cast<std::size_t>(expected_count), 0);
    for (const std::string &message : messages) {
        int index = -1;
        ASSERT_TRUE(parse_indexed_log_message(message, prefix, &index)) << message;
        ASSERT_LT(index, expected_count) << message;
        ++seen[static_cast<std::size_t>(index)];
    }

    for (int index = 0; index < expected_count; ++index) {
        EXPECT_EQ(seen[static_cast<std::size_t>(index)], 1) << "index=" << index;
    }
}

[[nodiscard]] std::size_t line_begin_for_position(std::string_view text,
                                                  std::size_t position) noexcept {
    const std::size_t newline = text.rfind('\n', position);
    return newline == std::string_view::npos ? 0U : newline + 1U;
}

[[nodiscard]] std::string_view line_for_position(std::string_view text,
                                                 std::size_t position) noexcept {
    const std::size_t begin = line_begin_for_position(text, position);
    const std::size_t newline = text.find('\n', begin);
    const std::size_t end = newline == std::string_view::npos ? text.size() : newline + 1U;
    return text.substr(begin, end - begin);
}

TEST(LogTests, TaskIdTagIsInsertedAtFirstUserLogField) {
    const std::string message = "I0603 10:31:40.550430 123 log_tests.cpp:42] user first\nsecond\n";
    const std::string user_message = "user first\nsecond\n";
    const std::string tagged =
        af::detail::task_id_tagged_user_log_message(message, user_message, 1025);

    EXPECT_EQ(tagged, "I0603 10:31:40.550430 123 log_tests.cpp:42] [task=1025] user first\n"
                      "I0603 10:31:40.550430 123 log_tests.cpp:42] [task=1025] second\n");
    EXPECT_EQ(count_substring_occurrences(tagged, "[task=1025] "), 2U);
    EXPECT_EQ(tagged.find("[task=1025] "), message.size() - user_message.size());
    EXPECT_EQ(tagged.find("\n[task=1025] "), std::string::npos);

    const std::string_view first_line = line_for_position(tagged, tagged.find("user first"));
    const std::string_view second_line = line_for_position(tagged, tagged.find("second"));
    const std::size_t first_task_tag_pos = first_line.find("[task=1025] ");
    const std::size_t second_task_tag_pos = second_line.find("[task=1025] ");
    ASSERT_NE(first_task_tag_pos, std::string_view::npos);
    ASSERT_NE(second_task_tag_pos, std::string_view::npos);
    EXPECT_NE(second_task_tag_pos, 0U);
    EXPECT_EQ(first_line.substr(0, first_task_tag_pos), second_line.substr(0, second_task_tag_pos));
}

TEST(LogTests, TaskIdTagUsesSharedLogEntryUserFieldStart) {
    const std::string prefix = "I0603 10:31:40.550430 123 log_tests.cpp:42] ";
    const std::string message = prefix + "user first\nsecond\n";
    const std::string_view formatted_message(message);
    const std::string_view user_message(formatted_message.data() + prefix.size(),
                                        formatted_message.size() - prefix.size());
    const std::string tagged =
        af::detail::task_id_tagged_user_log_message(formatted_message, user_message, 1025);

    EXPECT_EQ(tagged, prefix + "[task=1025] user first\n" + prefix + "[task=1025] second\n");
    EXPECT_NE(tagged.find("[task=1025] "), 0U);
    EXPECT_EQ(count_substring_occurrences(tagged, "[task=1025] "), 2U);
    EXPECT_EQ(tagged.find("\n[task=1025] "), std::string::npos);
}

TEST(LogTests, TaskIdTagKeepsOriginalMessageWhenUserLogFieldCannotBeLocated) {
    const std::string message = "I0603 10:31:40.550430 123 log_tests.cpp:42] user first\n";
    const std::string tagged =
        af::detail::task_id_tagged_user_log_message(message, "different user field\n", 1025);

    EXPECT_EQ(tagged, message);
}

TEST(LogTests, LogSocketNonblockingHelperReportsInvalidFd) {
    EXPECT_FALSE(af::detail::set_log_socket_nonblocking(-1));
}

TEST(LogTests, AsyncLogConfigProfilesSelectQueueStrategy) {
    af::AsyncLogConfig ordered = af::AsyncLogConfig::ordered();
    ordered.use_ordered(4);
    EXPECT_EQ(ordered.ordering, af::LogOrdering::Ordered);
    EXPECT_EQ(ordered.queue_shard_count, 4U);
    EXPECT_EQ(ordered.runtime_thread_count, 0U);

    const af::AsyncLogConfig ordered_factory = af::AsyncLogConfig::ordered(3);
    EXPECT_EQ(ordered_factory.ordering, af::LogOrdering::Ordered);
    EXPECT_EQ(ordered_factory.queue_shard_count, 3U);
    EXPECT_EQ(ordered_factory.runtime_thread_count, 0U);

    af::AsyncLogConfig relaxed = af::AsyncLogConfig::relaxed();
    relaxed.use_relaxed(8, 16);
    EXPECT_EQ(relaxed.ordering, af::LogOrdering::Relaxed);
    EXPECT_EQ(relaxed.runtime_thread_count, 8U);
    EXPECT_EQ(relaxed.queue_shard_count, 16U);

    const af::AsyncLogConfig relaxed_factory = af::AsyncLogConfig::relaxed(6, 12);
    EXPECT_EQ(relaxed_factory.ordering, af::LogOrdering::Relaxed);
    EXPECT_EQ(relaxed_factory.runtime_thread_count, 6U);
    EXPECT_EQ(relaxed_factory.queue_shard_count, 12U);

    relaxed.use_ordered(2);
    EXPECT_EQ(relaxed.ordering, af::LogOrdering::Ordered);
    EXPECT_EQ(relaxed.queue_shard_count, 2U);
    EXPECT_EQ(relaxed.runtime_thread_count, 0U);

    ordered.use_relaxed();
    EXPECT_EQ(ordered.ordering, af::LogOrdering::Relaxed);
    EXPECT_EQ(ordered.runtime_thread_count, af::AsyncLogConfig::auto_runtime_thread_count);
    EXPECT_EQ(ordered.queue_shard_count, af::AsyncLogConfig::auto_queue_shard_count);
    EXPECT_EQ(ordered.record_pool_slab_object_count, 0U);
}

TEST(LogTests, RuntimeLogConfigCarriesRecordPoolSlabSize) {
    af::log_config source = af::log_config::relaxed();
    source.queue_capacity = 16;
    source.queue_shard_count = 2;
    source.runtime_thread_count = 3;
    source.max_batch_records = 4;
    source.record_pool.slab_object_count = 7;

    const af::AsyncLogConfig target = af::make_async_log_config(source, 5);

    EXPECT_EQ(target.ordering, af::LogOrdering::Relaxed);
    EXPECT_EQ(target.queue_capacity, 16U);
    EXPECT_EQ(target.queue_shard_count, 2U);
    EXPECT_EQ(target.runtime_thread_count, 3U);
    EXPECT_EQ(target.max_batch_size, 4U);
    EXPECT_EQ(target.record_pool_slab_object_count, 7U);
}

class BlockingLogBackend final : public af::LogBackend {
public:
    void write_batch(af::Span<af::detail::LogRecord *const> records) noexcept override {
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
    void write_batch(af::Span<af::detail::LogRecord *const> records) noexcept override {
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
    void write_batch(af::Span<af::detail::LogRecord *const> records) noexcept override {
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

    void write_batch(af::Span<af::detail::LogRecord *const> records) noexcept override {
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

class RuntimeInstanceThreadObservingLogBackend final : public af::LogBackend {
public:
    RuntimeInstanceThreadObservingLogBackend(af::runtime &owner,
                                             af::runtime::thread_index expected_thread)
        : owner_(owner), expected_thread_index_(expected_thread) {}

    void write_batch(af::Span<af::detail::LogRecord *const> records) noexcept override {
        record_count_.fetch_add(records.size(), std::memory_order_relaxed);
        const bool on_runtime_thread = af::runtime::current() == &owner_;
        ran_on_runtime_thread_.store(on_runtime_thread, std::memory_order_release);
        if (on_runtime_thread) {
            observed_thread_index_.store(af::runtime::current_thread_index(),
                                         std::memory_order_release);
        }
    }

    [[nodiscard]] std::size_t record_count() const noexcept {
        return record_count_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool ran_on_runtime_thread() const noexcept {
        return ran_on_runtime_thread_.load(std::memory_order_acquire);
    }

    [[nodiscard]] af::runtime::thread_index observed_thread_index() const noexcept {
        return observed_thread_index_.load(std::memory_order_acquire);
    }

    [[nodiscard]] af::runtime::thread_index expected_thread_index() const noexcept {
        return expected_thread_index_;
    }

private:
    af::runtime &owner_;
    const af::runtime::thread_index expected_thread_index_;
    std::atomic<std::size_t> record_count_{0};
    std::atomic<bool> ran_on_runtime_thread_{false};
    std::atomic<af::runtime::thread_index> observed_thread_index_{af::runtime_invalid_thread_index};
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
    void write_batch(af::Span<af::detail::LogRecord *const> records) noexcept override {
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
        af::thread_group<LogTestRuntimeThreadTag, 2, af::thread_kind::cpu>("log-src"));
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
        LOG(INFO) << "runtime lane log";
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

class RuntimeStopLoggingTask final : public LogTestTaskBase {
public:
    explicit RuntimeStopLoggingTask(LogTestTaskBase::FactoryToken token) : LogTestTaskBase(token) {}

    bool do_it(LogTestRuntime::Thread target, af::AsyncLogHandle *logging,
               std::atomic<int> *completed) {
        logging_ = logging;
        completed_ = completed;
        return schedule(target);
    }

private:
    af::TaskResult run() override {
        LOG(INFO) << "runtime owner stops async logging";
        logging_->stop();
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::AsyncLogHandle *logging_{nullptr};
    std::atomic<int> *completed_{nullptr};
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

class RuntimeInstanceTaskIdLogTask final : public af::runtime_task {
public:
    RuntimeInstanceTaskIdLogTask(af::runtime_task::factory_token token, af::runtime &owner,
                                 std::atomic<int> &completed)
        : af::runtime_task(token, owner), completed_(completed) {}

    bool do_it(std::uint16_t target) noexcept {
        return schedule_to(target);
    }

private:
    af::task_result run_task() noexcept override {
        LOG(INFO) << "runtime instance task id disabled log";
        completed_.fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> &completed_;
};

struct LogUdpIoThreadTag;

inline constexpr af::thread_kind log_udp_io_thread_kind = af::thread_kind::io;

struct LogUdpIoRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<LogUdpIoThreadTag, 1, log_udp_io_thread_kind>("log-udp-io"));
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
        af::thread_group<DefaultConsumerLogicThreadTag, 1, af::thread_kind::cpu>("log-def-cpu"),
        af::thread_group<DefaultConsumerIoThreadTag, 1, af::thread_kind::io>("log-def-io"));
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
        af::thread_group<DefaultConsumerWorkerThreadTag, 1, af::thread_kind::cpu>("log-pref-cpu"),
        af::thread_group<DefaultConsumerIoThreadTag, 1, af::thread_kind::io>("log-pref-io"),
        af::thread_group<DefaultConsumerLogThreadTag, 1, af::thread_kind::cpu>("log-pref-log"));
};

using LogDefaultLogRuntime = af::AsyncRuntime<LogDefaultLogRuntimeTraits>;

struct LogDefaultLogThreads {
    static constexpr auto IO_0 =
        LogDefaultLogRuntime::thread_group<DefaultConsumerIoThreadTag>().template at<0>();
};

static_assert(af::default_async_log_consumer_thread<LogDefaultLogRuntime>() ==
              LogDefaultLogThreads::IO_0);

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
        af::Span<af::detail::LogRecord *const>(record_ptrs.data(), record_ptrs.size()));
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
        af::Span<af::detail::LogRecord *const>(empty_ptrs.data(), empty_ptrs.size()));
    ASSERT_TRUE(backend.flush(std::chrono::seconds(2)));

    af::RuntimeFileLogBackendStats stats = backend.stats();
    EXPECT_EQ(stats.queued_records, 0U);
    EXPECT_EQ(stats.written_records, 0U);
    EXPECT_EQ(stats.dropped_records, 0U);

    af::detail::LogRecord record;
    record.reset("runtime file backend after empty\n");
    std::array<af::detail::LogRecord *, 1> record_ptrs{&record};

    backend.write_batch(
        af::Span<af::detail::LogRecord *const>(record_ptrs.data(), record_ptrs.size()));
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

        af::AsyncLogConfig config = af::AsyncLogConfig::relaxed(0U, shard_count);
        config.queue_capacity = 8;
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

TEST(LogTests, RuntimeAwareSinkUsesRuntimeLaneWhenExternalMpscIsFull) {
    LogTestRuntimeGuard runtime_guard;

    auto backend = std::make_unique<BlockingLogBackend>();
    auto *blocking_backend = backend.get();

    af::AsyncLogConfig config = af::AsyncLogConfig::relaxed(0U, 1U);
    config.queue_capacity = 1;
    config.runtime_lane_capacity = 2;
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

TEST(LogTests, RuntimeInstanceAwareSinkDrainsOnConfiguredRuntimeThread) {
    af::runtime_config runtime_config;
    runtime_config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 1),
    };
    runtime_config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(runtime_config);
    ASSERT_TRUE(runtime.start());

    const auto consumer_thread = runtime.select_thread(af::thread_selector::cpu(0));
    auto backend =
        std::make_unique<RuntimeInstanceThreadObservingLogBackend>(runtime, consumer_thread);
    auto *observing_backend = backend.get();

    af::AsyncLogConfig log_config;
    log_config.queue_capacity = 16;
    log_config.max_batch_size = 4;
    log_config.backends.push_back(std::move(backend));
    auto logging =
        af::start_async_logging_for_runtime(runtime, std::move(log_config), consumer_thread);

    LOG(INFO) << "runtime instance-bound consumer external log";

    ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
    logging->stop(std::chrono::milliseconds(250));
    runtime.stop();

    EXPECT_EQ(observing_backend->record_count(), 1U);
    EXPECT_TRUE(observing_backend->ran_on_runtime_thread());
    EXPECT_EQ(observing_backend->observed_thread_index(),
              observing_backend->expected_thread_index());
}

TEST(LogTests, RuntimeInstanceAsyncLoggingUsesStructuredLoggerConfig) {
    const auto path =
        std::filesystem::path(::testing::TempDir()) / "asyncflow-runtime-config-log.log";
    std::filesystem::remove(path);

    af::runtime_config runtime_config;
    runtime_config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 1),
    };
    runtime_config.logger = af::log_config::ordered();
    runtime_config.logger.consumer_thread = af::thread_selector::cpu(0);
    runtime_config.logger.queue_capacity = 16;
    runtime_config.logger.queue_shard_count = 1;
    runtime_config.logger.max_batch_records = 4;
    runtime_config.logger.max_batch_delay = std::chrono::microseconds(500);
    runtime_config.shutdown.log_flush_timeout = std::chrono::seconds(1);
    runtime_config.logger.backends = {
        af::file_log_backend_config{path.string(), false, false, 8},
    };

    af::runtime runtime(runtime_config);
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(runtime.logger_started());

    LOG(INFO) << "runtime config async logger";

    ASSERT_TRUE(runtime.flush_logger(std::chrono::seconds(2)));
    runtime.stop();
    EXPECT_FALSE(runtime.logger_started());

    const std::string contents = read_file(path);
    EXPECT_NE(contents.find("runtime config async logger"), std::string::npos);
    std::filesystem::remove(path);
}

TEST(LogTests, RuntimeInstanceTaskIdDiagnosticsCanDisableLogTag) {
    const auto path =
        std::filesystem::path(::testing::TempDir()) / "asyncflow-runtime-task-id-disabled.log";
    std::filesystem::remove(path);

    af::runtime_config runtime_config;
    runtime_config.threads = {
        af::cpu_threads("logic", 1),
    };
    runtime_config.diagnostics.enable_task_id = false;
    runtime_config.logger.consumer_thread = af::thread_selector::cpu(0);
    runtime_config.logger.queue_capacity = 16;
    runtime_config.logger.max_batch_records = 4;
    runtime_config.logger.backends = {
        af::file_log_backend_config{path.string(), false, false, 8},
    };

    af::runtime runtime(runtime_config);
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(runtime.logger_started());

    std::atomic<int> completed{0};
    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    auto task = af::make_task<RuntimeInstanceTaskIdLogTask>(runtime, completed);
    EXPECT_EQ(task->task_id(), af::runtime_invalid_task_id);
    ASSERT_TRUE(task->do_it(cpu_thread));
    task.reset();

    ASSERT_TRUE(wait_until_at_least(completed, 1));
    ASSERT_TRUE(runtime.flush_logger(std::chrono::seconds(2)));
    runtime.stop();

    const std::string contents = read_file(path);
    EXPECT_NE(contents.find("runtime instance task id disabled log"), std::string::npos);
    EXPECT_EQ(contents.find("[task="), std::string::npos);
    std::filesystem::remove(path);
}

TEST(LogTests, RuntimeInstanceOwnedAsyncLoggerDrainsWhenRuntimeStops) {
    const auto path =
        std::filesystem::path(::testing::TempDir()) / "asyncflow-runtime-owned-log.log";
    std::filesystem::remove(path);

    af::runtime_config runtime_config;
    runtime_config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 1),
    };
    runtime_config.logger.consumer_thread = af::thread_selector::cpu(0);
    runtime_config.logger.queue_capacity = 16;
    runtime_config.logger.max_batch_records = 4;
    runtime_config.logger.backends = {
        af::file_log_backend_config{path.string(), false, false, 8},
    };

    af::runtime runtime(runtime_config);
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(runtime.logger_started());

    LOG(INFO) << "runtime owned async logger drains on stop";
    runtime.stop();
    EXPECT_FALSE(runtime.logger_started());

    const std::string contents = read_file(path);
    EXPECT_NE(contents.find("runtime owned async logger drains on stop"), std::string::npos);
    std::filesystem::remove(path);
}

TEST(LogTests, RuntimeInstanceLoggerStaysDisabledWithoutBackends) {
    af::runtime_config runtime_config;
    runtime_config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 1),
    };

    af::runtime runtime(runtime_config);
    ASSERT_TRUE(runtime.start());
    EXPECT_FALSE(runtime.logger_started());
    EXPECT_TRUE(runtime.flush_logger(std::chrono::milliseconds(1)));
    runtime.stop();
}

TEST(LogTests, RuntimeAwareSinkCanStopFromConsumerRuntimeThread) {
    LogTestRuntimeGuard runtime_guard;

    auto backend = std::make_unique<CapturingLogBackend>();
    auto *capturing_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 16;
    config.max_batch_size = 4;
    config.backends.push_back(std::move(backend));
    auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(std::move(config),
                                                                       LogTestThreads::Runtime_1);

    std::atomic<int> completed{0};
    ASSERT_TRUE(LogTestRuntime::start_task<RuntimeStopLoggingTask>(LogTestThreads::Runtime_1,
                                                                   logging.get(), &completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    const std::vector<std::string> messages = capturing_backend->messages();
    ASSERT_EQ(messages.size(), 1U);
    EXPECT_NE(messages.front().find("runtime owner stops async logging"), std::string::npos);
}

TEST(LogTests, RuntimeAwareSinkDefaultConsumerPrefersIoThread) {
    LogDefaultIoRuntimeGuard runtime_guard;

    auto backend = std::make_unique<RuntimeThreadObservingLogBackend<LogDefaultIoRuntime>>(
        LogDefaultIoThreads::IO_0);
    auto *observing_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 16;
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

    af::AsyncLogConfig config = af::AsyncLogConfig::relaxed(1U, 1U);
    config.queue_capacity = 1;
    config.runtime_lane_capacity = 1;
    config.max_batch_size = 1;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.backends.push_back(std::move(backend));

    LogTestRuntimeGuard runtime_guard;
    auto logging = af::start_async_logging_for_runtime<LogTestRuntime>(std::move(config),
                                                                       LogTestThreads::Runtime_1);

    constexpr int record_count = 32;
    for (int i = 0; i < record_count; ++i) {
        std::atomic<int> completed{0};
        ASSERT_TRUE(LogTestRuntime::start_task<RuntimeLogTask>(&completed));
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        ASSERT_TRUE(logging->flush(std::chrono::seconds(2)));
    }

    const af::AsyncLogStats stats = logging->stats();
    EXPECT_EQ(stats.accepted, static_cast<std::uint64_t>(record_count));
    EXPECT_EQ(stats.dropped, 0U);
    EXPECT_EQ(counting_backend->record_count(), static_cast<std::size_t>(record_count));
    logging->stop();
}

TEST(LogTests, SharedRecordPoolExpandsAndBatchReleaseReusesSlots) {
    af::detail::AsyncLogRecordPool pool(4);
    std::array<af::detail::LogRecord *, 5> records{};

    for (std::size_t i = 0; i < records.size(); ++i) {
        records[i] = pool.try_acquire("shared batch release log record\n");
        ASSERT_NE(records[i], nullptr);
    }

    af::detail::release_async_log_records(
        af::Span<af::detail::LogRecord *const>(records.data(), records.size()));

    for (std::size_t i = 0; i < records.size(); ++i) {
        records[i] = pool.try_acquire("shared batch release reused log record\n");
        ASSERT_NE(records[i], nullptr);
    }

    af::detail::release_async_log_records(
        af::Span<af::detail::LogRecord *const>(records.data(), records.size()));
}

TEST(LogTests, RuntimeAwareSinkTagsFirstUserLogFieldWithRuntimeTaskId) {
    const auto path = std::filesystem::temp_directory_path() / "async_flow_task_id_log.txt";
    std::filesystem::remove(path);

    af::AsyncLogConfig config;
    config.queue_capacity = 16;
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

TEST(LogTests, RuntimeAwareSinkTagsEachUserLogLineAfterPrefix) {
    const auto path = std::filesystem::temp_directory_path() / "async_flow_multiline_task_log.txt";
    std::filesystem::remove(path);

    af::AsyncLogConfig config;
    config.queue_capacity = 16;
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
    const std::size_t second_user_line_pos = contents.find("runtime task id log second");
    ASSERT_NE(task_tag_pos, std::string::npos);
    ASSERT_NE(first_user_line_pos, std::string::npos);
    ASSERT_NE(second_user_line_pos, std::string::npos);
    EXPECT_EQ(first_user_line_pos, task_tag_pos + task_tag.size());
    EXPECT_EQ(count_substring_occurrences(contents, task_tag), 2U);
    EXPECT_NE(contents.find(task_tag + "runtime task id log first"), std::string::npos);
    EXPECT_NE(contents.find(task_tag + "runtime task id log second"), std::string::npos);
    EXPECT_EQ(contents.find("\n" + task_tag), std::string::npos);

    const std::size_t first_line_begin = line_begin_for_position(contents, first_user_line_pos);
    const std::size_t first_prefix_end = contents.find("] ", first_line_begin);
    ASSERT_NE(first_prefix_end, std::string::npos);
    EXPECT_EQ(task_tag_pos, first_prefix_end + 2U);

    const std::size_t second_line_begin = contents.rfind('\n', second_user_line_pos);
    ASSERT_NE(second_line_begin, std::string::npos);
    EXPECT_EQ(contents[second_line_begin + 1U], 'I');

    const std::size_t second_task_tag_pos = contents.find(task_tag, task_tag_pos + task_tag.size());
    const std::size_t second_prefix_end = contents.find("] ", second_line_begin + 1U);
    ASSERT_NE(second_task_tag_pos, std::string::npos);
    ASSERT_NE(second_prefix_end, std::string::npos);
    EXPECT_EQ(second_task_tag_pos, second_prefix_end + 2U);
    EXPECT_LT(second_line_begin + 1U, second_task_tag_pos);

    const std::string_view first_line = line_for_position(contents, first_user_line_pos);
    const std::string_view second_line = line_for_position(contents, second_user_line_pos);
    const std::size_t first_line_task_tag_pos = first_line.find(task_tag);
    const std::size_t second_line_task_tag_pos = second_line.find(task_tag);
    ASSERT_NE(first_line_task_tag_pos, std::string_view::npos);
    ASSERT_NE(second_line_task_tag_pos, std::string_view::npos);
    EXPECT_NE(first_line_task_tag_pos, 0U);
    EXPECT_NE(second_line_task_tag_pos, 0U);
    EXPECT_EQ(first_line.substr(0, first_line_task_tag_pos),
              second_line.substr(0, second_line_task_tag_pos));
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

TEST(LogTests, OrderedLoggingIsDefaultSingleMpscQueue) {
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

    ASSERT_TRUE(logger->try_log("block ordered log worker\n"));
    ASSERT_TRUE(blocking_backend->wait_until_entered(std::chrono::seconds(2)));

    std::array<std::thread, 4> producers;
    std::atomic<int> accepted{0};
    for (std::size_t i = 0; i < producers.size(); ++i) {
        producers[i] = std::thread([&logger, &accepted] {
            if (logger->try_log("ordered producer log\n")) {
                accepted.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto &producer : producers) {
        producer.join();
    }

    EXPECT_EQ(accepted.load(std::memory_order_acquire), 2);
    const af::AsyncLogStats stats = logger->stats();
    EXPECT_EQ(stats.accepted, 3U);
    EXPECT_EQ(stats.dropped, 2U);

    blocking_backend->release();
    ASSERT_TRUE(logger->flush(std::chrono::seconds(2)));
    consumer.shutdown();
}

TEST(LogTests, OrderedLoggingPreservesSingleProducerFifoAcrossBatches) {
    auto backend = std::make_unique<CapturingLogBackend>();
    auto *capturing_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 256;
    config.queue_shard_count = 1;
    config.max_batch_size = 7;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.backends.push_back(std::move(backend));

    LogTestRuntimeGuard runtime_guard;
    auto logger = std::make_shared<af::AsyncLogger>(std::move(config));
    ScopedRuntimeLogConsumer<LogTestRuntime> consumer(logger, LogTestThreads::Runtime_1, 64);
    ASSERT_TRUE(consumer.start());

    constexpr int record_count = 128;
    for (int index = 0; index < record_count; ++index) {
        ASSERT_TRUE(logger->try_log("ordered-fifo-" + std::to_string(index)));
    }

    ASSERT_TRUE(logger->flush(std::chrono::seconds(2)));
    const std::vector<std::string> messages = capturing_backend->messages();
    ASSERT_EQ(messages.size(), static_cast<std::size_t>(record_count));
    for (int index = 0; index < record_count; ++index) {
        EXPECT_EQ(messages[static_cast<std::size_t>(index)],
                  "ordered-fifo-" + std::to_string(index));
    }
    const af::AsyncLogStats stats = logger->stats();
    EXPECT_EQ(stats.accepted, static_cast<std::uint64_t>(record_count));
    EXPECT_EQ(stats.dropped, 0U);
    consumer.shutdown();
}

TEST(LogTests, OrderedLoggingConcurrentProducersDrainWithoutLossOrDuplicates) {
    auto backend = std::make_unique<CapturingLogBackend>();
    auto *capturing_backend = backend.get();

    af::AsyncLogConfig config;
    config.queue_capacity = 8192;
    config.queue_shard_count = 8;
    config.max_batch_size = 128;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.backends.push_back(std::move(backend));

    LogTestRuntimeGuard runtime_guard;
    auto logger = std::make_shared<af::AsyncLogger>(std::move(config));
    ScopedRuntimeLogConsumer<LogTestRuntime> consumer(logger, LogTestThreads::Runtime_1, 256);
    ASSERT_TRUE(consumer.start());

    constexpr int producer_count = 8;
    constexpr int records_per_producer = 512;
    constexpr int expected_records = producer_count * records_per_producer;
    constexpr std::string_view prefix = "ordered-stress-";

    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<int> failures{0};
    std::array<std::thread, producer_count> producers;
    for (int producer = 0; producer < producer_count; ++producer) {
        producers[static_cast<std::size_t>(producer)] = std::thread([&, producer] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            const int base = producer * records_per_producer;
            for (int index = 0; index < records_per_producer; ++index) {
                if (!logger->try_log(std::string(prefix) + std::to_string(base + index))) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != producer_count) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (std::thread &producer : producers) {
        producer.join();
    }

    EXPECT_EQ(failures.load(std::memory_order_acquire), 0);
    ASSERT_TRUE(logger->flush(std::chrono::seconds(5)));
    const af::AsyncLogStats stats = logger->stats();
    EXPECT_EQ(stats.accepted, static_cast<std::uint64_t>(expected_records));
    EXPECT_EQ(stats.dropped, 0U);
    expect_exact_indexed_log_set(capturing_backend->messages(), prefix, expected_records);
    consumer.shutdown();
}

TEST(LogTests, ShardedQueuesAvoidSingleQueueProducerContention) {
    auto backend = std::make_unique<BlockingLogBackend>();
    auto *blocking_backend = backend.get();

    af::AsyncLogConfig config = af::AsyncLogConfig::relaxed(0U, 4U);
    config.queue_capacity = 1;
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

TEST(LogTests, RelaxedLoggingConcurrentProducersDrainWithoutLossOrDuplicates) {
    auto backend = std::make_unique<CapturingLogBackend>();
    auto *capturing_backend = backend.get();

    af::AsyncLogConfig config = af::AsyncLogConfig::relaxed(0U, 8U);
    config.queue_capacity = 8192;
    config.max_batch_size = 128;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.backends.push_back(std::move(backend));

    LogTestRuntimeGuard runtime_guard;
    auto logger = std::make_shared<af::AsyncLogger>(std::move(config));
    ScopedRuntimeLogConsumer<LogTestRuntime> consumer(logger, LogTestThreads::Runtime_1, 256);
    ASSERT_TRUE(consumer.start());

    constexpr int producer_count = 8;
    constexpr int records_per_producer = 512;
    constexpr int expected_records = producer_count * records_per_producer;
    constexpr std::string_view prefix = "relaxed-stress-";

    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<int> failures{0};
    std::array<std::thread, producer_count> producers;
    for (int producer = 0; producer < producer_count; ++producer) {
        producers[static_cast<std::size_t>(producer)] = std::thread([&, producer] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            const int base = producer * records_per_producer;
            for (int index = 0; index < records_per_producer; ++index) {
                if (!logger->try_log(std::string(prefix) + std::to_string(base + index))) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != producer_count) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (std::thread &producer : producers) {
        producer.join();
    }

    EXPECT_EQ(failures.load(std::memory_order_acquire), 0);
    ASSERT_TRUE(logger->flush(std::chrono::seconds(5)));
    const af::AsyncLogStats stats = logger->stats();
    EXPECT_EQ(stats.accepted, static_cast<std::uint64_t>(expected_records));
    EXPECT_EQ(stats.dropped, 0U);
    expect_exact_indexed_log_set(capturing_backend->messages(), prefix, expected_records);
    consumer.shutdown();
}

TEST(LogTests, ShardedQueuesDrainConcurrentProducers) {
    auto backend = std::make_unique<CountingLogBackend>();
    auto *counting_backend = backend.get();

    af::AsyncLogConfig config = af::AsyncLogConfig::relaxed(0U, 8U);
    config.queue_capacity = 4096;
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
            af::Span<af::detail::LogRecord *const>(record_ptrs.data(), record_ptrs.size()));
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    server.join();
    close_fd(listener);

    EXPECT_NE(received.find("tcp backend one\n"), std::string::npos);
    EXPECT_NE(received.find("tcp backend two\n"), std::string::npos);
    EXPECT_NE(received.find("tcp backend three\n"), std::string::npos);
    EXPECT_NE(received.find("tcp backend four\n"), std::string::npos);
}

TEST(LogTests, UdpBackendWritesBatchedRecordsToLoopbackDatagrams) {
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
        af::Span<af::detail::LogRecord *const>(record_ptrs.data(), record_ptrs.size()));

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
}

TEST(LogTests, RuntimeUdpBackendSendsBatchesOnIoThread) {
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
        af::Span<af::detail::LogRecord *const>(record_ptrs.data(), record_ptrs.size()));
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
}

TEST(LogTests, RuntimeTcpBackendSendsBatchesOnIoThread) {
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
        af::Span<af::detail::LogRecord *const>(record_ptrs.data(), record_ptrs.size()));
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
}

TEST(LogTests, RuntimeTcpAsyncLoggerBackendSendsOnIoThread) {
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
}
