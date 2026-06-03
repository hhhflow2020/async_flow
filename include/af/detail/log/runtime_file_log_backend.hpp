#pragma once

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/log_backend.hpp"
#include "af/detail/log/runtime_log_backend_common.hpp"
#include "af/io_file.hpp"
#include "af/task.hpp"

#if !defined(_WIN32)
#include <fcntl.h>
#include <unistd.h>
#endif

namespace af {

template <typename RuntimeT> struct RuntimeFileLogBackendConfig {
    typename RuntimeT::Thread thread{};
    std::filesystem::path path;
    bool append{true};
    bool close_on_exec{true};
    bool fsync_on_flush{true};
    std::size_t batch_queue_capacity{1024};
    std::size_t max_batch_records{64};
    std::size_t max_batches_per_run{64};
};

struct RuntimeFileLogBackendStats {
    std::uint64_t queued_records{0};
    std::uint64_t written_records{0};
    std::uint64_t dropped_records{0};
    std::uint64_t flushes{0};
    int last_error{0};
    int last_error_stage{0};
};

namespace detail {

class RuntimeFileLogBatch {
public:
    explicit RuntimeFileLogBatch(std::size_t max_records) {
        payload.reserve(max_records * default_log_inline_message_bytes);
    }

    void reset() noexcept {
        record_count = 0;
        payload.clear();
    }

    [[nodiscard]] bool append(std::string_view message, std::size_t max_records) {
        if (message.empty()) {
            return true;
        }
        if (record_count >= max_records) {
            return false;
        }

        payload.insert(payload.end(), message.data(), message.data() + message.size());
        ++record_count;
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return record_count == 0U;
    }

    std::uint32_t record_count{0};
    std::vector<char> payload;
};

template <typename RuntimeT>
class RuntimeFileLogState : public RuntimeLogQueueState<RuntimeFileLogBatch> {
public:
    using Thread = typename RuntimeT::Thread;
    using Batch = RuntimeFileLogBatch;
    using QueueState = RuntimeLogQueueState<Batch>;

    explicit RuntimeFileLogState(RuntimeFileLogBackendConfig<RuntimeT> config)
        : QueueState(config.batch_queue_capacity,
                     normalize_max_batch_records(config.max_batch_records),
                     config.max_batches_per_run),
          thread(config.thread), path(config.path.string()), append(config.append),
          close_on_exec(config.close_on_exec), fsync_on_flush(config.fsync_on_flush),
          written_records(0), flushes(0), last_error(0), last_error_stage(0), flush_requests(0),
          completed_flushes(0) {}

    RuntimeFileLogState(const RuntimeFileLogState &) = delete;
    RuntimeFileLogState &operator=(const RuntimeFileLogState &) = delete;

    ~RuntimeFileLogState() {
#if !defined(_WIN32)
        close_file();
#endif
    }

    [[nodiscard]] std::uint64_t request_flush() noexcept {
        return flush_requests.fetch_add(1U, std::memory_order_relaxed) + 1U;
    }

    [[nodiscard]] bool flush_until(std::uint64_t target,
                                   std::chrono::steady_clock::time_point deadline) noexcept {
        if (completed_flushes.load(std::memory_order_acquire) >= target) {
            return true;
        }

        std::unique_lock lock(flush_mutex_);
        return flush_cv_.wait_until(lock, deadline, [this, target] {
            return completed_flushes.load(std::memory_order_acquire) >= target;
        });
    }

    [[nodiscard]] RuntimeFileLogBackendStats stats() const noexcept {
        return RuntimeFileLogBackendStats{
            queued_records.load(std::memory_order_relaxed),
            written_records.load(std::memory_order_relaxed),
            dropped_records.load(std::memory_order_relaxed),
            flushes.load(std::memory_order_relaxed),
            last_error.load(std::memory_order_relaxed),
            last_error_stage.load(std::memory_order_relaxed),
        };
    }

#if !defined(_WIN32)
    [[nodiscard]] bool open_file() noexcept {
        if (fd >= 0) {
            return true;
        }
        if (path.empty()) {
            last_error.store(EINVAL, std::memory_order_relaxed);
            last_error_stage.store(1, std::memory_order_relaxed);
            return false;
        }

        int flags = O_CREAT | O_WRONLY;
        if (append || opened_once) {
            flags |= O_APPEND;
        } else {
            flags |= O_TRUNC;
        }
#if defined(O_CLOEXEC)
        if (close_on_exec) {
            flags |= O_CLOEXEC;
        }
#endif

        int candidate = ::open(path.c_str(), flags, 0644);
        if (candidate < 0) {
            last_error.store(errno == 0 ? EIO : errno, std::memory_order_relaxed);
            last_error_stage.store(2, std::memory_order_relaxed);
            return false;
        }

        fd = candidate;
        opened_once = true;
        return true;
    }

    void close_file() noexcept {
        if (fd >= 0) {
            static_cast<void>(::close(fd));
            fd = -1;
        }
    }
#endif

    void complete_requested_flushes() noexcept {
        const std::uint64_t requested = flush_requests.load(std::memory_order_relaxed);
        completed_flushes.store(requested, std::memory_order_release);
        completed_flushes.notify_all();
        flush_cv_.notify_all();
    }

    [[nodiscard]] bool has_pending_flush() const noexcept {
        return completed_flushes.load(std::memory_order_relaxed) <
               flush_requests.load(std::memory_order_relaxed);
    }

    Thread thread;
    const std::string path;
    const bool append;
    const bool close_on_exec;
    const bool fsync_on_flush;
    CacheLineAtomic<std::uint64_t> written_records;
    CacheLineAtomic<std::uint64_t> flushes;
    CacheLineAtomic<int> last_error;
    CacheLineAtomic<int> last_error_stage;
    CacheLineAtomic<std::uint64_t> flush_requests;
    CacheLineAtomic<std::uint64_t> completed_flushes;
    bool io_waiting{false};

#if !defined(_WIN32)
    int fd{-1};
    bool opened_once{false};
#endif

private:
    [[nodiscard]] static std::size_t normalize_max_batch_records(std::size_t requested) noexcept {
        constexpr std::size_t max_supported_records = 1024;
        if (requested == 0U) {
            return 1U;
        }
        return std::min(requested, max_supported_records);
    }

    std::mutex flush_mutex_;
    std::condition_variable flush_cv_;
};

template <typename RuntimeT> class RuntimeFileLogWriterTask final : public RuntimeT::Task {
public:
    using TaskBase = typename RuntimeT::Task;
    using State = RuntimeFileLogState<RuntimeT>;
    using Batch = RuntimeFileLogBatch;

    explicit RuntimeFileLogWriterTask(typename TaskBase::FactoryToken token) : TaskBase(token) {}

    [[nodiscard]] bool start(State *state) noexcept {
        state_ = state;
        return this->schedule(state_->thread);
    }

    [[nodiscard]] bool wake() noexcept {
        return state_ != nullptr && this->schedule(state_->thread);
    }

private:
    TaskResult run() override {
        if (state_ == nullptr) [[unlikely]] {
            return this->done();
        }

        if (state_->stopping.load(std::memory_order_acquire)) {
            drop_current();
            drop_ready_batches();
#if !defined(_WIN32)
            state_->close_file();
#endif
            return finish();
        }
#if !defined(_WIN32)
        if (state_->io_waiting && !io_wait_ready()) {
            return io_pending();
        }
#endif

        std::size_t drained_batches = 0;
        for (;;) {
            if (current_ == nullptr) {
                current_ = state_->ready_batches.try_pop();
                current_byte_ = 0;
                if (current_ == nullptr) {
                    const FlushResult flush_result = flush_if_requested();
                    if (flush_result == FlushResult::Pending) {
                        return io_pending();
                    }
                    if (state_->stopping.load(std::memory_order_acquire)) {
                        return finish();
                    }
                    return idle();
                }
            }

            const WriteResult result = write_current();
            if (result == WriteResult::Pending) {
                return io_pending();
            }

            state_->complete_batch(current_);
            current_ = nullptr;
            ++drained_batches;
            if (drained_batches >= state_->max_batches_per_run) {
                return this->again();
            }
        }
    }

    TaskResult idle() noexcept {
        state_->wake_queued.store(false, std::memory_order_release);
        if (state_->stopping.load(std::memory_order_acquire) ||
            state_->pending_batches.load(std::memory_order_acquire) != 0U ||
            state_->has_pending_flush()) {
            state_->wake_queued.store(true, std::memory_order_release);
            return this->again();
        }
        return this->pending();
    }

    TaskResult io_pending() noexcept {
        return this->pending();
    }

    TaskResult finish() noexcept {
        state_->wake_queued.store(false, std::memory_order_release);
        state_->mark_finished();
        return this->done();
    }

    enum class WriteResult : std::uint8_t {
        Complete,
        Pending,
    };

    enum class FlushResult : std::uint8_t {
        Complete,
        Pending,
    };

    [[nodiscard]] WriteResult write_current() noexcept {
#if defined(_WIN32)
        state_->dropped_records.fetch_add(current_->record_count, std::memory_order_relaxed);
        current_byte_ = current_->payload.size();
        return WriteResult::Complete;
#else
        if (!state_->open_file()) {
            drop_current_records();
            return WriteResult::Complete;
        }

        while (current_byte_ < current_->payload.size()) {
            const char *data = current_->payload.data() + current_byte_;
            const std::size_t size = current_->payload.size() - current_byte_;
            state_->io_waiting = true;
            const IoStatus status =
                io_write_some(*this, state_->thread, state_->fd, data, size, write_state_);
            if (status.pending()) {
                return WriteResult::Pending;
            }

            state_->io_waiting = false;
            write_state_.reset();
            if (status.ready() && status.bytes > 0U) {
                current_byte_ += status.bytes;
                continue;
            }
            if (status.ready()) {
                state_->last_error.store(EIO, std::memory_order_relaxed);
                state_->last_error_stage.store(3, std::memory_order_relaxed);
            } else {
                state_->last_error.store(status.failed() ? status.error : EIO,
                                         std::memory_order_relaxed);
                state_->last_error_stage.store(4, std::memory_order_relaxed);
            }
            state_->close_file();
            drop_current_records();
            return WriteResult::Complete;
        }

        state_->written_records.fetch_add(current_->record_count, std::memory_order_relaxed);
        return WriteResult::Complete;
#endif
    }

    [[nodiscard]] FlushResult flush_if_requested() noexcept {
        if (!state_->has_pending_flush()) {
            return FlushResult::Complete;
        }
#if defined(_WIN32)
        state_->complete_requested_flushes();
        return FlushResult::Complete;
#else
        if (!state_->fsync_on_flush || state_->fd < 0) {
            state_->flushes.fetch_add(1U, std::memory_order_relaxed);
            state_->complete_requested_flushes();
            return FlushResult::Complete;
        }

        if (!TaskBase::Runtime::io_uring_backend_available(state_->thread)) {
            while (::fsync(state_->fd) != 0) {
                if (errno == EINTR) {
                    continue;
                }
                state_->last_error.store(errno == 0 ? EIO : errno, std::memory_order_relaxed);
                state_->last_error_stage.store(5, std::memory_order_relaxed);
                break;
            }
            state_->flushes.fetch_add(1U, std::memory_order_relaxed);
            state_->complete_requested_flushes();
            return FlushResult::Complete;
        }

        state_->io_waiting = true;
        const IoStatus status = io_fsync(*this, state_->thread, state_->fd, 0, fsync_state_);
        if (status.pending()) {
            return FlushResult::Pending;
        }

        state_->io_waiting = false;
        fsync_state_.reset();
        if (status.failed()) {
            state_->last_error.store(status.error == 0 ? EIO : status.error,
                                     std::memory_order_relaxed);
            state_->last_error_stage.store(6, std::memory_order_relaxed);
        }
        state_->flushes.fetch_add(1U, std::memory_order_relaxed);
        state_->complete_requested_flushes();
        return FlushResult::Complete;
#endif
    }

#if !defined(_WIN32)
    [[nodiscard]] bool io_wait_ready() const noexcept {
        return io_wait_result_ready(write_state_) || io_wait_result_ready(fsync_state_);
    }

    IoOpState write_state_{};
    IoOpState fsync_state_{};
#endif

    void drop_current_records() noexcept {
        if (current_ != nullptr) {
            state_->dropped_records.fetch_add(current_->record_count, std::memory_order_relaxed);
            current_byte_ = current_->payload.size();
        }
    }

    void drop_current() noexcept {
        if (current_ == nullptr) {
            return;
        }
#if !defined(_WIN32)
        state_->io_waiting = false;
        write_state_.reset();
        fsync_state_.reset();
#endif
        drop_current_records();
        state_->complete_batch(current_);
        current_ = nullptr;
    }

    void drop_ready_batches() noexcept {
        while (Batch *batch = state_->ready_batches.try_pop()) {
            state_->dropped_records.fetch_add(batch->record_count, std::memory_order_relaxed);
            state_->complete_batch(batch);
        }
    }

    State *state_{nullptr};
    Batch *current_{nullptr};
    std::size_t current_byte_{0};
};

} // namespace detail

template <typename RuntimeT> class RuntimeFileLogBackend final : public LogBackend {
public:
    using Config = RuntimeFileLogBackendConfig<RuntimeT>;
    using State = detail::RuntimeFileLogState<RuntimeT>;
    using WriterTask = detail::RuntimeFileLogWriterTask<RuntimeT>;

    explicit RuntimeFileLogBackend(Config config)
        : binding_(std::make_unique<State>(std::move(config))) {}

    ~RuntimeFileLogBackend() override {
        shutdown();
    }

    void write_batch(std::span<detail::LogRecord *const> records) noexcept override {
        static_cast<void>(binding_.enqueue_and_wake(records));
    }

    void flush() noexcept override {
        static_cast<void>(flush(std::chrono::seconds(5)));
    }

    [[nodiscard]] bool flush(std::chrono::milliseconds timeout) noexcept override {
        State &state = binding_.state();
        const std::uint64_t target = state.request_flush();
        if (!binding_.wake()) {
            return false;
        }
        return state.flush_until(target, std::chrono::steady_clock::now() + timeout);
    }

    void shutdown() noexcept override {
        bool expected = false;
        if (!shutdown_started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
            return;
        }

        static_cast<void>(flush(std::chrono::seconds(5)));
        binding_.stop_and_wait(std::chrono::steady_clock::now() + std::chrono::seconds(5));
    }

    [[nodiscard]] RuntimeFileLogBackendStats stats() const noexcept {
        return binding_.state().stats();
    }

private:
    detail::RuntimeLogTaskBinding<RuntimeT, State, WriterTask> binding_;
    detail::CacheLineAtomic<bool> shutdown_started_{false};
};

template <typename RuntimeT>
[[nodiscard]] inline std::unique_ptr<LogBackend>
make_runtime_file_log_backend(RuntimeFileLogBackendConfig<RuntimeT> config) {
    return std::make_unique<RuntimeFileLogBackend<RuntimeT>>(std::move(config));
}

} // namespace af
