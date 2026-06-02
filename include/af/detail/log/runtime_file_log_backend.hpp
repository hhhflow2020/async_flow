#pragma once

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/log/log_backend.hpp"
#include "af/detail/queue/bounded_spsc_queue.hpp"
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

template <typename RuntimeT> class RuntimeFileLogState {
public:
    using Thread = typename RuntimeT::Thread;
    using Batch = RuntimeFileLogBatch;

    explicit RuntimeFileLogState(RuntimeFileLogBackendConfig<RuntimeT> config)
        : thread(config.thread), path(config.path.string()), append(config.append),
          close_on_exec(config.close_on_exec), fsync_on_flush(config.fsync_on_flush),
          max_batch_records(normalize_max_batch_records(config.max_batch_records)),
          max_batches_per_run(config.max_batches_per_run == 0U ? 1U : config.max_batches_per_run),
          ready_batches(config.batch_queue_capacity), free_batches(config.batch_queue_capacity) {
        reserve_batches(config.batch_queue_capacity);
    }

    RuntimeFileLogState(const RuntimeFileLogState &) = delete;
    RuntimeFileLogState &operator=(const RuntimeFileLogState &) = delete;

    ~RuntimeFileLogState() {
#if !defined(_WIN32)
        close_file();
#endif
    }

    [[nodiscard]] bool enqueue(std::span<LogRecord *const> records) noexcept {
        if (records.empty() || stopping.load(std::memory_order_acquire)) {
            return false;
        }

        bool enqueued_any = false;
        std::size_t index = 0;
        while (index < records.size()) {
            Batch *batch = free_batches.try_pop();
            if (batch == nullptr) {
                dropped_records.fetch_add(records.size() - index, std::memory_order_relaxed);
                return enqueued_any;
            }

            batch->reset();
            const std::size_t begin = index;
            while (index < records.size()) {
                const std::string_view message = records[index]->message();
                if (!batch->append(message, max_batch_records)) {
                    break;
                }
                ++index;
                if (batch->record_count >= max_batch_records) {
                    break;
                }
            }

            if (batch->empty()) {
                recycle_batch(batch);
                if (index == begin) {
                    ++index;
                }
                continue;
            }

            queued_records.fetch_add(batch->record_count, std::memory_order_relaxed);
            pending_batches.fetch_add(1U, std::memory_order_acq_rel);
            if (!ready_batches.try_push(batch)) [[unlikely]] {
                complete_batch(batch);
                dropped_records.fetch_add(index - begin, std::memory_order_relaxed);
                return enqueued_any;
            }
            enqueued_any = true;
        }
        return enqueued_any;
    }

    [[nodiscard]] std::uint64_t request_flush() noexcept {
        return flush_requests.fetch_add(1U, std::memory_order_acq_rel) + 1U;
    }

    [[nodiscard]] bool flush_until(std::uint64_t target,
                                   std::chrono::steady_clock::time_point deadline) noexcept {
        while (completed_flushes.load(std::memory_order_acquire) < target) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::yield();
        }
        return true;
    }

    [[nodiscard]] RuntimeFileLogBackendStats stats() const noexcept {
        return RuntimeFileLogBackendStats{
            queued_records.load(std::memory_order_acquire),
            written_records.load(std::memory_order_acquire),
            dropped_records.load(std::memory_order_acquire),
            flushes.load(std::memory_order_acquire),
            last_error.load(std::memory_order_acquire),
            last_error_stage.load(std::memory_order_acquire),
        };
    }

#if !defined(_WIN32)
    [[nodiscard]] bool open_file() noexcept {
        if (fd >= 0) {
            return true;
        }
        if (path.empty()) {
            last_error.store(EINVAL, std::memory_order_release);
            last_error_stage.store(1, std::memory_order_release);
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
            last_error.store(errno == 0 ? EIO : errno, std::memory_order_release);
            last_error_stage.store(2, std::memory_order_release);
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

    void complete_batch(Batch *batch) noexcept {
        if (batch == nullptr) {
            return;
        }
        recycle_batch(batch);
        if (pending_batches.fetch_sub(1U, std::memory_order_acq_rel) == 1U) {
            pending_batches.notify_all();
        }
    }

    void recycle_batch(Batch *batch) noexcept {
        const bool recycled = free_batches.try_push(batch);
        AF_ASSERT(recycled);
        static_cast<void>(recycled);
    }

    void complete_requested_flushes() noexcept {
        const std::uint64_t requested = flush_requests.load(std::memory_order_acquire);
        completed_flushes.store(requested, std::memory_order_release);
        completed_flushes.notify_all();
    }

    [[nodiscard]] bool has_pending_flush() const noexcept {
        return completed_flushes.load(std::memory_order_acquire) <
               flush_requests.load(std::memory_order_acquire);
    }

    Thread thread;
    const std::string path;
    const bool append;
    const bool close_on_exec;
    const bool fsync_on_flush;
    const std::size_t max_batch_records;
    const std::size_t max_batches_per_run;
    BoundedSpscQueue<Batch> ready_batches;
    BoundedSpscQueue<Batch> free_batches;
    std::vector<std::unique_ptr<Batch>> storage;
    std::atomic<std::uint64_t> queued_records{0};
    std::atomic<std::uint64_t> written_records{0};
    std::atomic<std::uint64_t> dropped_records{0};
    std::atomic<std::uint64_t> flushes{0};
    std::atomic<int> last_error{0};
    std::atomic<int> last_error_stage{0};
    std::atomic<std::size_t> pending_batches{0};
    std::atomic<std::uint64_t> flush_requests{0};
    std::atomic<std::uint64_t> completed_flushes{0};
    std::atomic<bool> wake_queued{false};
    std::atomic<bool> io_waiting{false};
    std::atomic<bool> stopping{false};
    std::atomic<bool> finished{false};

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

    void reserve_batches(std::size_t queue_capacity) {
        const std::size_t capacity = queue_capacity == 0U ? 1U : queue_capacity;
        storage.reserve(capacity);
        for (std::size_t i = 0; i < capacity; ++i) {
            auto batch = std::make_unique<Batch>(max_batch_records);
            Batch *ptr = batch.get();
            storage.push_back(std::move(batch));
            const bool ok = free_batches.try_push(ptr);
            AF_ASSERT(ok);
            static_cast<void>(ok);
        }
    }
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

        state_->wake_queued.store(false, std::memory_order_release);
        if (state_->stopping.load(std::memory_order_acquire)) {
            drop_current();
            drop_ready_batches();
#if !defined(_WIN32)
            state_->close_file();
#endif
            state_->finished.store(true, std::memory_order_release);
            state_->finished.notify_all();
            return this->done();
        }
#if !defined(_WIN32)
        if (state_->io_waiting.load(std::memory_order_acquire) && !io_wait_ready()) {
            return this->pending();
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
                        return this->pending();
                    }
                    if (state_->stopping.load(std::memory_order_acquire)) {
                        state_->finished.store(true, std::memory_order_release);
                        state_->finished.notify_all();
                        return this->done();
                    }
                    return this->pending();
                }
            }

            const WriteResult result = write_current();
            if (result == WriteResult::Pending) {
                return this->pending();
            }

            state_->complete_batch(current_);
            current_ = nullptr;
            ++drained_batches;
            if (drained_batches >= state_->max_batches_per_run) {
                return this->again();
            }
        }
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
            state_->io_waiting.store(true, std::memory_order_release);
            const IoStatus status =
                io_write_some(*this, state_->thread, state_->fd, data, size, write_state_);
            if (status.pending()) {
                return WriteResult::Pending;
            }

            state_->io_waiting.store(false, std::memory_order_release);
            write_state_.reset();
            if (status.ready() && status.bytes > 0U) {
                current_byte_ += status.bytes;
                continue;
            }
            if (status.ready()) {
                state_->last_error.store(EIO, std::memory_order_release);
                state_->last_error_stage.store(3, std::memory_order_release);
            } else {
                state_->last_error.store(status.failed() ? status.error : EIO,
                                         std::memory_order_release);
                state_->last_error_stage.store(4, std::memory_order_release);
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
                state_->last_error.store(errno == 0 ? EIO : errno, std::memory_order_release);
                state_->last_error_stage.store(5, std::memory_order_release);
                break;
            }
            state_->flushes.fetch_add(1U, std::memory_order_relaxed);
            state_->complete_requested_flushes();
            return FlushResult::Complete;
        }

        state_->io_waiting.store(true, std::memory_order_release);
        const IoStatus status = io_fsync(*this, state_->thread, state_->fd, 0, fsync_state_);
        if (status.pending()) {
            return FlushResult::Pending;
        }

        state_->io_waiting.store(false, std::memory_order_release);
        fsync_state_.reset();
        if (status.failed()) {
            state_->last_error.store(status.error == 0 ? EIO : status.error,
                                     std::memory_order_release);
            state_->last_error_stage.store(6, std::memory_order_release);
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
        state_->io_waiting.store(false, std::memory_order_release);
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
        : state_(std::make_unique<State>(std::move(config))),
          writer_(RuntimeT::template make_task<WriterTask>()) {}

    ~RuntimeFileLogBackend() override {
        shutdown();
    }

    void write_batch(std::span<detail::LogRecord *const> records) noexcept override {
        if (state_->enqueue(records)) {
            static_cast<void>(wake_writer());
        }
    }

    void flush() noexcept override {
        static_cast<void>(flush(std::chrono::seconds(5)));
    }

    [[nodiscard]] bool flush(std::chrono::milliseconds timeout) noexcept override {
        const std::uint64_t target = state_->request_flush();
        if (!wake_writer()) {
            return false;
        }
        return state_->flush_until(target, std::chrono::steady_clock::now() + timeout);
    }

    void shutdown() noexcept override {
        bool expected = false;
        if (!shutdown_started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
            return;
        }

        static_cast<void>(flush(std::chrono::seconds(5)));
        state_->stopping.store(true, std::memory_order_release);
        if (!writer_started_.load(std::memory_order_acquire) &&
            state_->pending_batches.load(std::memory_order_acquire) == 0U) {
            state_->finished.store(true, std::memory_order_release);
            writer_.reset();
            return;
        }
        if (wake_writer()) {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (!state_->finished.load(std::memory_order_acquire) &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::yield();
            }
        }
        writer_.reset();
    }

    [[nodiscard]] RuntimeFileLogBackendStats stats() const noexcept {
        return state_->stats();
    }

private:
    [[nodiscard]] bool wake_writer() noexcept {
        if (!writer_) {
            return false;
        }
        if (state_->finished.load(std::memory_order_acquire)) {
            return true;
        }
        if (state_->io_waiting.load(std::memory_order_acquire) &&
            !state_->stopping.load(std::memory_order_acquire)) {
            return true;
        }

        bool wake_expected = false;
        if (!state_->wake_queued.compare_exchange_strong(
                wake_expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return true;
        }

        bool expected = false;
        if (writer_started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
            if (writer_->start(state_.get())) {
                return true;
            }
            writer_started_.store(false, std::memory_order_release);
            state_->wake_queued.store(false, std::memory_order_release);
            return false;
        }

        if (writer_->wake()) {
            return true;
        }
        state_->wake_queued.store(false, std::memory_order_release);
        return false;
    }

    std::unique_ptr<State> state_;
    typename RuntimeT::template TaskHandle<WriterTask> writer_;
    std::atomic<bool> writer_started_{false};
    std::atomic<bool> shutdown_started_{false};
};

template <typename RuntimeT>
[[nodiscard]] inline std::unique_ptr<LogBackend>
make_runtime_file_log_backend(RuntimeFileLogBackendConfig<RuntimeT> config) {
    return std::make_unique<RuntimeFileLogBackend<RuntimeT>>(std::move(config));
}

} // namespace af
