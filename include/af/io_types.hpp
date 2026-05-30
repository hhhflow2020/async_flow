#pragma once
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <limits>
#include <type_traits>
#include <utility>

#include "af/task.hpp"

#if !defined(_WIN32)
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

#if defined(__linux__)
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/sendfile.h>
#include <sys/timerfd.h>
#endif

#if !defined(__linux__)
struct statx;
#endif

namespace af {

#if defined(_WIN32)
using IoOffset = std::int64_t;
#else
using IoOffset = off_t;
#endif

#if !defined(_WIN32)
struct IoFixedBuffer {
    void* data{nullptr};
    std::size_t size{0};
    std::uint16_t index{0};
};
#endif

template <typename ThreadT>
class IoFixedFile;

struct IoRecvmsgMultishotView {
    std::uint16_t buffer_id{0};
    std::uint32_t name_offset{0};
    std::uint32_t name_size{0};
    std::uint32_t control_offset{0};
    std::uint32_t control_size{0};
    std::uint32_t payload_offset{0};
    std::uint32_t payload_size{0};
    std::uint32_t flags{0};
};

#if defined(__linux__)
namespace detail {
struct IoProvidedBufferRingEntry {
    std::uint64_t addr{0};
    std::uint32_t len{0};
    std::uint16_t bid{0};
    std::uint16_t resv{0};
};
} // namespace detail

struct IoProvidedBuffer {
    void* data{nullptr};
    std::uint32_t size{0};
    std::uint16_t id{0};
};

class IoProvidedBufferRing {
public:
    IoProvidedBufferRing() noexcept = default;

    IoProvidedBufferRing(const IoProvidedBufferRing&) = delete;
    IoProvidedBufferRing& operator=(const IoProvidedBufferRing&) = delete;

    IoProvidedBufferRing(IoProvidedBufferRing&& other) noexcept
        : ring_(std::exchange(other.ring_, nullptr)),
          ring_size_(std::exchange(other.ring_size_, 0)),
          entries_(std::exchange(other.entries_, 0)) {}

    IoProvidedBufferRing& operator=(IoProvidedBufferRing&& other) noexcept {
        if (this != &other) {
            reset();
            ring_ = std::exchange(other.ring_, nullptr);
            ring_size_ = std::exchange(other.ring_size_, 0);
            entries_ = std::exchange(other.entries_, 0);
        }
        return *this;
    }

    ~IoProvidedBufferRing() {
        reset();
    }

    [[nodiscard]] bool init(unsigned entries, int& error) noexcept {
        reset();
        error = 0;
        if (entries == 0U ||
            (entries & (entries - 1U)) != 0U ||
            entries > max_entries()) {
            error = EINVAL;
            return false;
        }

        const std::size_t bytes = allocation_size(entries);
        void* memory = ::mmap(
            nullptr,
            bytes,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS,
            -1,
            0);
        if (memory == MAP_FAILED) {
            ring_ = nullptr;
            ring_size_ = 0;
            entries_ = 0;
            error = errno == 0 ? EIO : errno;
            return false;
        }

        ring_ = memory;
        ring_size_ = bytes;
        entries_ = entries;
        __atomic_store_n(tail(), static_cast<std::uint16_t>(0), __ATOMIC_RELEASE);
        return true;
    }

    void reset() noexcept {
        if (ring_ != nullptr) {
            ::munmap(ring_, ring_size_);
            ring_ = nullptr;
        }
        ring_size_ = 0;
        entries_ = 0;
    }

    [[nodiscard]] bool add(const IoProvidedBuffer* buffers, unsigned count, int& error) noexcept {
        error = 0;
        if (ring_ == nullptr || buffers == nullptr || count == 0U || count > entries_) {
            error = EINVAL;
            return false;
        }

        std::uint16_t current_tail = __atomic_load_n(tail(), __ATOMIC_ACQUIRE);
        const unsigned mask = entries_ - 1U;
        auto* entries = reinterpret_cast<detail::IoProvidedBufferRingEntry*>(ring_);
        for (unsigned i = 0; i < count; ++i) {
            if (buffers[i].data == nullptr || buffers[i].size == 0U) {
                error = EINVAL;
                return false;
            }
            detail::IoProvidedBufferRingEntry& entry =
                entries[(static_cast<unsigned>(current_tail) + i) & mask];
            entry.addr = reinterpret_cast<std::uint64_t>(buffers[i].data);
            entry.len = buffers[i].size;
            entry.bid = buffers[i].id;
            entry.resv = 0;
        }
        current_tail = static_cast<std::uint16_t>(current_tail + count);
        __atomic_store_n(tail(), current_tail, __ATOMIC_RELEASE);
        return true;
    }

    [[nodiscard]] void* ring() const noexcept {
        return ring_;
    }

    [[nodiscard]] std::size_t ring_size() const noexcept {
        return ring_size_;
    }

    [[nodiscard]] unsigned entries() const noexcept {
        return entries_;
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return ring_ != nullptr;
    }

    [[nodiscard]] static constexpr unsigned max_entries() noexcept {
        return 32768U;
    }

    [[nodiscard]] static constexpr std::size_t allocation_size(unsigned entries) noexcept {
        return sizeof(detail::IoProvidedBufferRingEntry) * static_cast<std::size_t>(entries);
    }

private:
    [[nodiscard]] std::uint16_t* tail() noexcept {
        return reinterpret_cast<std::uint16_t*>(
            static_cast<std::byte*>(ring_) + tail_offset);
    }

    [[nodiscard]] const std::uint16_t* tail() const noexcept {
        return reinterpret_cast<const std::uint16_t*>(
            static_cast<const std::byte*>(ring_) + tail_offset);
    }

    static constexpr std::size_t tail_offset =
        sizeof(std::uint64_t) + sizeof(std::uint32_t) + sizeof(std::uint16_t);

    void* ring_{nullptr};
    std::size_t ring_size_{0};
    unsigned entries_{0};
};
#endif

enum class IoStep : std::uint8_t {
    Ready,
    Pending,
    Closed,
    Failed,
};

struct IoStatus {
    IoStep step{IoStep::Failed};
    std::size_t bytes{0};
    int error{0};

    [[nodiscard]] static IoStatus ready(std::size_t byte_count) noexcept {
        return {IoStep::Ready, byte_count, 0};
    }

    [[nodiscard]] static IoStatus make_pending() noexcept {
        return {IoStep::Pending, 0, 0};
    }

    [[nodiscard]] static IoStatus make_closed() noexcept {
        return {IoStep::Closed, 0, 0};
    }

    [[nodiscard]] static IoStatus failed(int error_code) noexcept {
        return {IoStep::Failed, 0, error_code == 0 ? EIO : error_code};
    }

    [[nodiscard]] bool ready() const noexcept {
        return step == IoStep::Ready;
    }

    [[nodiscard]] bool pending() const noexcept {
        return step == IoStep::Pending;
    }

    [[nodiscard]] bool closed() const noexcept {
        return step == IoStep::Closed;
    }

    [[nodiscard]] bool failed() const noexcept {
        return step == IoStep::Failed;
    }
};

class UniqueFd {
public:
    UniqueFd() noexcept = default;
    explicit UniqueFd(int fd) noexcept : fd_(fd) {}

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            reset(std::exchange(other.fd_, -1));
        }
        return *this;
    }

    ~UniqueFd() {
        reset();
    }

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return fd_ >= 0;
    }

    [[nodiscard]] int release() noexcept {
        return std::exchange(fd_, -1);
    }

    void reset(int fd = -1) noexcept {
        if (fd_ == fd) {
            return;
        }
#if !defined(_WIN32)
        if (fd_ >= 0) {
            ::close(fd_);
        }
#endif
        fd_ = fd;
    }

private:
    int fd_{-1};
};

} // namespace af
