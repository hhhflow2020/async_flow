#if !defined(AF_IO_TYPES_FRAGMENT_INCLUDE)
#error "io_types_provided_buffer_fragment.hpp is an io_types implementation fragment"
#endif

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
