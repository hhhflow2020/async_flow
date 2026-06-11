#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include "af/span.hpp"
#include <string_view>
#include <utility>

#include "absl/container/inlined_vector.h"

namespace af {

namespace detail {

inline constexpr std::size_t io_buffer_pool_block_size = 16U * 1024U;
inline constexpr std::size_t io_buffer_pool_local_cache_capacity = 256U;
inline constexpr std::array<std::size_t, 4> io_buffer_pool_large_size_classes{
    32U * 1024U,
    64U * 1024U,
    128U * 1024U,
    256U * 1024U,
};
inline constexpr std::size_t io_buffer_pool_large_cache_capacity = 16U;
inline constexpr std::size_t buffer_chain_inline_capacity = 4U;

[[nodiscard]] inline std::byte *allocate_aligned_bytes(std::size_t capacity) {
    if (capacity == 0U) {
        return nullptr;
    }
    return static_cast<std::byte *>(::operator new[](capacity, std::align_val_t(64)));
}

inline void release_aligned_bytes(std::byte *data) noexcept {
    if (data != nullptr) {
        ::operator delete[](data, std::align_val_t(64));
    }
}

[[nodiscard]] inline std::size_t large_buffer_size_class_index(std::size_t size) noexcept {
    for (std::size_t i = 0; i < io_buffer_pool_large_size_classes.size(); ++i) {
        if (size <= io_buffer_pool_large_size_classes[i]) {
            return i;
        }
    }
    return io_buffer_pool_large_size_classes.size();
}

struct buffer_storage {
    buffer_storage() = default;

    buffer_storage(const buffer_storage &) = delete;
    buffer_storage &operator=(const buffer_storage &) = delete;

    buffer_storage(buffer_storage &&) = delete;
    buffer_storage &operator=(buffer_storage &&) = delete;

    [[nodiscard]] std::byte *data() noexcept {
        return data_;
    }

    [[nodiscard]] const std::byte *data() const noexcept {
        return data_;
    }

    [[nodiscard]] std::size_t capacity() const noexcept {
        return capacity_;
    }

    [[nodiscard]] std::size_t physical_capacity() const noexcept {
        return physical_capacity_;
    }

    void set_capacity(std::size_t capacity) noexcept {
        capacity_ = capacity;
    }

protected:
    std::byte *data_{nullptr};
    std::size_t capacity_{0};
    std::size_t physical_capacity_{0};
};

struct heap_buffer_storage final : buffer_storage {
    explicit heap_buffer_storage(std::size_t capacity) : bytes(allocate_aligned_bytes(capacity)) {
        data_ = bytes;
        capacity_ = capacity;
        physical_capacity_ = capacity;
    }

    ~heap_buffer_storage() {
        release_aligned_bytes(bytes);
    }

    std::byte *bytes{nullptr};
};

struct pooled_buffer_storage final : buffer_storage {
    pooled_buffer_storage() noexcept {
        data_ = bytes.data();
        capacity_ = bytes.size();
        physical_capacity_ = bytes.size();
    }

    alignas(64) std::array<std::byte, io_buffer_pool_block_size> bytes;
};

struct large_pooled_buffer_storage final : buffer_storage {
    explicit large_pooled_buffer_storage(std::size_t capacity)
        : bytes(allocate_aligned_bytes(capacity)) {
        data_ = bytes;
        capacity_ = capacity;
        physical_capacity_ = capacity;
    }

    ~large_pooled_buffer_storage() {
        release_aligned_bytes(bytes);
    }

    std::byte *bytes{nullptr};
};

class io_buffer_pool_cache {
public:
    io_buffer_pool_cache() = default;

    io_buffer_pool_cache(const io_buffer_pool_cache &) = delete;
    io_buffer_pool_cache &operator=(const io_buffer_pool_cache &) = delete;

    ~io_buffer_pool_cache() {
        while (size_ != 0U) {
            delete slots_[--size_];
            slots_[size_] = nullptr;
        }
    }

    [[nodiscard]] pooled_buffer_storage *acquire() {
        if (size_ != 0U) [[likely]] {
            pooled_buffer_storage *storage = slots_[--size_];
            slots_[size_] = nullptr;
            return storage;
        }
        return new pooled_buffer_storage();
    }

    void release(pooled_buffer_storage *storage) noexcept {
        if (storage == nullptr) {
            return;
        }
        if (size_ < slots_.size()) [[likely]] {
            slots_[size_++] = storage;
            return;
        }
        delete storage;
    }

private:
    std::array<pooled_buffer_storage *, io_buffer_pool_local_cache_capacity> slots_{};
    std::size_t size_{0};
};

class io_large_buffer_pool_cache {
public:
    io_large_buffer_pool_cache() = default;

    io_large_buffer_pool_cache(const io_large_buffer_pool_cache &) = delete;
    io_large_buffer_pool_cache &operator=(const io_large_buffer_pool_cache &) = delete;

    ~io_large_buffer_pool_cache() {
        for (std::size_t index = 0; index < slots_.size(); ++index) {
            while (sizes_[index] != 0U) {
                delete slots_[index][--sizes_[index]];
                slots_[index][sizes_[index]] = nullptr;
            }
        }
    }

    [[nodiscard]] large_pooled_buffer_storage *acquire(std::size_t index) {
        if (index >= io_buffer_pool_large_size_classes.size()) {
            return nullptr;
        }
        if (sizes_[index] != 0U) [[likely]] {
            large_pooled_buffer_storage *storage = slots_[index][--sizes_[index]];
            slots_[index][sizes_[index]] = nullptr;
            return storage;
        }
        return new large_pooled_buffer_storage(io_buffer_pool_large_size_classes[index]);
    }

    void release(std::size_t index, large_pooled_buffer_storage *storage) noexcept {
        if (storage == nullptr) {
            return;
        }
        if (index < slots_.size() && sizes_[index] < slots_[index].size()) [[likely]] {
            slots_[index][sizes_[index]++] = storage;
            return;
        }
        delete storage;
    }

private:
    using slot_list =
        std::array<large_pooled_buffer_storage *, io_buffer_pool_large_cache_capacity>;

    std::array<slot_list, io_buffer_pool_large_size_classes.size()> slots_{};
    std::array<std::size_t, io_buffer_pool_large_size_classes.size()> sizes_{};
};

[[nodiscard]] inline io_buffer_pool_cache &thread_io_buffer_pool() noexcept {
    thread_local io_buffer_pool_cache cache;
    return cache;
}

[[nodiscard]] inline io_large_buffer_pool_cache &thread_large_io_buffer_pool() noexcept {
    thread_local io_large_buffer_pool_cache cache;
    return cache;
}

[[nodiscard]] inline std::shared_ptr<buffer_storage>
make_exact_buffer_storage(std::size_t capacity) {
    return std::make_shared<heap_buffer_storage>(capacity);
}

[[nodiscard]] inline std::shared_ptr<buffer_storage>
make_pooled_buffer_storage(std::size_t capacity) {
    pooled_buffer_storage *storage = thread_io_buffer_pool().acquire();
    storage->set_capacity(capacity);
    return std::shared_ptr<buffer_storage>(storage, [](buffer_storage *raw) noexcept {
        thread_io_buffer_pool().release(static_cast<pooled_buffer_storage *>(raw));
    });
}

[[nodiscard]] inline std::shared_ptr<buffer_storage>
make_large_pooled_buffer_storage(std::size_t capacity) {
    const std::size_t index = large_buffer_size_class_index(capacity);
    if (index >= io_buffer_pool_large_size_classes.size()) {
        return make_exact_buffer_storage(capacity);
    }
    large_pooled_buffer_storage *storage = thread_large_io_buffer_pool().acquire(index);
    storage->set_capacity(capacity);
    return std::shared_ptr<buffer_storage>(storage, [index](buffer_storage *raw) noexcept {
        thread_large_io_buffer_pool().release(index,
                                              static_cast<large_pooled_buffer_storage *>(raw));
    });
}

[[nodiscard]] inline std::shared_ptr<buffer_storage> make_copy_buffer_storage(std::size_t size) {
    if (size == 0U) {
        return make_exact_buffer_storage(size);
    }
    if (size <= io_buffer_pool_block_size) [[likely]] {
        return make_pooled_buffer_storage(size);
    }
    return make_large_pooled_buffer_storage(size);
}

} // namespace detail

class buffer_view {
public:
    constexpr buffer_view() noexcept = default;

    constexpr buffer_view(const void *data, std::size_t size) noexcept
        : data_(static_cast<const std::byte *>(data)), size_(size) {}

    [[nodiscard]] constexpr const std::byte *data() const noexcept {
        return data_;
    }

    [[nodiscard]] constexpr std::size_t size() const noexcept {
        return size_;
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return size_ == 0U;
    }

    [[nodiscard]] constexpr af::span<const std::byte> span() const noexcept {
        return {data_, size_};
    }

    [[nodiscard]] std::string_view string_view() const noexcept {
        return {reinterpret_cast<const char *>(data_), size_};
    }

private:
    const std::byte *data_{nullptr};
    std::size_t size_{0};
};

class buffer {
public:
    static constexpr std::size_t npos = static_cast<std::size_t>(-1);

    buffer() = default;

    explicit buffer(std::size_t size)
        : storage_(detail::make_exact_buffer_storage(size)), size_(size) {}

    [[nodiscard]] static buffer with_capacity(std::size_t capacity, std::size_t headroom = 0U) {
        if (headroom > capacity) {
            headroom = capacity;
        }
        return buffer(detail::make_exact_buffer_storage(capacity), headroom, 0U);
    }

    [[nodiscard]] static buffer copy(buffer_view view) {
        buffer result(detail::make_copy_buffer_storage(view.size()), 0U, view.size());
        if (!view.empty()) {
            std::memcpy(result.mutable_data(), view.data(), view.size());
        }
        return result;
    }

    [[nodiscard]] static buffer copy(const void *data, std::size_t size) {
        return copy(buffer_view(data, size));
    }

    [[nodiscard]] std::byte *mutable_data() noexcept {
        return storage_ == nullptr ? nullptr : storage_->data() + offset_;
    }

    [[nodiscard]] std::byte *tail_data() noexcept {
        return storage_ == nullptr ? nullptr : storage_->data() + offset_ + size_;
    }

    [[nodiscard]] const std::byte *data() const noexcept {
        return storage_ == nullptr ? nullptr : storage_->data() + offset_;
    }

    [[nodiscard]] const std::byte *tail_data() const noexcept {
        return storage_ == nullptr ? nullptr : storage_->data() + offset_ + size_;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return size_;
    }

    [[nodiscard]] bool empty() const noexcept {
        return size_ == 0U;
    }

    [[nodiscard]] buffer_view view() const noexcept {
        return {data(), size_};
    }

    [[nodiscard]] std::size_t capacity() const noexcept {
        return storage_ == nullptr ? 0U : storage_->capacity();
    }

    [[nodiscard]] std::size_t headroom() const noexcept {
        return storage_ == nullptr ? 0U : offset_;
    }

    [[nodiscard]] std::size_t tailroom() const noexcept {
        if (storage_ == nullptr) {
            return 0U;
        }
        const std::size_t end = offset_ + size_;
        return end >= storage_->capacity() ? 0U : storage_->capacity() - end;
    }

    [[nodiscard]] bool try_append_uninitialized(std::size_t count) noexcept {
        if (count > tailroom()) {
            return false;
        }
        size_ += count;
        return true;
    }

    [[nodiscard]] std::byte *try_append_uninitialized_data(std::size_t count) noexcept {
        std::byte *tail = tail_data();
        return try_append_uninitialized(count) ? tail : nullptr;
    }

    [[nodiscard]] bool try_append(buffer_view view) noexcept {
        if (view.size() > tailroom()) {
            return false;
        }
        if (!view.empty()) {
            std::memcpy(tail_data(), view.data(), view.size());
            size_ += view.size();
        }
        return true;
    }

    [[nodiscard]] bool try_append(const void *data, std::size_t size) noexcept {
        return try_append(buffer_view(data, size));
    }

    [[nodiscard]] buffer slice(std::size_t offset, std::size_t count = npos) const noexcept {
        if (storage_ == nullptr || offset > size_) {
            return {};
        }
        const std::size_t available = size_ - offset;
        const std::size_t slice_size = count == npos || count > available ? available : count;
        return buffer(storage_, offset_ + offset, slice_size);
    }

    void remove_prefix(std::size_t count) noexcept {
        if (count >= size_) {
            offset_ += size_;
            size_ = 0;
            return;
        }
        offset_ += count;
        size_ -= count;
    }

    void remove_suffix(std::size_t count) noexcept {
        if (count >= size_) {
            size_ = 0;
            return;
        }
        size_ -= count;
    }

private:
    buffer(std::shared_ptr<detail::buffer_storage> storage, std::size_t offset,
           std::size_t size) noexcept
        : storage_(std::move(storage)), offset_(offset), size_(size) {}

    std::shared_ptr<detail::buffer_storage> storage_;
    std::size_t offset_{0};
    std::size_t size_{0};
};

class buffer_chain {
public:
    using buffer_list = absl::InlinedVector<buffer, detail::buffer_chain_inline_capacity>;

    void push_back(buffer buf) {
        if (!buf.empty()) {
            if (!total_dirty_) {
                total_bytes_ += buf.size();
            }
            buffers_.push_back(std::move(buf));
        }
    }

    [[nodiscard]] bool empty() const noexcept {
        return first_ >= buffers_.size();
    }

    [[nodiscard]] std::size_t size() const noexcept {
        ensure_total_clean();
        return total_bytes_;
    }

    void clear() noexcept {
        buffers_.clear();
        first_ = 0;
        total_bytes_ = 0;
        total_dirty_ = false;
    }

    void pop_front() noexcept {
        if (empty()) {
            return;
        }
        ensure_total_clean();
        total_bytes_ -= buffers_[first_].size();
        ++first_;
        compact_front_if_needed();
    }

    void remove_prefix(std::size_t count) noexcept {
        ensure_total_clean();
        while (count != 0U && !empty()) {
            buffer &front = buffers_[first_];
            const std::size_t consumed = count < front.size() ? count : front.size();
            front.remove_prefix(consumed);
            total_bytes_ -= consumed;
            count -= consumed;
            if (front.empty()) {
                ++first_;
                compact_front_if_needed();
            }
        }
    }

    [[nodiscard]] buffer_list &buffers() noexcept {
        compact_front();
        total_dirty_ = true;
        return buffers_;
    }

    [[nodiscard]] const buffer_list &buffers() const noexcept {
        compact_front();
        return buffers_;
    }

    [[nodiscard]] std::size_t fill_views(af::span<buffer_view> views) const noexcept {
        std::size_t count = 0;
        for (std::size_t i = first_; i < buffers_.size() && count < views.size(); ++i) {
            if (buffers_[i].empty()) {
                continue;
            }
            views[count] = buffers_[i].view();
            ++count;
        }
        return count;
    }

private:
    void ensure_total_clean() const noexcept {
        if (!total_dirty_) {
            return;
        }
        std::size_t total = 0;
        for (std::size_t i = first_; i < buffers_.size(); ++i) {
            const buffer &buf = buffers_[i];
            total += buf.size();
        }
        total_bytes_ = total;
        total_dirty_ = false;
    }

    void compact_front_if_needed() noexcept {
        if (first_ >= buffers_.size()) {
            buffers_.clear();
            first_ = 0;
            return;
        }
        if (first_ >= 32U && first_ * 2U >= buffers_.size()) {
            compact_front();
        }
    }

    void compact_front() const noexcept {
        if (first_ == 0U) {
            return;
        }
        if (first_ >= buffers_.size()) {
            buffers_.clear();
            first_ = 0;
            return;
        }
        buffers_.erase(buffers_.begin(), buffers_.begin() + static_cast<std::ptrdiff_t>(first_));
        first_ = 0;
    }

    mutable buffer_list buffers_;
    mutable std::size_t first_{0};
    mutable std::size_t total_bytes_{0};
    mutable bool total_dirty_{false};
};

} // namespace af
