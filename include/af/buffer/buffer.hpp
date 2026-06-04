#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace af {

class BufferView {
public:
    constexpr BufferView() noexcept = default;

    constexpr BufferView(const void *data, std::size_t size) noexcept
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

    [[nodiscard]] constexpr std::span<const std::byte> span() const noexcept {
        return {data_, size_};
    }

    [[nodiscard]] std::string_view string_view() const noexcept {
        return {reinterpret_cast<const char *>(data_), size_};
    }

private:
    const std::byte *data_{nullptr};
    std::size_t size_{0};
};

class Buffer {
public:
    static constexpr std::size_t npos = static_cast<std::size_t>(-1);

    Buffer() = default;

    explicit Buffer(std::size_t size)
        : storage_(std::make_shared<std::vector<std::byte>>(size)), size_(size) {}

    [[nodiscard]] static Buffer with_capacity(std::size_t capacity, std::size_t headroom = 0U) {
        if (headroom > capacity) {
            headroom = capacity;
        }
        return Buffer(std::make_shared<std::vector<std::byte>>(capacity), headroom, 0U);
    }

    [[nodiscard]] static Buffer copy(BufferView view) {
        Buffer buffer(view.size());
        if (!view.empty()) {
            std::memcpy(buffer.mutable_data(), view.data(), view.size());
        }
        return buffer;
    }

    [[nodiscard]] static Buffer copy(const void *data, std::size_t size) {
        return copy(BufferView(data, size));
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

    [[nodiscard]] BufferView view() const noexcept {
        return {data(), size_};
    }

    [[nodiscard]] std::size_t capacity() const noexcept {
        return storage_ == nullptr ? 0U : storage_->size();
    }

    [[nodiscard]] std::size_t headroom() const noexcept {
        return storage_ == nullptr ? 0U : offset_;
    }

    [[nodiscard]] std::size_t tailroom() const noexcept {
        if (storage_ == nullptr) {
            return 0U;
        }
        const std::size_t end = offset_ + size_;
        return end >= storage_->size() ? 0U : storage_->size() - end;
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

    [[nodiscard]] bool try_append(BufferView view) noexcept {
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
        return try_append(BufferView(data, size));
    }

    [[nodiscard]] Buffer slice(std::size_t offset, std::size_t count = npos) const noexcept {
        if (storage_ == nullptr || offset > size_) {
            return {};
        }
        const std::size_t available = size_ - offset;
        const std::size_t slice_size = count == npos || count > available ? available : count;
        return Buffer(storage_, offset_ + offset, slice_size);
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
    Buffer(std::shared_ptr<std::vector<std::byte>> storage, std::size_t offset,
           std::size_t size) noexcept
        : storage_(std::move(storage)), offset_(offset), size_(size) {}

    std::shared_ptr<std::vector<std::byte>> storage_;
    std::size_t offset_{0};
    std::size_t size_{0};
};

class BufferChain {
public:
    void push_back(Buffer buffer) {
        if (!buffer.empty()) {
            if (!total_dirty_) {
                total_bytes_ += buffer.size();
            }
            buffers_.push_back(std::move(buffer));
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
            Buffer &front = buffers_[first_];
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

    [[nodiscard]] std::vector<Buffer> &buffers() noexcept {
        compact_front();
        total_dirty_ = true;
        return buffers_;
    }

    [[nodiscard]] const std::vector<Buffer> &buffers() const noexcept {
        compact_front();
        return buffers_;
    }

    [[nodiscard]] std::size_t fill_views(std::span<BufferView> views) const noexcept {
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
            const Buffer &buffer = buffers_[i];
            total += buffer.size();
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

    mutable std::vector<Buffer> buffers_;
    mutable std::size_t first_{0};
    mutable std::size_t total_bytes_{0};
    mutable bool total_dirty_{false};
};

} // namespace af
