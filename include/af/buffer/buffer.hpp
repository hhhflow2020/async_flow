#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string_view>
#include <utility>

#include "absl/container/inlined_vector.h"
#include "af/span.hpp"
#include "folly/io/IOBuf.h"

namespace af {

namespace detail {

inline constexpr std::size_t buffer_chain_inline_capacity = 4U;

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

    explicit buffer(std::size_t size) : iobuf_(make_iobuf(size, 0U)) {
        if (iobuf_ != nullptr) {
            iobuf_->append(size);
        }
    }

    buffer(const buffer &other) : iobuf_(clone_one(other.iobuf_)) {}
    buffer &operator=(const buffer &other) {
        if (this != &other) {
            iobuf_ = clone_one(other.iobuf_);
        }
        return *this;
    }

    buffer(buffer &&) noexcept = default;
    buffer &operator=(buffer &&) noexcept = default;

    [[nodiscard]] static buffer with_capacity(std::size_t capacity, std::size_t headroom = 0U) {
        if (headroom > capacity) {
            headroom = capacity;
        }
        return buffer(make_iobuf(capacity, headroom));
    }

    [[nodiscard]] static buffer copy(buffer_view view) {
        buffer result = with_capacity(view.size());
        if (!view.empty()) {
            static_cast<void>(result.try_append(view));
        }
        return result;
    }

    [[nodiscard]] static buffer copy(const void *data, std::size_t size) {
        return copy(buffer_view(data, size));
    }

    [[nodiscard]] std::byte *mutable_data() noexcept {
        return ensure_writable() ? to_byte(iobuf_->writableData()) : nullptr;
    }

    [[nodiscard]] std::byte *tail_data() noexcept {
        return ensure_writable() ? to_byte(iobuf_->writableTail()) : nullptr;
    }

    [[nodiscard]] const std::byte *data() const noexcept {
        return iobuf_ == nullptr ? nullptr : to_const_byte(iobuf_->data());
    }

    [[nodiscard]] const std::byte *tail_data() const noexcept {
        return iobuf_ == nullptr ? nullptr : to_const_byte(iobuf_->tail());
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return iobuf_ == nullptr ? 0U : iobuf_->length();
    }

    [[nodiscard]] bool empty() const noexcept {
        return size() == 0U;
    }

    [[nodiscard]] buffer_view view() const noexcept {
        return {data(), size()};
    }

    [[nodiscard]] std::size_t capacity() const noexcept {
        return iobuf_ == nullptr ? 0U : iobuf_->capacity();
    }

    [[nodiscard]] std::size_t headroom() const noexcept {
        return iobuf_ == nullptr ? 0U : iobuf_->headroom();
    }

    [[nodiscard]] std::size_t tailroom() const noexcept {
        return iobuf_ == nullptr ? 0U : iobuf_->tailroom();
    }

    [[nodiscard]] bool try_append_uninitialized(std::size_t count) noexcept {
        if (count > tailroom() || !ensure_writable() || count > iobuf_->tailroom()) {
            return false;
        }
        iobuf_->append(count);
        return true;
    }

    [[nodiscard]] std::byte *try_append_uninitialized_data(std::size_t count) noexcept {
        if (count > tailroom() || !ensure_writable() || count > iobuf_->tailroom()) {
            return nullptr;
        }
        std::byte *tail = to_byte(iobuf_->writableTail());
        iobuf_->append(count);
        return tail;
    }

    [[nodiscard]] bool try_append(buffer_view view) noexcept {
        if (view.size() > tailroom() || !ensure_writable() || view.size() > iobuf_->tailroom()) {
            return false;
        }
        if (!view.empty()) {
            std::memcpy(iobuf_->writableTail(), view.data(), view.size());
            iobuf_->append(view.size());
        }
        return true;
    }

    [[nodiscard]] bool try_append(const void *data, std::size_t size) noexcept {
        return try_append(buffer_view(data, size));
    }

    [[nodiscard]] buffer slice(std::size_t offset, std::size_t count = npos) const {
        if (iobuf_ == nullptr || offset > size()) {
            return {};
        }
        std::unique_ptr<folly::IOBuf> cloned = iobuf_->cloneOne();
        if (offset != 0U) {
            cloned->trimStart(offset);
        }
        const std::size_t available = cloned->length();
        const std::size_t slice_size = count == npos || count > available ? available : count;
        if (slice_size < available) {
            cloned->trimEnd(available - slice_size);
        }
        return buffer(std::move(cloned));
    }

    void remove_prefix(std::size_t count) noexcept {
        if (iobuf_ == nullptr) {
            return;
        }
        const std::size_t consumed = std::min(count, iobuf_->length());
        iobuf_->trimStart(consumed);
    }

    void remove_suffix(std::size_t count) noexcept {
        if (iobuf_ == nullptr) {
            return;
        }
        const std::size_t consumed = std::min(count, iobuf_->length());
        iobuf_->trimEnd(consumed);
    }

private:
    explicit buffer(std::unique_ptr<folly::IOBuf> iobuf) noexcept : iobuf_(std::move(iobuf)) {}

    [[nodiscard]] static std::unique_ptr<folly::IOBuf> make_iobuf(std::size_t capacity,
                                                                  std::size_t headroom) {
        if (capacity == 0U) {
            return nullptr;
        }
        std::unique_ptr<folly::IOBuf> iobuf = folly::IOBuf::createCombined(capacity);
        if (iobuf->capacity() > capacity) {
            iobuf->trimWritableTail(iobuf->capacity() - capacity);
        }
        if (headroom != 0U) {
            iobuf->advance(headroom);
        }
        return iobuf;
    }

    [[nodiscard]] static std::unique_ptr<folly::IOBuf>
    clone_one(const std::unique_ptr<folly::IOBuf> &iobuf) {
        return iobuf == nullptr ? nullptr : iobuf->cloneOne();
    }

    [[nodiscard]] bool ensure_writable() noexcept {
        if (iobuf_ == nullptr) {
            return false;
        }
        try {
            iobuf_->unshareOne();
            return true;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] static std::byte *to_byte(std::uint8_t *data) noexcept {
        return reinterpret_cast<std::byte *>(data);
    }

    [[nodiscard]] static const std::byte *to_const_byte(const std::uint8_t *data) noexcept {
        return reinterpret_cast<const std::byte *>(data);
    }

    std::unique_ptr<folly::IOBuf> iobuf_;
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
