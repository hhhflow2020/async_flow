#pragma once

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
    Buffer() = default;

    explicit Buffer(std::size_t size)
        : storage_(std::make_shared<std::vector<std::byte>>(size)), size_(size) {}

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

    [[nodiscard]] const std::byte *data() const noexcept {
        return storage_ == nullptr ? nullptr : storage_->data() + offset_;
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

    void remove_prefix(std::size_t count) noexcept {
        if (count >= size_) {
            offset_ += size_;
            size_ = 0;
            return;
        }
        offset_ += count;
        size_ -= count;
    }

private:
    std::shared_ptr<std::vector<std::byte>> storage_;
    std::size_t offset_{0};
    std::size_t size_{0};
};

class BufferChain {
public:
    void push_back(Buffer buffer) {
        if (!buffer.empty()) {
            buffers_.push_back(std::move(buffer));
        }
    }

    [[nodiscard]] bool empty() const noexcept {
        return buffers_.empty();
    }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t result = 0;
        for (const Buffer &buffer : buffers_) {
            result += buffer.size();
        }
        return result;
    }

    [[nodiscard]] std::vector<Buffer> &buffers() noexcept {
        return buffers_;
    }

    [[nodiscard]] const std::vector<Buffer> &buffers() const noexcept {
        return buffers_;
    }

private:
    std::vector<Buffer> buffers_;
};

} // namespace af
