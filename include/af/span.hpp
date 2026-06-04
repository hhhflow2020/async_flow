#pragma once

#include <array>
#include <cstddef>
#include <type_traits>

namespace af {

template <typename T> class Span {
public:
    using element_type = T;
    using value_type = typename std::remove_cv<T>::type;
    using pointer = T *;
    using reference = T &;
    using iterator = pointer;

    constexpr Span() noexcept = default;

    constexpr Span(pointer data, std::size_t size) noexcept : data_(data), size_(size) {}

    template <typename U, std::size_t SizeV,
              typename = typename std::enable_if<std::is_convertible<U *, pointer>::value>::type>
    constexpr Span(std::array<U, SizeV> &values) noexcept : data_(values.data()), size_(SizeV) {}

    template <typename U, std::size_t SizeV,
              typename = typename std::enable_if<std::is_convertible<const U *, pointer>::value>::type>
    constexpr Span(const std::array<U, SizeV> &values) noexcept : data_(values.data()), size_(SizeV) {}

    [[nodiscard]] constexpr pointer data() const noexcept {
        return data_;
    }

    [[nodiscard]] constexpr std::size_t size() const noexcept {
        return size_;
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return size_ == 0U;
    }

    [[nodiscard]] constexpr reference front() const noexcept {
        return data_[0];
    }

    [[nodiscard]] constexpr reference operator[](std::size_t index) const noexcept {
        return data_[index];
    }

    [[nodiscard]] constexpr iterator begin() const noexcept {
        return data_;
    }

    [[nodiscard]] constexpr iterator end() const noexcept {
        return data_ + size_;
    }

    [[nodiscard]] constexpr Span subspan(std::size_t offset) const noexcept {
        return offset >= size_ ? Span{} : Span{data_ + offset, size_ - offset};
    }

    [[nodiscard]] constexpr Span subspan(std::size_t offset, std::size_t count) const noexcept {
        if (offset >= size_) {
            return {};
        }
        const std::size_t remaining = size_ - offset;
        return Span{data_ + offset, count < remaining ? count : remaining};
    }

private:
    pointer data_{nullptr};
    std::size_t size_{0};
};

} // namespace af
