#pragma once

#include <array>
#include <cstddef>
#include <type_traits>

namespace af {

template <typename T> class span {
public:
    using element_type = T;
    using value_type = typename std::remove_cv<T>::type;
    using pointer = T *;
    using reference = T &;
    using iterator = pointer;

    constexpr span() noexcept = default;

    constexpr span(pointer data, std::size_t size) noexcept : data_(data), size_(size) {}

    template <
        typename value_t, std::size_t size_v,
        typename = typename std::enable_if<std::is_convertible<value_t *, pointer>::value>::type>
    constexpr span(std::array<value_t, size_v> &values) noexcept
        : data_(values.data()), size_(size_v) {}

    template <typename value_t, std::size_t size_v,
              typename = typename std::enable_if<
                  std::is_convertible<const value_t *, pointer>::value>::type>
    constexpr span(const std::array<value_t, size_v> &values) noexcept
        : data_(values.data()), size_(size_v) {}

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

    [[nodiscard]] constexpr span subspan(std::size_t offset) const noexcept {
        return offset >= size_ ? span{} : span{data_ + offset, size_ - offset};
    }

    [[nodiscard]] constexpr span subspan(std::size_t offset, std::size_t count) const noexcept {
        if (offset >= size_) {
            return {};
        }
        const std::size_t remaining = size_ - offset;
        return span{data_ + offset, count < remaining ? count : remaining};
    }

private:
    pointer data_{nullptr};
    std::size_t size_{0};
};

} // namespace af
