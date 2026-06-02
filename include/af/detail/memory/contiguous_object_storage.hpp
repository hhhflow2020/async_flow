#pragma once

#include <cstddef>
#include <new>
#include <utility>

#include "af/detail/config.hpp"

namespace af::detail {

template <typename T> class ContiguousObjectStorage {
public:
    ContiguousObjectStorage() noexcept = default;
    ContiguousObjectStorage(const ContiguousObjectStorage &) = delete;
    ContiguousObjectStorage &operator=(const ContiguousObjectStorage &) = delete;

    ContiguousObjectStorage(ContiguousObjectStorage &&other) noexcept {
        move_from(other);
    }

    ContiguousObjectStorage &operator=(ContiguousObjectStorage &&other) noexcept {
        if (this == &other) {
            return *this;
        }

        clear();
        release_storage();
        move_from(other);
        return *this;
    }

    ~ContiguousObjectStorage() {
        clear();
        release_storage();
    }

    void reserve_exact(std::size_t capacity) {
        clear();
        release_storage();
        if (capacity == 0) {
            return;
        }

        data_ =
            static_cast<T *>(::operator new(sizeof(T) * capacity, std::align_val_t{alignof(T)}));
        capacity_ = capacity;
    }

    template <typename... Args> T &emplace_back(Args &&...args) {
        AF_ASSERT(size_ < capacity_);
        ::new (static_cast<void *>(data_ + size_)) T(std::forward<Args>(args)...);
        return data_[size_++];
    }

    void clear() noexcept {
        while (size_ != 0) {
            --size_;
            data_[size_].~T();
        }
    }

    [[nodiscard]] T *operator[](std::size_t index) noexcept {
        AF_ASSERT(index < size_);
        return data_ + index;
    }

    [[nodiscard]] const T *operator[](std::size_t index) const noexcept {
        AF_ASSERT(index < size_);
        return data_ + index;
    }

    [[nodiscard]] T *begin() noexcept {
        return data_;
    }

    [[nodiscard]] T *end() noexcept {
        return data_ + size_;
    }

    [[nodiscard]] const T *begin() const noexcept {
        return data_;
    }

    [[nodiscard]] const T *end() const noexcept {
        return data_ + size_;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return size_;
    }

    [[nodiscard]] bool empty() const noexcept {
        return size_ == 0;
    }

private:
    void release_storage() noexcept {
        if (data_ == nullptr) {
            return;
        }

        ::operator delete(data_, std::align_val_t{alignof(T)});
        data_ = nullptr;
        capacity_ = 0;
    }

    void move_from(ContiguousObjectStorage &other) noexcept {
        data_ = other.data_;
        size_ = other.size_;
        capacity_ = other.capacity_;

        other.data_ = nullptr;
        other.size_ = 0;
        other.capacity_ = 0;
    }

    T *data_{nullptr};
    std::size_t size_{0};
    std::size_t capacity_{0};
};

} // namespace af::detail
