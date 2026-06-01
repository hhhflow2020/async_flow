#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>

#include "af/detail/config.hpp"

namespace af::detail {

template <std::size_t ThreadCount> class ReadySourceSet {
    static_assert(ThreadCount > 0, "ReadySourceSet requires at least one runtime thread");

    static constexpr std::size_t bits_per_word = 64;

    struct alignas(hardware_cache_line_size) Word {
        std::atomic<std::uint64_t> bits{0};
    };

public:
    static constexpr std::size_t word_count = (ThreadCount + bits_per_word - 1U) / bits_per_word;

    ReadySourceSet() = default;
    ReadySourceSet(const ReadySourceSet &) = delete;
    ReadySourceSet &operator=(const ReadySourceSet &) = delete;

    void mark(std::uint16_t source) noexcept {
        const std::size_t word = word_index(source);
        const std::uint64_t bit = source_bit(source);
        if ((words_[word].bits.load(std::memory_order_relaxed) & bit) == 0U) {
            words_[word].bits.fetch_or(bit, std::memory_order_release);
        }
    }

    void clear(std::uint16_t source) noexcept {
        words_[word_index(source)].bits.fetch_and(~source_bit(source), std::memory_order_acq_rel);
    }

    [[nodiscard]] std::uint64_t load_word(std::size_t word) const noexcept {
        return words_[word].bits.load(std::memory_order_acquire) & valid_mask(word);
    }

    [[nodiscard]] static constexpr std::uint16_t word_base(std::size_t word) noexcept {
        return static_cast<std::uint16_t>(word * bits_per_word);
    }

    [[nodiscard]] static constexpr std::uint64_t valid_mask(std::size_t word) noexcept {
        if (word + 1U != word_count || ThreadCount % bits_per_word == 0U) {
            return ~0ULL;
        }
        return (1ULL << (ThreadCount % bits_per_word)) - 1ULL;
    }

private:
    [[nodiscard]] static constexpr std::size_t word_index(std::uint16_t source) noexcept {
        return static_cast<std::size_t>(source) / bits_per_word;
    }

    [[nodiscard]] static constexpr std::uint64_t source_bit(std::uint16_t source) noexcept {
        return 1ULL << (static_cast<std::size_t>(source) % bits_per_word);
    }

    std::array<Word, word_count> words_{};
};

} // namespace af::detail
