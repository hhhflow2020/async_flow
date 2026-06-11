#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "af/detail/config.hpp"
#include "af/memory/cache_line.hpp"

namespace af::detail {

template <typename T, std::size_t chunk_size_v, bool cache_allocated_slot_index_v,
          std::size_t local_cache_capacity_v>
struct object_pool_block_layout {
    using slot_index_type = std::uint16_t;

    static_assert(local_cache_capacity_v > 0);
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                  "ObjectPool tagged free stack requires lock-free 64-bit atomics");
    static_assert(std::atomic<slot_index_type>::is_always_lock_free,
                  "ObjectPool free-list slot links require lock-free index atomics");

    static constexpr slot_index_type null_slot_index = std::numeric_limits<slot_index_type>::max();
    static_assert(chunk_size_v < static_cast<std::size_t>(null_slot_index),
                  "ObjectPool chunk size must leave 48 bits for the free-list ABA tag");

    static constexpr bool cache_allocated_slot_index = cache_allocated_slot_index_v;
    static constexpr std::size_t local_cache_capacity = local_cache_capacity_v;
    static constexpr std::size_t slot_index_bits = sizeof(slot_index_type) * 8U;
    static constexpr std::uint64_t slot_index_mask = (std::uint64_t{1} << slot_index_bits) - 1U;

    static constexpr std::size_t align_up(std::size_t value, std::size_t alignment) noexcept {
        return ((value + alignment - 1U) / alignment) * alignment;
    }

    static constexpr std::size_t slot_align = alignof(T) > hardware_cache_line_size
                                                  ? alignof(T)
                                                  : hardware_cache_line_size;
    static constexpr std::size_t slot_header_size =
        sizeof(void *) + sizeof(std::atomic<slot_index_type>);
    static constexpr std::size_t compact_storage_offset = align_up(slot_header_size, alignof(T));
    static constexpr bool compact_slot =
        compact_storage_offset + sizeof(T) <= hardware_cache_line_size;
    static constexpr std::size_t noncompact_storage_offset =
        align_up(hardware_cache_line_size, alignof(T));
    static constexpr std::size_t storage_offset =
        compact_slot ? compact_storage_offset : noncompact_storage_offset;
    static constexpr std::size_t storage_align = compact_slot ? alignof(T) : slot_align;
    static constexpr std::size_t slot_payload_size = storage_offset + sizeof(T);
    static constexpr std::size_t slot_size = align_up(slot_payload_size, slot_align);
    static constexpr std::size_t storage_size = slot_size - storage_offset;

    struct block;

    struct alignas(slot_align) slot {
        block *owner{nullptr};
        std::atomic<slot_index_type> next_free{null_slot_index};
        alignas(storage_align) std::byte storage[storage_size];
    };

    static_assert(std::is_standard_layout_v<slot>,
                  "ObjectPool slot lookup requires standard-layout slots");
    static_assert(offsetof(slot, storage) == storage_offset,
                  "ObjectPool slot storage offset must match computed layout");
    static_assert(offsetof(slot, storage) % alignof(T) == 0U,
                  "ObjectPool slot storage must satisfy T alignment");
    static_assert(sizeof(slot) % hardware_cache_line_size == 0U,
                  "ObjectPool slots must not share cache lines");

    struct block {
        explicit block() {
            for (std::size_t i = 0; i < chunk_size_v; ++i) {
                auto &entry = slots[i];
                entry.owner = this;
                const auto next =
                    i + 1U < chunk_size_v ? static_cast<slot_index_type>(i + 1U) : null_slot_index;
                entry.next_free.store(next, std::memory_order_relaxed);
            }
            free_head.store(pack_free_head(0U, 0U), std::memory_order_relaxed);
        }

        [[nodiscard]] std::size_t try_pop_many(slot **out, std::size_t max_count) noexcept {
            if (max_count == 0U) {
                return 0;
            }
            AF_ASSERT(max_count <= local_cache_capacity);
            slot_index_type popped_indices[cache_allocated_slot_index ? local_cache_capacity : 1U];
            std::uint64_t head = free_head.load(std::memory_order_acquire);
            for (;;) {
                slot_index_type index = free_head_index(head);
                if (index == null_slot_index) {
                    return 0;
                }

                std::size_t count = 0;
                while (count < max_count && index != null_slot_index) {
                    slot &entry = slots[index];
                    out[count] = &entry;
                    if constexpr (cache_allocated_slot_index) {
                        popped_indices[count] = index;
                    }
                    ++count;
                    index = entry.next_free.load(std::memory_order_relaxed);
                }

                const std::uint64_t desired = pack_free_head(index, free_head_version(head) + 1U);
                if (free_head.compare_exchange_weak(head, desired, std::memory_order_acquire,
                                                    std::memory_order_acquire)) {
                    if constexpr (cache_allocated_slot_index) {
                        for (std::size_t i = 0; i < count; ++i) {
                            out[i]->next_free.store(popped_indices[i], std::memory_order_relaxed);
                        }
                    }
                    return count;
                }
            }
        }

        void push(slot *entry) noexcept {
            const slot_index_type index = released_slot_index(entry);
            std::uint64_t head = free_head.load(std::memory_order_relaxed);
            for (;;) {
                entry->next_free.store(free_head_index(head), std::memory_order_relaxed);
                const std::uint64_t desired = pack_free_head(index, free_head_version(head) + 1U);
                if (free_head.compare_exchange_weak(head, desired, std::memory_order_release,
                                                    std::memory_order_relaxed)) {
                    return;
                }
            }
        }

        void push_many(slot **pushed_slots, std::size_t count) noexcept {
            AF_ASSERT(count != 0U);
            const slot_index_type first = released_slot_index(pushed_slots[0]);
            for (std::size_t i = 1; i < count; ++i) {
                pushed_slots[i - 1]->next_free.store(released_slot_index(pushed_slots[i]),
                                                     std::memory_order_relaxed);
            }

            slot *last = pushed_slots[count - 1U];
            std::uint64_t head = free_head.load(std::memory_order_relaxed);
            for (;;) {
                last->next_free.store(free_head_index(head), std::memory_order_relaxed);
                const std::uint64_t desired = pack_free_head(first, free_head_version(head) + 1U);
                if (free_head.compare_exchange_weak(head, desired, std::memory_order_release,
                                                    std::memory_order_relaxed)) {
                    return;
                }
            }
        }

        block *next{nullptr};
        alignas(hardware_cache_line_size) std::atomic<std::uint64_t> free_head{
            pack_free_head(null_slot_index, 0U)};
        slot slots[chunk_size_v];

    private:
        [[nodiscard]] static constexpr std::uint64_t
        pack_free_head(slot_index_type index, std::uint64_t version) noexcept {
            return (version << slot_index_bits) | static_cast<std::uint64_t>(index);
        }

        [[nodiscard]] static constexpr slot_index_type
        free_head_index(std::uint64_t head) noexcept {
            return static_cast<slot_index_type>(head & slot_index_mask);
        }

        [[nodiscard]] static constexpr std::uint64_t
        free_head_version(std::uint64_t head) noexcept {
            return head >> slot_index_bits;
        }

        [[nodiscard]] slot_index_type slot_index(const slot *entry) const noexcept {
            const auto index = static_cast<std::size_t>(entry - slots);
            AF_ASSERT(index < chunk_size_v);
            return static_cast<slot_index_type>(index);
        }

        [[nodiscard]] slot_index_type released_slot_index(const slot *entry) const noexcept {
            if constexpr (cache_allocated_slot_index) {
                const slot_index_type index = entry->next_free.load(std::memory_order_relaxed);
                AF_ASSERT(index != null_slot_index);
                return index;
            }
            return slot_index(entry);
        }
    };
};

} // namespace af::detail
