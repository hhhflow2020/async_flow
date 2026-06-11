#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <type_traits>

#include "af/detail/config.hpp"
#include "af/thread_kind.hpp"

namespace af {

template <typename LayoutT> class thread_id {
public:
    using layout = LayoutT;
    using Layout = layout;

    constexpr thread_id() noexcept = default;

    [[nodiscard]] static constexpr thread_id from_index(std::uint16_t index) noexcept {
        return thread_id(index);
    }

    [[nodiscard]] constexpr std::uint16_t index() const noexcept {
        return index_;
    }

    [[nodiscard]] explicit constexpr operator std::uint16_t() const noexcept {
        return index_;
    }

    [[nodiscard]] friend constexpr bool operator==(thread_id lhs, thread_id rhs) noexcept {
        return lhs.index_ == rhs.index_;
    }

    [[nodiscard]] friend constexpr bool operator!=(thread_id lhs, thread_id rhs) noexcept {
        return !(lhs == rhs);
    }

private:
    explicit constexpr thread_id(std::uint16_t index) noexcept : index_(index) {}

    std::uint16_t index_{std::numeric_limits<std::uint16_t>::max()};
};

template <typename LayoutT> using ThreadId = thread_id<LayoutT>;

template <typename ThreadT, std::uint16_t BeginIndexV, std::uint16_t CountV>
class static_thread_group {
    static_assert(CountV > 0, "thread groups must contain at least one thread");

public:
    using thread = ThreadT;
    using Thread = thread;

    static constexpr std::uint16_t begin_index = BeginIndexV;
    static constexpr std::uint16_t count = CountV;
    static constexpr std::uint16_t end_index = BeginIndexV + CountV;

    [[nodiscard]] static constexpr thread begin() noexcept {
        return thread::from_index(begin_index);
    }

    [[nodiscard]] static constexpr thread end() noexcept {
        return thread::from_index(end_index);
    }

    [[nodiscard]] static constexpr thread front() noexcept {
        return begin();
    }

    [[nodiscard]] static constexpr thread back() noexcept {
        return thread::from_index(static_cast<std::uint16_t>(end_index - 1U));
    }

    [[nodiscard]] static constexpr thread at(std::uint16_t offset) noexcept {
        AF_ASSERT(offset < count);
        return thread::from_index(static_cast<std::uint16_t>(begin_index + offset));
    }

    template <std::uint16_t Offset> [[nodiscard]] static constexpr thread at() noexcept {
        static_assert(Offset < count, "thread group offset is out of range");
        return thread::from_index(static_cast<std::uint16_t>(begin_index + Offset));
    }

    template <typename Key> [[nodiscard]] static constexpr thread shard(Key key) noexcept {
        const auto value = static_cast<std::uint64_t>(key);
        return at(static_cast<std::uint16_t>(value % count));
    }

    [[nodiscard]] static constexpr bool contains(thread value) noexcept {
        const std::uint16_t index = value.index();
        return index >= begin_index && index < end_index;
    }

    [[nodiscard]] static constexpr std::uint16_t offset_of(thread value) noexcept {
        AF_ASSERT(contains(value));
        return static_cast<std::uint16_t>(value.index() - begin_index);
    }
};

template <typename ThreadT, std::uint16_t BeginIndexV, std::uint16_t CountV>
using ThreadGroup = static_thread_group<ThreadT, BeginIndexV, CountV>;

template <typename TagT, std::uint16_t CountV, af::thread_kind KindV = af::thread_kind::cpu>
struct thread_group_spec {
    static_assert(CountV > 0, "thread groups must contain at least one thread");

    using tag = TagT;
    using Tag = tag;

    static constexpr std::uint16_t count = CountV;
    static constexpr af::thread_kind kind = KindV;

    constexpr explicit thread_group_spec(const char *name_value = "worker") noexcept
        : name((name_value == nullptr || name_value[0] == '\0') ? "worker" : name_value) {}

    [[nodiscard]] constexpr const char *group_name() const noexcept {
        return name;
    }

    const char *name{"worker"};
};

template <typename TagT, std::uint16_t CountV, af::thread_kind KindV = af::thread_kind::cpu>
using ThreadGroupSpec = thread_group_spec<TagT, CountV, KindV>;

namespace detail {

template <typename> inline constexpr bool thread_layout_always_false_v = false;

template <typename TagT, std::uint16_t CountV> struct thread_group_shape {
    using tag = TagT;
    using Tag = tag;

    static constexpr std::uint16_t count = CountV;
};

template <typename... Groups> struct thread_layout_shape {};

struct thread_layout_entry {
    af::thread_kind kind{af::thread_kind::cpu};
    const char *name{"worker"};
    std::uint16_t group_offset{0};
};

template <typename TagT, std::uint16_t CountV>
using ThreadGroupShape = thread_group_shape<TagT, CountV>;

template <typename... Groups> using ThreadLayoutShape = thread_layout_shape<Groups...>;

using ThreadLayoutEntry = thread_layout_entry;

template <typename Tag, typename... Specs> constexpr std::size_t thread_group_tag_count() {
    return (std::size_t{0} + ... +
            (std::is_same_v<Tag, typename Specs::tag> ? std::size_t{1} : std::size_t{0}));
}

template <typename Tag> constexpr std::uint16_t missing_thread_group_tag() {
    static_assert(thread_layout_always_false_v<Tag>, "thread group tag is not part of this layout");
    return 0;
}

template <typename Tag, typename First, typename... Rest>
constexpr std::uint16_t thread_group_begin_index(std::uint16_t offset = 0) {
    if constexpr (std::is_same_v<Tag, typename First::tag>) {
        return offset;
    } else if constexpr (sizeof...(Rest) > 0) {
        return thread_group_begin_index<Tag, Rest...>(
            static_cast<std::uint16_t>(offset + First::count));
    } else {
        return missing_thread_group_tag<Tag>();
    }
}

template <typename Tag, typename First, typename... Rest>
constexpr std::uint16_t thread_group_count() {
    if constexpr (std::is_same_v<Tag, typename First::tag>) {
        return First::count;
    } else if constexpr (sizeof...(Rest) > 0) {
        return thread_group_count<Tag, Rest...>();
    } else {
        return missing_thread_group_tag<Tag>();
    }
}

} // namespace detail

template <typename... Specs> class static_thread_layout {
    static_assert(sizeof...(Specs) > 0, "thread_layout requires at least one thread group");
    static_assert(((detail::thread_group_tag_count<typename Specs::tag, Specs...>() == 1) && ...),
                  "thread group tags must be unique inside one layout");

public:
    using thread_shape = detail::thread_layout_shape<
        detail::thread_group_shape<typename Specs::tag, Specs::count>...>;
    using ThreadShape = thread_shape;
    using thread = thread_id<thread_shape>;
    using Thread = thread;

    static constexpr std::uint64_t total_thread_count =
        (std::uint64_t{0} + ... + static_cast<std::uint64_t>(Specs::count));
    static_assert(total_thread_count > 0, "thread_layout requires at least one thread");
    static_assert(total_thread_count <= std::numeric_limits<std::uint16_t>::max(),
                  "thread_layout thread_count must fit in 16-bit thread indexes");

    static constexpr std::uint16_t thread_count = static_cast<std::uint16_t>(total_thread_count);
    static constexpr std::uint16_t invalid_thread_index = thread_count;

    constexpr explicit static_thread_layout(Specs... specs) noexcept
        : entry_table_(make_entry_table(specs...)) {}

    template <typename Tag> [[nodiscard]] static constexpr auto group() noexcept {
        constexpr std::uint16_t begin = detail::thread_group_begin_index<Tag, Specs...>();
        constexpr std::uint16_t count = detail::thread_group_count<Tag, Specs...>();
        return static_thread_group<thread, begin, count>{};
    }

    template <typename Tag> [[nodiscard]] static constexpr std::uint16_t group_begin_index() {
        return detail::thread_group_begin_index<Tag, Specs...>();
    }

    template <typename Tag> [[nodiscard]] static constexpr std::uint16_t group_count() {
        return detail::thread_group_count<Tag, Specs...>();
    }

    [[nodiscard]] constexpr af::thread_kind thread_kind(thread value) const noexcept {
        const std::uint16_t index = value.index();
        if (index >= thread_count) {
            return af::thread_kind::cpu;
        }
        return entry_table_[index].kind;
    }

    [[nodiscard]] std::string_view thread_name(thread value) const noexcept {
        const std::uint16_t index = value.index();
        if (index >= thread_count) {
            return "invalid";
        }
        return entry_table_[index].name;
    }

    [[nodiscard]] constexpr std::uint16_t thread_group_offset(thread value) const noexcept {
        const std::uint16_t index = value.index();
        if (index >= thread_count) {
            return 0;
        }
        return entry_table_[index].group_offset;
    }

private:
    [[nodiscard]] static constexpr auto make_entry_table(Specs... specs) noexcept {
        std::array<detail::thread_layout_entry, thread_count> result{};
        std::uint16_t index = 0;
        (([&] {
             for (std::uint16_t i = 0; i < decltype(specs)::count; ++i) {
                 result[index++] =
                     detail::thread_layout_entry{decltype(specs)::kind, specs.group_name(), i};
             }
         }()),
         ...);
        return result;
    }

    std::array<detail::thread_layout_entry, thread_count> entry_table_{};
};

template <typename... Specs> using ThreadLayout = static_thread_layout<Specs...>;

template <typename TagT, std::uint16_t CountV, af::thread_kind KindV = af::thread_kind::cpu>
[[nodiscard]] constexpr auto thread_group(const char *name = "worker") noexcept {
    return thread_group_spec<TagT, CountV, KindV>{name};
}

template <typename... Specs> [[nodiscard]] constexpr auto thread_layout(Specs... specs) noexcept {
    return static_thread_layout<Specs...>{specs...};
}

} // namespace af
