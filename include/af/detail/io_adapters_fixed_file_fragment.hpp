#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_fixed_file_fragment.hpp is an io_adapters implementation fragment"
#endif

template <typename ThreadT>
class IoFixedFile {
public:
    constexpr IoFixedFile() noexcept = default;
    constexpr IoFixedFile(ThreadT thread, int index) noexcept : thread_(thread), index_(index) {}

    [[nodiscard]] constexpr ThreadT thread() const noexcept {
        return thread_;
    }

    [[nodiscard]] constexpr int index() const noexcept {
        return index_;
    }

    [[nodiscard]] constexpr bool valid() const noexcept {
        return index_ >= 0;
    }

    constexpr void reset(ThreadT thread, int index) noexcept {
        thread_ = thread;
        index_ = index;
    }

#include "af/detail/io_adapters_fixed_file_read_fragment.hpp"
#include "af/detail/io_adapters_fixed_file_recv_fragment.hpp"
#include "af/detail/io_adapters_fixed_file_write_fragment.hpp"
#include "af/detail/io_adapters_fixed_file_send_fragment.hpp"
#include "af/detail/io_adapters_fixed_file_sync_fragment.hpp"

private:
    ThreadT thread_{};
    int index_{-1};
};
