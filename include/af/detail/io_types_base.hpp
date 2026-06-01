#pragma once

#if defined(_WIN32)
using IoOffset = std::int64_t;
#else
using IoOffset = off_t;
#endif

#if !defined(_WIN32)
struct IoFixedBuffer {
    void *data{nullptr};
    std::size_t size{0};
    std::uint16_t index{0};
};
#endif

template <typename ThreadT> class IoFixedFile;

struct IoRecvmsgMultishotView {
    std::uint16_t buffer_id{0};
    std::uint32_t name_offset{0};
    std::uint32_t name_size{0};
    std::uint32_t control_offset{0};
    std::uint32_t control_size{0};
    std::uint32_t payload_offset{0};
    std::uint32_t payload_size{0};
    std::uint32_t flags{0};
};
