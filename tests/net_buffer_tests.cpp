#include <array>
#include <cstddef>
#include <cstring>

#include "af/buffer/buffer.hpp"

#include "gtest/gtest.h"

TEST(NetBufferTests, BufferCopyKeepsPayload) {
    const std::array<std::byte, 4> input{std::byte{'a'}, std::byte{'b'}, std::byte{'c'},
                                         std::byte{'d'}};
    af::Buffer buffer = af::Buffer::copy(input.data(), input.size());

    ASSERT_EQ(buffer.size(), input.size());
    EXPECT_EQ(std::memcmp(buffer.data(), input.data(), input.size()), 0);
}

TEST(NetBufferTests, RemovePrefixAdvancesView) {
    const char *payload = "abcdef";
    af::Buffer buffer = af::Buffer::copy(payload, 6);

    buffer.remove_prefix(2);

    ASSERT_EQ(buffer.size(), 4U);
    EXPECT_EQ(buffer.view().string_view(), "cdef");
}

TEST(NetBufferTests, BufferChainTracksTotalBytes) {
    af::BufferChain chain;
    chain.push_back(af::Buffer::copy("ab", 2));
    chain.push_back(af::Buffer::copy("cde", 3));

    EXPECT_FALSE(chain.empty());
    EXPECT_EQ(chain.size(), 5U);
    EXPECT_EQ(chain.buffers().size(), 2U);
}
