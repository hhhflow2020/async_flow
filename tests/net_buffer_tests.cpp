#include <array>
#include <cstddef>
#include <cstring>

#include "af/buffer/buffer.hpp"

#include "folly/io/IOBuf.h"
#include "gtest/gtest.h"

TEST(NetBufferTests, BufferCopyKeepsPayload) {
    const std::array<std::byte, 4> input{std::byte{'a'}, std::byte{'b'}, std::byte{'c'},
                                         std::byte{'d'}};
    af::buffer buffer = af::buffer::copy(input.data(), input.size());

    ASSERT_EQ(buffer.size(), input.size());
    EXPECT_EQ(std::memcmp(buffer.data(), input.data(), input.size()), 0);
}

TEST(NetBufferTests, BufferExposesReadOnlyIobufStorage) {
    af::buffer buffer = af::buffer::copy("payload", 7);

    const folly::IOBuf *iobuf = buffer.iobuf();

    ASSERT_NE(iobuf, nullptr);
    EXPECT_EQ(iobuf->length(), 7U);
    EXPECT_EQ(std::memcmp(iobuf->data(), "payload", 7), 0);
}

TEST(NetBufferTests, BufferCopyDoesNotExposePooledBlockTailroom) {
    af::buffer one = af::buffer::copy("a", 1);
    af::buffer four = af::buffer::copy("data", 4);

    ASSERT_EQ(one.size(), 1U);
    ASSERT_EQ(four.size(), 4U);
    EXPECT_EQ(one.view().string_view(), "a");
    EXPECT_EQ(four.view().string_view(), "data");
    EXPECT_EQ(one.capacity(), one.size());
    EXPECT_EQ(four.capacity(), four.size());
    EXPECT_EQ(one.tailroom(), 0U);
    EXPECT_FALSE(one.try_append("b", 1));
}

TEST(NetBufferTests, BufferWithCapacityTracksHeadroomAndTailroom) {
    af::buffer buffer = af::buffer::with_capacity(16, 4);

    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(buffer.capacity(), 16U);
    EXPECT_EQ(buffer.headroom(), 4U);
    EXPECT_EQ(buffer.tailroom(), 12U);
}

TEST(NetBufferTests, BufferTryAppendUsesExistingTailroom) {
    af::buffer buffer = af::buffer::with_capacity(8, 2);

    ASSERT_TRUE(buffer.try_append("abc", 3));
    EXPECT_EQ(buffer.size(), 3U);
    EXPECT_EQ(buffer.headroom(), 2U);
    EXPECT_EQ(buffer.tailroom(), 3U);
    EXPECT_EQ(buffer.view().string_view(), "abc");
}

TEST(NetBufferTests, BufferTryAppendRejectsOverflowWithoutChangingSize) {
    af::buffer buffer = af::buffer::with_capacity(4);

    ASSERT_TRUE(buffer.try_append("abc", 3));
    EXPECT_FALSE(buffer.try_append("de", 2));

    EXPECT_EQ(buffer.size(), 3U);
    EXPECT_EQ(buffer.view().string_view(), "abc");
}

TEST(NetBufferTests, BufferAppendUninitializedReturnsWritableTail) {
    af::buffer buffer = af::buffer::with_capacity(8);

    std::byte *tail = buffer.try_append_uninitialized_data(4);
    ASSERT_NE(tail, nullptr);
    std::memcpy(tail, "data", 4);

    EXPECT_EQ(buffer.size(), 4U);
    EXPECT_EQ(buffer.view().string_view(), "data");
    EXPECT_EQ(buffer.tailroom(), 4U);
}

TEST(NetBufferTests, RemovePrefixAdvancesView) {
    const char *payload = "abcdef";
    af::buffer buffer = af::buffer::copy(payload, 6);

    buffer.remove_prefix(2);

    ASSERT_EQ(buffer.size(), 4U);
    EXPECT_EQ(buffer.view().string_view(), "cdef");
}

TEST(NetBufferTests, RemoveSuffixTrimsView) {
    af::buffer buffer = af::buffer::copy("abcdef", 6);

    buffer.remove_suffix(2);

    ASSERT_EQ(buffer.size(), 4U);
    EXPECT_EQ(buffer.view().string_view(), "abcd");
}

TEST(NetBufferTests, BufferSliceSharesStorageWithoutCopying) {
    af::buffer buffer = af::buffer::copy("abcdef", 6);
    af::buffer slice = buffer.slice(2, 3);

    ASSERT_EQ(slice.size(), 3U);
    EXPECT_EQ(slice.view().string_view(), "cde");

    buffer.remove_prefix(4);
    EXPECT_EQ(buffer.view().string_view(), "ef");
    EXPECT_EQ(slice.view().string_view(), "cde");
}

TEST(NetBufferTests, BufferCopyKeepsIndependentViewState) {
    af::buffer first = af::buffer::copy("abcdef", 6);
    af::buffer second = first;

    first.remove_prefix(2);
    first.remove_suffix(2);

    EXPECT_EQ(first.view().string_view(), "cd");
    EXPECT_EQ(second.view().string_view(), "abcdef");
}

TEST(NetBufferTests, BufferSliceUnsharesBeforeMutableWrite) {
    af::buffer original = af::buffer::copy("abcdef", 6);
    af::buffer slice = original.slice(1, 3);

    std::byte *data = slice.mutable_data();
    ASSERT_NE(data, nullptr);
    data[0] = std::byte{'X'};

    EXPECT_EQ(original.view().string_view(), "abcdef");
    EXPECT_EQ(slice.view().string_view(), "Xcd");
}

TEST(NetBufferTests, BufferChainTracksTotalBytes) {
    af::buffer_chain chain;
    chain.push_back(af::buffer::copy("ab", 2));
    chain.push_back(af::buffer::copy("cde", 3));

    EXPECT_FALSE(chain.empty());
    EXPECT_EQ(chain.size(), 5U);
    EXPECT_EQ(chain.buffers().size(), 2U);
}

TEST(NetBufferTests, BufferChainUsesInlineStorageForSmallChains) {
    af::buffer_chain chain;

    EXPECT_GE(chain.buffers().capacity(), af::detail::buffer_chain_inline_capacity);

    for (std::size_t i = 0; i < af::detail::buffer_chain_inline_capacity; ++i) {
        chain.push_back(af::buffer::copy("x", 1));
    }

    EXPECT_EQ(chain.buffers().size(), af::detail::buffer_chain_inline_capacity);
    EXPECT_GE(chain.buffers().capacity(), af::detail::buffer_chain_inline_capacity);
}

TEST(NetBufferTests, BufferChainRemovePrefixConsumesAcrossBuffers) {
    af::buffer_chain chain;
    chain.push_back(af::buffer::copy("ab", 2));
    chain.push_back(af::buffer::copy("cde", 3));
    chain.push_back(af::buffer::copy("fg", 2));

    chain.remove_prefix(4);

    ASSERT_EQ(chain.size(), 3U);
    ASSERT_EQ(chain.buffers().size(), 2U);
    EXPECT_EQ(chain.buffers()[0].view().string_view(), "e");
    EXPECT_EQ(chain.buffers()[1].view().string_view(), "fg");
}

TEST(NetBufferTests, BufferChainPopFrontKeepsOnlyActiveBuffersVisible) {
    af::buffer_chain chain;
    chain.push_back(af::buffer::copy("ab", 2));
    chain.push_back(af::buffer::copy("cd", 2));
    chain.push_back(af::buffer::copy("ef", 2));

    chain.pop_front();

    ASSERT_EQ(chain.size(), 4U);
    ASSERT_EQ(chain.buffers().size(), 2U);
    EXPECT_EQ(chain.buffers()[0].view().string_view(), "cd");
    EXPECT_EQ(chain.buffers()[1].view().string_view(), "ef");
}

TEST(NetBufferTests, BufferChainRecomputesSizeAfterMutableBufferAccess) {
    af::buffer_chain chain;
    chain.push_back(af::buffer::copy("ab", 2));

    chain.buffers().push_back(af::buffer::copy("cde", 3));

    EXPECT_EQ(chain.size(), 5U);
}

TEST(NetBufferTests, BufferChainFillsScatterGatherViews) {
    af::buffer_chain chain;
    chain.push_back(af::buffer::copy("ab", 2));
    chain.push_back(af::buffer::copy("cde", 3));
    chain.push_back(af::buffer::copy("fg", 2));

    std::array<af::buffer_view, 2> views{};
    const std::size_t count = chain.fill_views(views);

    ASSERT_EQ(count, 2U);
    EXPECT_EQ(views[0].string_view(), "ab");
    EXPECT_EQ(views[1].string_view(), "cde");
    EXPECT_EQ(chain.size(), 7U);
}

TEST(NetBufferTests, BufferChainFillsIobufsForScatterGather) {
    af::buffer_chain chain;
    chain.push_back(af::buffer::copy("ab", 2));
    chain.push_back(af::buffer::copy("cde", 3));
    chain.push_back(af::buffer::copy("fg", 2));

    std::array<const folly::IOBuf *, 2> iobufs{};
    const std::size_t count = chain.fill_iobufs(iobufs);

    ASSERT_EQ(count, 2U);
    ASSERT_NE(iobufs[0], nullptr);
    ASSERT_NE(iobufs[1], nullptr);
    EXPECT_EQ(iobufs[0]->length(), 2U);
    EXPECT_EQ(iobufs[1]->length(), 3U);
    EXPECT_EQ(std::memcmp(iobufs[0]->data(), "ab", 2), 0);
    EXPECT_EQ(std::memcmp(iobufs[1]->data(), "cde", 3), 0);
    EXPECT_EQ(chain.size(), 7U);
}

TEST(NetBufferTests, BufferChainFillViewsStartsAtActivePrefix) {
    af::buffer_chain chain;
    chain.push_back(af::buffer::copy("ab", 2));
    chain.push_back(af::buffer::copy("cd", 2));
    chain.push_back(af::buffer::copy("ef", 2));

    chain.pop_front();

    std::array<af::buffer_view, 4> views{};
    const std::size_t count = chain.fill_views(views);

    ASSERT_EQ(count, 2U);
    EXPECT_EQ(views[0].string_view(), "cd");
    EXPECT_EQ(views[1].string_view(), "ef");
    EXPECT_EQ(chain.size(), 4U);
}
