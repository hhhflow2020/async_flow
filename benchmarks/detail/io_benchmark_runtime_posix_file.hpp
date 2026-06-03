#pragma once

struct FakeRuntimePosixFileOps {
    static bool io_submit_read_fixed_at(BenchIoThread, int, void *, std::size_t, std::uint64_t,
                                        std::uint16_t, void *, af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_write_fixed_at(BenchIoThread, int, const void *, std::size_t,
                                         std::uint64_t, std::uint16_t, void *,
                                         af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_read_fixed_file_at(BenchIoThread, int, void *, std::size_t, std::uint64_t,
                                             void *, af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_write_fixed_file_at(BenchIoThread, int, const void *, std::size_t,
                                              std::uint64_t, void *, af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_readv_fixed_file_at(BenchIoThread, int, const iovec *, int, std::uint64_t,
                                              void *, af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_writev_fixed_file_at(BenchIoThread, int, const iovec *, int,
                                               std::uint64_t, void *, af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_read_fixed_file_at(BenchIoThread, int, void *, std::size_t, std::uint64_t,
                                             std::uint16_t, void *, af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_write_fixed_file_at(BenchIoThread, int, const void *, std::size_t,
                                              std::uint64_t, std::uint16_t, void *,
                                              af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_fsync_fixed_file(BenchIoThread, int, std::uint32_t, void *,
                                           af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_readv_at(BenchIoThread, int, const iovec *, int, std::uint64_t, void *,
                                   af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_writev_at(BenchIoThread, int, const iovec *, int, std::uint64_t, void *,
                                    af::IoResult *) noexcept {
        return false;
    }
};
