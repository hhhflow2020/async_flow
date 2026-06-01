#pragma once

struct FakeRuntimeFilesystemOps {
    static bool io_submit_openat(BenchIoThread, int, const char *, int, std::uint32_t, void *,
                                 af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_openat_direct(BenchIoThread, int, const char *, int, std::uint32_t, int,
                                        void *, af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_openat2(BenchIoThread, int, const char *, const struct open_how *, void *,
                                  af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_close(BenchIoThread, int, void *, af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_shutdown(BenchIoThread, int, int, void *, af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_statx(BenchIoThread, int, const char *, int, std::uint32_t,
                                struct statx *, void *, af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_fallocate(BenchIoThread, int, int, std::uint64_t, std::uint64_t, void *,
                                    af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_renameat(BenchIoThread, int, const char *, int, const char *,
                                   std::uint32_t, void *, af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_unlinkat(BenchIoThread, int, const char *, int, void *,
                                   af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_mkdirat(BenchIoThread, int, const char *, std::uint32_t, void *,
                                  af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_symlinkat(BenchIoThread, const char *, int, const char *, void *,
                                    af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_linkat(BenchIoThread, int, const char *, int, const char *, std::uint32_t,
                                 void *, af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_ftruncate(BenchIoThread, int, std::uint64_t, void *,
                                    af::IoResult *) noexcept {
        return false;
    }

    static bool io_submit_splice(BenchIoThread, int, std::int64_t, int, std::int64_t, std::size_t,
                                 unsigned int, void *, af::IoResult *) noexcept {
        return false;
    }
};
