#pragma once

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "af/detail/log/log_backend.hpp"

#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>

namespace af {

struct file_log_backend_options {
    std::filesystem::path path;
    bool append{true};
    bool close_on_exec{true};
    bool fsync_on_flush{true};
    std::size_t write_batch_iov{64};
};

class file_log_backend final : public log_backend {
public:
    explicit file_log_backend(file_log_backend_options config)
        : path_(config.path.string()), append_(config.append), close_on_exec_(config.close_on_exec),
          fsync_on_flush_(config.fsync_on_flush),
          iovecs_(normalize_write_batch_iov(config.write_batch_iov)) {}

    file_log_backend(const file_log_backend &) = delete;
    file_log_backend &operator=(const file_log_backend &) = delete;

    ~file_log_backend() override {
        close_file();
    }

    void write_batch(af::span<detail::log_record *const> records) noexcept override {
        if (records.empty() || !open_if_needed()) {
            return;
        }

        std::size_t index = 0;
        while (index < records.size()) {
            std::size_t count = 0;
            while (index < records.size() && count < iovecs_.size()) {
                std::string_view message = records[index]->message();
                iovecs_[count].iov_base = const_cast<char *>(message.data());
                iovecs_[count].iov_len = message.size();
                ++count;
                ++index;
            }
            writev_all(iovecs_.data(), count);
        }
    }

    void flush() noexcept override {
        if (fsync_on_flush_ && fd_ >= 0) {
            static_cast<void>(::fsync(fd_));
        }
    }

private:
    static constexpr std::size_t max_iov_count = 1024;

    [[nodiscard]] static std::size_t normalize_write_batch_iov(std::size_t requested) noexcept {
        if (requested == 0U) {
            return 1U;
        }
        return std::min(requested, max_iov_count);
    }

    [[nodiscard]] bool open_if_needed() noexcept {
        if (fd_ >= 0) {
            return true;
        }

        int flags = O_CREAT | O_WRONLY;
        flags |= append_ ? O_APPEND : O_TRUNC;
#if defined(O_CLOEXEC)
        if (close_on_exec_) {
            flags |= O_CLOEXEC;
        }
#endif
        fd_ = ::open(path_.c_str(), flags, 0644);
        return fd_ >= 0;
    }

    void writev_all(iovec *iovecs, std::size_t count) noexcept {
        while (count != 0U) {
            const auto written = ::writev(fd_, iovecs, static_cast<int>(count));
            if (written < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return;
            }
            if (written == 0) {
                return;
            }

            auto remaining = static_cast<std::size_t>(written);
            while (count != 0U && remaining >= iovecs[0].iov_len) {
                remaining -= iovecs[0].iov_len;
                ++iovecs;
                --count;
            }
            if (count != 0U && remaining != 0U) {
                auto *base = static_cast<char *>(iovecs[0].iov_base);
                iovecs[0].iov_base = base + remaining;
                iovecs[0].iov_len -= remaining;
            }
        }
    }

    void close_file() noexcept {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
            fd_ = -1;
        }
    }

    std::string path_;
    bool append_{true};
    bool close_on_exec_{true};
    bool fsync_on_flush_{true};
    int fd_{-1};
    std::vector<iovec> iovecs_;
};

[[nodiscard]] inline std::unique_ptr<log_backend>
make_file_log_backend(file_log_backend_options config) {
    return std::make_unique<file_log_backend>(std::move(config));
}

} // namespace af
