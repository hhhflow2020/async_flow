#pragma once

#include <cstddef>
#include <cstdint>

#include "io_uring_sendmsg_zc_runtime.hpp"

#if defined(__linux__)
#include <sys/uio.h>

namespace io_uring_sendmsg_zc_example {

class SendmsgZcTask final : public SendmsgZcTaskBase {
public:
    explicit SendmsgZcTask(SendmsgZcTaskBase::FactoryToken token) : SendmsgZcTaskBase(token) {}

    bool do_it(int socket_fd, const char *first, std::size_t first_size, const char *second,
               std::size_t second_size, std::size_t *bytes_sent) {
        stream_.reset(SendmsgZcThreads::IO_0, socket_fd);
        first_ = first;
        first_size_ = first_size;
        second_ = second;
        second_size_ = second_size;
        bytes_sent_ = bytes_sent;
        return schedule(SendmsgZcThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        const std::size_t total_size = first_size_ + second_size_;
        if (sent_ >= total_size) {
            return done();
        }

        int iov_count = 0;
        if (sent_ < first_size_) {
            iov_[iov_count++] = iovec{const_cast<char *>(first_ + sent_), first_size_ - sent_};
            iov_[iov_count++] = iovec{const_cast<char *>(second_), second_size_};
        } else {
            const std::size_t second_offset = sent_ - first_size_;
            iov_[iov_count++] =
                iovec{const_cast<char *>(second_ + second_offset), second_size_ - second_offset};
        }

        const af::IoStatus status = stream_.sendv_zc_some(*this, iov_, iov_count, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > total_size - sent_) {
            return failed();
        }

        sent_ += status.bytes;
        *bytes_sent_ = sent_;
        return again();
    }

    af::TcpStream<SendmsgZcThread> stream_{};
    const char *first_{nullptr};
    const char *second_{nullptr};
    std::size_t first_size_{0};
    std::size_t second_size_{0};
    std::size_t sent_{0};
    iovec iov_[2]{};
    af::IoOpState send_{};
    std::size_t *bytes_sent_{nullptr};
};

} // namespace io_uring_sendmsg_zc_example

#else

namespace io_uring_sendmsg_zc_example {

class SendmsgZcTask final : public SendmsgZcTaskBase {
public:
    explicit SendmsgZcTask(SendmsgZcTaskBase::FactoryToken token) : SendmsgZcTaskBase(token) {}

    bool do_it(int socket_fd, const char *first, std::size_t first_size, const char *second,
               std::size_t second_size, std::size_t *bytes_sent) {
        static_cast<void>(socket_fd);
        static_cast<void>(first);
        static_cast<void>(first_size);
        static_cast<void>(second);
        static_cast<void>(second_size);
        static_cast<void>(bytes_sent);
        return false;
    }

private:
    af::TaskResult run() override {
        return failed();
    }
};

} // namespace io_uring_sendmsg_zc_example

#endif
