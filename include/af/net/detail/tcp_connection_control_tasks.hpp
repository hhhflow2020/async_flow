#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>

#include "af/buffer/buffer.hpp"
#include "af/net/detail/tcp_server_shard.hpp"
#include "af/net/detail/tcp_socket_ops.hpp"
#include "af/net/detail/tcp_state.hpp"
#include "af/net/tcp_types.hpp"

#include <sys/socket.h>

namespace af::net::detail {

template <typename Runtime> class TcpAdoptConnectionTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;

public:
    explicit TcpAdoptConnectionTask(typename Base::FactoryToken token) : Base(token) {}

    ~TcpAdoptConnectionTask() override {
        detail::close_fd(fd_);
    }

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, ListenerId listener_id,
               int fd, const sockaddr *peer, socklen_t peer_size) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        listener_id_ = listener_id;
        fd_ = fd;
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr || fd_ < 0 || peer == nullptr ||
            peer_size > sizeof(peer_)) {
            fd_ = -1;
            return false;
        }
        peer_size_ = peer_size;
        std::memcpy(&peer_, peer, peer_size_);
        if (!this->schedule(Runtime::thread_from_index(shard_index_))) {
            fd_ = -1;
            return false;
        }
        return true;
    }

private:
    af::TaskResult run() override {
        int fd = fd_;
        fd_ = -1;
        if (state_ != nullptr && shard_index_ < state_->shards.size() &&
            state_->shards[shard_index_] != nullptr && fd >= 0) {
            state_->shards[shard_index_]->adopt_connection(
                listener_id_, fd, reinterpret_cast<const sockaddr *>(&peer_), peer_size_);
            return this->done();
        }
        detail::close_fd(fd);
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    ListenerId listener_id_{};
    int fd_{-1};
    sockaddr_storage peer_{};
    socklen_t peer_size_{0};
};

template <typename Runtime> class TcpConnectionCommandTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;

public:
    explicit TcpConnectionCommandTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, std::uint32_t slot,
               std::uint32_t generation, af::Buffer buffer) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        slot_ = slot;
        generation_ = generation;
        kind_ = TcpConnectionCommandKind::Send;
        buffer_ = std::move(buffer);
        return schedule_on_owner();
    }

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, std::uint32_t slot,
               std::uint32_t generation, TcpConnectionCommandKind kind, bool flag) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        slot_ = slot;
        generation_ = generation;
        kind_ = kind;
        flag_ = flag;
        return schedule_on_owner();
    }

private:
    [[nodiscard]] bool schedule_on_owner() noexcept {
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(shard_index_));
    }

    af::TaskResult run() override {
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr) {
            return this->done();
        }
        auto *shard = state_->shards[shard_index_].get();
        switch (kind_) {
        case TcpConnectionCommandKind::Send:
            static_cast<void>(shard->send_to(slot_, generation_, std::move(buffer_)));
            break;
        case TcpConnectionCommandKind::Close:
            static_cast<void>(shard->close_connection(slot_, generation_));
            break;
        case TcpConnectionCommandKind::CloseAfterFlush:
            static_cast<void>(shard->close_connection_after_flush(slot_, generation_));
            break;
        case TcpConnectionCommandKind::ShutdownWrite:
            static_cast<void>(shard->shutdown_connection_write(slot_, generation_));
            break;
        case TcpConnectionCommandKind::PauseRead:
            static_cast<void>(shard->pause_connection_read(slot_, generation_));
            break;
        case TcpConnectionCommandKind::ResumeRead:
            static_cast<void>(shard->resume_connection_read(slot_, generation_));
            break;
        case TcpConnectionCommandKind::SetNoDelay:
            static_cast<void>(shard->set_connection_no_delay(slot_, generation_, flag_));
            break;
        case TcpConnectionCommandKind::SetKeepAlive:
            static_cast<void>(shard->set_connection_keepalive(slot_, generation_, flag_));
            break;
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::uint32_t slot_{0};
    std::uint32_t generation_{0};
    TcpConnectionCommandKind kind_{TcpConnectionCommandKind::Close};
    bool flag_{false};
    af::Buffer buffer_;
};

} // namespace af::net::detail
