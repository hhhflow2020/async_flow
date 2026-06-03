#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_net_channel.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

template <typename RuntimeT, typename TraitsT>
bool Executor<RuntimeT, TraitsT>::register_net_channel(detail::NetIoChannel *channel,
                                                       std::uint32_t events) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "net channel registration must run on the owner IO thread");
    if (RuntimeT::current_thread_index_ != index_ || channel == nullptr || channel->fd < 0 ||
        channel->on_event == nullptr) {
        return false;
    }

#if AF_DETAIL_HAS_NATIVE_IO_WAIT
    if (!native_io_backend_available() || net_channels_.find(channel->fd) != net_channels_.end() ||
        io_waits_.find(channel->fd) != io_waits_.end()) {
        return false;
    }
    channel->active = false;
    channel->interests = 0;
    channel->backend_token = nullptr;
    auto [it, inserted] = net_channels_.try_emplace(channel->fd, channel);
    if (!inserted) {
        return false;
    }
    if (!update_net_channel_interest(channel, events)) {
        net_channels_.erase(it);
        channel->active = false;
        channel->interests = 0;
        return false;
    }
    return true;
#else
    static_cast<void>(events);
    return false;
#endif
}

template <typename RuntimeT, typename TraitsT>
bool Executor<RuntimeT, TraitsT>::update_net_channel(detail::NetIoChannel *channel,
                                                     std::uint32_t events) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "net channel updates must run on the owner IO thread");
    if (RuntimeT::current_thread_index_ != index_ || channel == nullptr || channel->fd < 0) {
        return false;
    }

#if AF_DETAIL_HAS_NATIVE_IO_WAIT
    auto it = net_channels_.find(channel->fd);
    if (it == net_channels_.end() || it->second != channel) {
        return false;
    }
    return update_net_channel_interest(channel, events);
#else
    static_cast<void>(events);
    return false;
#endif
}

template <typename RuntimeT, typename TraitsT>
bool Executor<RuntimeT, TraitsT>::unregister_net_channel(detail::NetIoChannel *channel) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "net channel unregister must run on the owner IO thread");
    if (RuntimeT::current_thread_index_ != index_ || channel == nullptr || channel->fd < 0) {
        return false;
    }

#if AF_DETAIL_HAS_NATIVE_IO_WAIT
    auto it = net_channels_.find(channel->fd);
    if (it == net_channels_.end() || it->second != channel) {
        return false;
    }
    static_cast<void>(update_net_channel_interest(channel, 0));
    net_channels_.erase(it);
    channel->active = false;
    channel->interests = 0;
    return true;
#else
    return false;
#endif
}

} // namespace af::detail
