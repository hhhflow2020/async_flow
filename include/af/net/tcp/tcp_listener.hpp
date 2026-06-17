#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "af/net/detail/socket_address.hpp"
#include "af/net/detail/tcp_socket_ops.hpp"
#include "af/net/endpoint.hpp"
#include "af/net/tcp/tcp_types.hpp"
#include "af/runtime.hpp"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace af::net {

class tcp_listener;

using tcp_listener_accept_callback = void (*)(void *owner, tcp_listener &listener, int fd,
                                              const sockaddr *peer, socklen_t peer_size) noexcept;
using tcp_listener_error_callback = void (*)(void *owner, tcp_listener &listener,
                                             int error) noexcept;

struct tcp_listener_callbacks {
    void *owner{nullptr};
    tcp_listener_accept_callback on_accept{nullptr};
    tcp_listener_error_callback on_error{nullptr};
};

class tcp_listener {
public:
    explicit tcp_listener(af::runtime &owner) noexcept : owner_(&owner) {}

    tcp_listener(const tcp_listener &) = delete;
    tcp_listener &operator=(const tcp_listener &) = delete;

    ~tcp_listener() {
        if (started_) {
            AF_ASSERT(on_owner_thread() &&
                      "tcp_listener must be stopped on the owner reactor thread before "
                      "destruction");
            if (on_owner_thread()) {
                static_cast<void>(stop());
                return;
            }
        }
        close_without_unregister();
    }

    [[nodiscard]] bool start(af::thread_ref thread, tcp_listener_config config,
                             tcp_listener_callbacks callbacks) noexcept {
        if (started_) {
            return false;
        }
        callbacks_ = callbacks;
        if (!prepare_owner_thread(thread)) {
            return false;
        }
        if (!normalize_config(config)) {
            return false;
        }

        af::detail::socket_address address;
        int error = 0;
        if (!af::detail::socket_address_from_endpoint(config.endpoint, address, error)) {
            notify_error(error);
            return false;
        }

        const bool unix_domain = address.family == AF_UNIX;
        if (unix_domain && config.options.unlink_existing_unix_path) {
            unlink_unix_path(config.endpoint);
        }

        int fd = ::socket(address.family, SOCK_STREAM, unix_domain ? 0 : IPPROTO_TCP);
        if (fd < 0) {
            notify_error(errno);
            return false;
        }
        if (!configure_socket(fd, address.family, config.options)) {
            const int saved_errno = errno == 0 ? EIO : errno;
            ::close(fd);
            notify_error(saved_errno);
            return false;
        }
        if (::bind(fd, reinterpret_cast<const sockaddr *>(&address.storage), address.size) != 0) {
            const int saved_errno = errno == 0 ? EADDRNOTAVAIL : errno;
            ::close(fd);
            notify_error(saved_errno);
            return false;
        }
        if (::listen(fd, config.options.backlog) != 0) {
            const int saved_errno = errno == 0 ? EIO : errno;
            ::close(fd);
            if (unix_domain && config.options.unlink_unix_path_on_close) {
                unlink_unix_path(config.endpoint);
            }
            notify_error(saved_errno);
            return false;
        }

        af::detail::socket_address local_address;
        socklen_t local_size = sizeof(local_address.storage);
        if (::getsockname(fd, reinterpret_cast<sockaddr *>(&local_address.storage), &local_size) ==
            0) {
            local_address.size = local_size;
            local_endpoint_ = af::detail::endpoint_from_socket_address(
                reinterpret_cast<const sockaddr *>(&local_address.storage), local_address.size);
        } else {
            local_endpoint_ = config.endpoint;
        }

        config_ = std::move(config);
        fd_ = fd;
        source_.fd = fd_;
        source_.interests = af::reactor_readable;
        source_.owner = this;
        source_.on_event = &tcp_listener::on_reactor_event;
        if (!owner_->register_reactor_source(owner_thread_, &source_)) {
            close_without_unregister();
            notify_error(EIO);
            return false;
        }
        started_ = true;
        return true;
    }

    [[nodiscard]] bool stop() noexcept {
        if (!started_) {
            return false;
        }
        AF_ASSERT(on_owner_thread() && "tcp_listener::stop must run on the owner reactor thread");
        if (!on_owner_thread()) {
            return false;
        }
        const bool unregistered = owner_->unregister_reactor_source(owner_thread_, &source_);
        close_without_unregister();
        return unregistered;
    }

    [[nodiscard]] bool started() const noexcept {
        return started_;
    }

    [[nodiscard]] af::thread_ref owner_thread() const noexcept {
        return owner_thread_;
    }

    [[nodiscard]] int native_fd() const noexcept {
        return fd_;
    }

    [[nodiscard]] const tcp_endpoint &local_endpoint() const noexcept {
        return local_endpoint_;
    }

private:
    [[nodiscard]] bool on_owner_thread() const noexcept {
        return owner_ != nullptr && af::runtime::current() == owner_ &&
               af::runtime::current_thread_index() == owner_thread_.index;
    }

    [[nodiscard]] bool prepare_owner_thread(af::thread_ref thread) noexcept {
        if (owner_ == nullptr) {
            return false;
        }
        if (!thread) {
            thread = owner_->io_threads().front();
        }
        if (!thread || !owner_->valid_thread(thread) ||
            owner_->thread_kind_of(thread) != af::thread_kind::io) {
            return false;
        }
        AF_ASSERT(af::runtime::current() == owner_ &&
                  af::runtime::current_thread_index() == thread.index &&
                  "tcp_listener::start must run on the owner reactor thread");
        if (af::runtime::current() != owner_ ||
            af::runtime::current_thread_index() != thread.index) {
            return false;
        }
        owner_thread_ = thread;
        return true;
    }

    [[nodiscard]] bool normalize_config(tcp_listener_config &config) noexcept {
        if (config.options.backlog <= 0) {
            config.options.backlog = 1;
        }
        if (config.options.accept_budget == 0U) {
            config.options.accept_budget = 1U;
        }
        return true;
    }

    [[nodiscard]] bool configure_socket(int fd, int family,
                                        const tcp_listener_options &options) noexcept {
        if (!af::net::detail::set_nonblocking(fd) || !af::net::detail::set_cloexec(fd)) {
            return false;
        }
        af::net::detail::set_no_sigpipe(fd);

        if (family == AF_UNIX) {
            return true;
        }

        int one = 1;
        if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
            return false;
        }
#if defined(SO_REUSEPORT)
        if (options.reuse_port &&
            ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) != 0) {
            return false;
        }
#else
        static_cast<void>(options);
#endif
        if (family == AF_INET6) {
            const int value = options.ipv6_only ? 1 : 0;
            if (::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &value, sizeof(value)) != 0) {
                return false;
            }
        }
        return true;
    }

    static void on_reactor_event(void *owner, af::fd_event_source &source,
                                 std::uint32_t events) noexcept {
        static_cast<void>(source);
        auto *listener = static_cast<tcp_listener *>(owner);
        if (listener == nullptr) {
            return;
        }
        if ((events & (af::reactor_error | af::reactor_hangup)) != 0U) {
            listener->notify_error(ECONNABORTED);
        }
        if ((events & af::reactor_readable) != 0U) {
            listener->accept_ready();
        }
    }

    void accept_ready() noexcept {
        for (std::size_t accepted = 0; accepted < config_.options.accept_budget; ++accepted) {
            sockaddr_storage peer{};
            socklen_t peer_size = sizeof(peer);
            const int fd = af::net::detail::accept_nonblocking(
                fd_, reinterpret_cast<sockaddr *>(&peer), &peer_size);
            if (fd >= 0) {
                af::net::detail::set_no_sigpipe(fd);
                if (callbacks_.on_accept != nullptr) {
                    callbacks_.on_accept(callbacks_.owner, *this, fd,
                                         reinterpret_cast<const sockaddr *>(&peer), peer_size);
                } else {
                    ::close(fd);
                }
                continue;
            }

            const int saved_errno = errno == 0 ? EIO : errno;
            if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK) {
                return;
            }
            if (saved_errno == EINTR) {
                --accepted;
                continue;
            }
            notify_error(saved_errno);
            return;
        }
    }

    void notify_error(int error) noexcept {
        if (callbacks_.on_error != nullptr) {
            callbacks_.on_error(callbacks_.owner, *this, error == 0 ? EIO : error);
        }
    }

    void close_without_unregister() noexcept {
        const bool unlink_path = fd_ >= 0 && should_unlink_bound_unix_path();
        af::net::detail::close_fd(fd_);
        if (unlink_path) {
            unlink_unix_path(config_.endpoint);
        }
        source_ = af::fd_event_source{};
        started_ = false;
    }

    [[nodiscard]] bool should_unlink_bound_unix_path() const noexcept {
        return config_.endpoint.family == address_family::unix_domain &&
               config_.options.unlink_unix_path_on_close;
    }

    static void unlink_unix_path(const tcp_endpoint &endpoint) noexcept {
        if (endpoint.family == address_family::unix_domain && !endpoint.address.empty()) {
            static_cast<void>(::unlink(endpoint.address.c_str()));
        }
    }

    af::runtime *owner_{nullptr};
    af::thread_ref owner_thread_{};
    tcp_listener_config config_;
    tcp_endpoint local_endpoint_{};
    tcp_listener_callbacks callbacks_{};
    af::fd_event_source source_{};
    int fd_{-1};
    bool started_{false};
};

} // namespace af::net
