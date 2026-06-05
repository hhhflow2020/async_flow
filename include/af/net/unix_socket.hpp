#pragma once

#include <cerrno>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "af/net/tcp_client.hpp"
#include "af/net/tcp_client_runtime.hpp"
#include "af/net/tcp_connection_runtime.hpp"
#include "af/net/tcp_server_control.hpp"
#include "af/net/udp_socket.hpp"
#include "af/net/udp_socket_runtime.hpp"

namespace af::net {

template <typename Runtime> using UnixConnectionRef = TcpConnectionRef<Runtime>;
template <typename Runtime> using UnixConnectionHandle = TcpConnectionHandle<Runtime>;
template <typename Runtime> using UnixDatagramSocketRef = UdpSocketRef<Runtime>;
template <typename Runtime> using UnixDatagramSocketHandle = UdpSocketHandle<Runtime>;

using unix_connection_ref = tcp_connection_ref;
using unix_connection_handle = tcp_connection_handle;
using unix_datagram_socket_ref = udp_socket_ref;
using unix_datagram_socket_handle = udp_socket_handle;
using unix_datagram_peer = udp_peer;

struct UnixStreamRuntimeConfig {};

struct UnixDatagramRuntimeConfig {};

namespace detail {

[[nodiscard]] inline TcpServerConfig to_tcp_server_config(UnixStreamRuntimeConfig config) noexcept {
    static_cast<void>(config);
    return TcpServerConfig{};
}

[[nodiscard]] inline TcpClientRuntimeConfig
to_tcp_client_config(UnixStreamRuntimeConfig config) noexcept {
    static_cast<void>(config);
    return TcpClientRuntimeConfig{};
}

[[nodiscard]] inline UdpSocketRuntimeConfig
to_udp_socket_config(UnixDatagramRuntimeConfig config) noexcept {
    static_cast<void>(config);
    return UdpSocketRuntimeConfig{};
}

} // namespace detail

template <typename Runtime> class UnixStreamServer {
public:
    using Thread = typename Runtime::Thread;

    struct ListenerConfig {
        std::string name;
        UnixEndpoint endpoint;
        std::vector<Thread> threads;
        TcpListenerOptions options;
    };

    UnixStreamServer() = default;

    explicit UnixStreamServer(UnixStreamRuntimeConfig config)
        : server_(detail::to_tcp_server_config(config)) {}

    explicit UnixStreamServer(std::vector<Thread> threads) : server_(std::move(threads)) {}

    UnixStreamServer &bind_threads(std::vector<Thread> threads) {
        server_.bind_threads(std::move(threads));
        return *this;
    }

    template <typename Group> UnixStreamServer &bind_threads(Group group) {
        server_.bind_threads(group);
        return *this;
    }

    template <typename Handler>
    [[nodiscard]] ListenerResult add_listener(ListenerConfig config, Handler handler = Handler{}) {
        config.options.reuse_port = false;
        return server_.template add_listener<Handler>(
            {
                .name = std::move(config.name),
                .endpoint = std::move(config.endpoint),
                .threads = std::move(config.threads),
                .options = config.options,
                .accept_strategy = AcceptStrategy::SingleAcceptor,
            },
            std::move(handler));
    }

    template <typename Handler>
    [[nodiscard]] ListenerResult listen(ListenerConfig config, Handler handler = Handler{}) {
        return add_listener(std::move(config), std::move(handler));
    }

    [[nodiscard]] bool
    remove_listener(TcpListenerHandle listener,
                    RemoveListenerPolicy policy = RemoveListenerPolicy::StopAcceptOnly) {
        return server_.remove_listener(listener, policy);
    }

    [[nodiscard]] bool start() {
        return server_.start();
    }

    [[nodiscard]] bool stop() {
        return server_.stop();
    }

    [[nodiscard]] TcpServer<Runtime> &tcp_server() noexcept {
        return server_;
    }

    [[nodiscard]] const TcpServer<Runtime> &tcp_server() const noexcept {
        return server_;
    }

private:
    TcpServer<Runtime> server_;
};

template <typename Runtime> class UnixStreamClient {
public:
    using Thread = typename Runtime::Thread;

    struct ConnectConfig {
        std::string name;
        UnixEndpoint endpoint;
        std::vector<Thread> threads;
        TcpClientOptions options;
        std::chrono::nanoseconds connect_timeout{std::chrono::seconds(30)};
    };

    UnixStreamClient() = default;

    explicit UnixStreamClient(UnixStreamRuntimeConfig config)
        : client_(detail::to_tcp_client_config(config)) {}

    explicit UnixStreamClient(std::vector<Thread> threads) : client_(std::move(threads)) {}

    UnixStreamClient &bind_threads(std::vector<Thread> threads) {
        client_.bind_threads(std::move(threads));
        return *this;
    }

    template <typename Group> UnixStreamClient &bind_threads(Group group) {
        client_.bind_threads(group);
        return *this;
    }

    template <typename Handler>
    [[nodiscard]] bool connect(ConnectConfig config, Handler handler = Handler{}) {
        return client_.template connect<Handler>(
            {
                .name = std::move(config.name),
                .remote_endpoint = std::move(config.endpoint),
                .threads = std::move(config.threads),
                .options = config.options,
                .connect_timeout = config.connect_timeout,
            },
            std::move(handler));
    }

    [[nodiscard]] bool stop() {
        return client_.stop();
    }

    [[nodiscard]] TcpClient<Runtime> &tcp_client() noexcept {
        return client_;
    }

    [[nodiscard]] const TcpClient<Runtime> &tcp_client() const noexcept {
        return client_;
    }

private:
    TcpClient<Runtime> client_;
};

template <typename Runtime> class UnixDatagramSocket {
public:
    using Thread = typename Runtime::Thread;

    struct BindConfig {
        std::string name;
        UnixEndpoint local_endpoint;
        std::vector<Thread> threads;
        UdpSocketOptions options;
    };

    struct ConnectConfig {
        std::string name;
        UnixEndpoint local_endpoint;
        UnixEndpoint remote_endpoint;
        std::vector<Thread> threads;
        UdpSocketOptions options;
    };

    UnixDatagramSocket() = default;

    explicit UnixDatagramSocket(UnixDatagramRuntimeConfig config)
        : socket_(detail::to_udp_socket_config(config)) {}

    explicit UnixDatagramSocket(std::vector<Thread> threads) : socket_(std::move(threads)) {}

    UnixDatagramSocket &bind_threads(std::vector<Thread> threads) {
        socket_.bind_threads(std::move(threads));
        return *this;
    }

    template <typename Group> UnixDatagramSocket &bind_threads(Group group) {
        socket_.bind_threads(group);
        return *this;
    }

    template <typename Handler>
    [[nodiscard]] bool bind(BindConfig config, Handler handler = Handler{}) {
        return start_impl<Handler>(std::move(config.name), std::move(config.local_endpoint),
                                   UnixEndpoint{}, false, std::move(config.threads), config.options,
                                   std::move(handler));
    }

    template <typename Handler>
    [[nodiscard]] bool listen(BindConfig config, Handler handler = Handler{}) {
        return bind(std::move(config), std::move(handler));
    }

    template <typename Handler>
    [[nodiscard]] bool connect(ConnectConfig config, Handler handler = Handler{}) {
        return start_impl<Handler>(std::move(config.name), std::move(config.local_endpoint),
                                   std::move(config.remote_endpoint), true,
                                   std::move(config.threads), config.options, std::move(handler));
    }

    [[nodiscard]] bool stop() {
        return socket_.stop();
    }

    [[nodiscard]] UnixDatagramSocketHandle<Runtime> handle() const {
        return socket_.handle();
    }

    [[nodiscard]] UdpSocket<Runtime> &udp_socket() noexcept {
        return socket_;
    }

    [[nodiscard]] const UdpSocket<Runtime> &udp_socket() const noexcept {
        return socket_;
    }

private:
    template <typename Handler>
    [[nodiscard]] bool start_impl(std::string name, UnixEndpoint local_endpoint,
                                  UnixEndpoint remote_endpoint, bool connect_remote,
                                  std::vector<Thread> threads, UdpSocketOptions options,
                                  Handler handler) {
        options.reuse_port = false;
        return socket_.template start<Handler>(
            {
                .name = std::move(name),
                .local_endpoint = std::move(local_endpoint),
                .remote_endpoint = std::move(remote_endpoint),
                .threads = std::move(threads),
                .options = options,
                .connect_remote = connect_remote,
            },
            std::move(handler));
    }

    UdpSocket<Runtime> socket_;
};

struct unix_stream_server_config {
    tcp_connection_config connection;
    std::chrono::milliseconds connection_close_timeout{std::chrono::seconds(5)};
};

struct unix_stream_listener_config {
    std::string name;
    unix_endpoint endpoint;
    std::vector<af::thread_ref> threads;
    tcp_listener_options options;
};

struct unix_stream_connect_config {
    std::string name;
    unix_endpoint endpoint;
    unix_endpoint local_endpoint = unix_endpoint::unix_path("");
    bool bind_local{false};
    af::thread_ref owner_thread{};
    tcp_connection_config connection;
    std::chrono::milliseconds connect_timeout{std::chrono::seconds(30)};
};

struct unix_datagram_bind_config {
    std::string name;
    unix_endpoint local_endpoint;
    std::vector<af::thread_ref> threads;
    udp_socket_options options;
};

struct unix_datagram_connect_config {
    std::string name;
    unix_endpoint local_endpoint;
    unix_endpoint remote_endpoint;
    std::vector<af::thread_ref> threads;
    udp_socket_options options;
};

using unix_stream_callbacks = tcp_connection_callbacks;
using unix_stream_client_callbacks = tcp_client_callbacks;
using unix_datagram_callbacks = udp_socket_callbacks;

class unix_stream_server {
public:
    explicit unix_stream_server(af::runtime &owner, unix_stream_server_config config = {})
        : server_(owner, to_tcp_server_config(config)) {}

    unix_stream_server(const unix_stream_server &) = delete;
    unix_stream_server &operator=(const unix_stream_server &) = delete;

    [[nodiscard]] listener_result add_listener(unix_stream_listener_config config,
                                               unix_stream_callbacks callbacks = {}) noexcept {
        if (config.endpoint.family != address_family::unix_domain) {
            return listener_result::failure(EAFNOSUPPORT);
        }
        tcp_listener_config listener_config;
        listener_config.name = std::move(config.name);
        listener_config.endpoint = std::move(config.endpoint);
        listener_config.threads = std::move(config.threads);
        listener_config.options = config.options;
        listener_config.options.reuse_port = false;
        listener_config.accept_strategy = accept_strategy::single_acceptor;
        return server_.add_listener_connection(std::move(listener_config), callbacks);
    }

    [[nodiscard]] bool remove_listener(
        tcp_listener_handle listener,
        remove_listener_policy policy = remove_listener_policy::stop_accept_only) noexcept {
        return server_.remove_listener(listener, policy);
    }

    [[nodiscard]] bool start() noexcept {
        return server_.start();
    }

    [[nodiscard]] bool stop() noexcept {
        return server_.stop();
    }

    [[nodiscard]] bool running() const noexcept {
        return server_.running();
    }

    [[nodiscard]] std::size_t listener_count() const noexcept {
        return server_.listener_count();
    }

    [[nodiscard]] std::size_t connection_count() const noexcept {
        return server_.connection_count();
    }

    [[nodiscard]] const unix_endpoint *local_endpoint(tcp_listener_handle listener) const noexcept {
        return server_.local_endpoint(listener);
    }

    [[nodiscard]] tcp_server &stream_server() noexcept {
        return server_;
    }

    [[nodiscard]] const tcp_server &stream_server() const noexcept {
        return server_;
    }

private:
    [[nodiscard]] static tcp_server_config
    to_tcp_server_config(unix_stream_server_config config) noexcept {
        tcp_server_config tcp_config;
        tcp_config.connection = config.connection;
        tcp_config.connection_close_timeout = config.connection_close_timeout;
        return tcp_config;
    }

    tcp_server server_;
};

class unix_stream_client {
public:
    explicit unix_stream_client(af::runtime &owner) : client_(owner) {}

    unix_stream_client(const unix_stream_client &) = delete;
    unix_stream_client &operator=(const unix_stream_client &) = delete;

    [[nodiscard]] bool connect(unix_stream_connect_config config,
                               unix_stream_client_callbacks callbacks = {}) noexcept {
        if (config.endpoint.family != address_family::unix_domain ||
            (config.bind_local && config.local_endpoint.family != address_family::unix_domain)) {
            return false;
        }
        tcp_client_connect_config connect_config;
        connect_config.name = std::move(config.name);
        connect_config.remote_endpoint = std::move(config.endpoint);
        connect_config.local_endpoint = std::move(config.local_endpoint);
        connect_config.bind_local = config.bind_local;
        connect_config.owner_thread = config.owner_thread;
        connect_config.connection = config.connection;
        connect_config.connect_timeout = config.connect_timeout;
        return client_.connect(std::move(connect_config), callbacks);
    }

    [[nodiscard]] bool stop() noexcept {
        return client_.stop();
    }

    [[nodiscard]] bool running() const noexcept {
        return client_.running();
    }

    [[nodiscard]] std::size_t connection_count() const noexcept {
        return client_.connection_count();
    }

    [[nodiscard]] std::size_t pending_connect_count() const noexcept {
        return client_.pending_connect_count();
    }

    [[nodiscard]] af::thread_ref owner_thread() const noexcept {
        return client_.owner_thread();
    }

    [[nodiscard]] tcp_client &stream_client() noexcept {
        return client_;
    }

    [[nodiscard]] const tcp_client &stream_client() const noexcept {
        return client_;
    }

private:
    tcp_client client_;
};

class unix_datagram_socket {
public:
    explicit unix_datagram_socket(af::runtime &owner) : owner_(&owner), socket_(owner) {}

    unix_datagram_socket(const unix_datagram_socket &) = delete;
    unix_datagram_socket &operator=(const unix_datagram_socket &) = delete;

    [[nodiscard]] bool bind(unix_datagram_bind_config config,
                            unix_datagram_callbacks callbacks = {}) noexcept {
        if (config.local_endpoint.family != address_family::unix_domain) {
            return false;
        }
        if (!normalize_threads(config.threads)) {
            return false;
        }
        config.options.reuse_port = false;
        udp_socket_config socket_config;
        socket_config.name = std::move(config.name);
        socket_config.local_endpoint = std::move(config.local_endpoint);
        socket_config.threads = std::move(config.threads);
        socket_config.options = config.options;
        socket_config.connect_remote = false;
        return socket_.start(std::move(socket_config), callbacks);
    }

    [[nodiscard]] bool listen(unix_datagram_bind_config config,
                              unix_datagram_callbacks callbacks = {}) noexcept {
        return bind(std::move(config), callbacks);
    }

    [[nodiscard]] bool connect(unix_datagram_connect_config config,
                               unix_datagram_callbacks callbacks = {}) noexcept {
        if (config.local_endpoint.family != address_family::unix_domain ||
            config.remote_endpoint.family != address_family::unix_domain) {
            return false;
        }
        if (!normalize_threads(config.threads)) {
            return false;
        }
        config.options.reuse_port = false;
        udp_socket_config socket_config;
        socket_config.name = std::move(config.name);
        socket_config.local_endpoint = std::move(config.local_endpoint);
        socket_config.remote_endpoint = std::move(config.remote_endpoint);
        socket_config.threads = std::move(config.threads);
        socket_config.options = config.options;
        socket_config.connect_remote = true;
        return socket_.start(std::move(socket_config), callbacks);
    }

    [[nodiscard]] bool stop() noexcept {
        return socket_.stop();
    }

    [[nodiscard]] bool running() const noexcept {
        return socket_.running();
    }

    [[nodiscard]] std::size_t active_shard_count() const noexcept {
        return socket_.active_shard_count();
    }

    [[nodiscard]] unix_datagram_socket_handle handle() const noexcept {
        return socket_.handle();
    }

    [[nodiscard]] unix_datagram_socket_handle
    handle_for_thread(af::thread_ref thread) const noexcept {
        return socket_.handle_for_thread(thread);
    }

    [[nodiscard]] const unix_endpoint *local_endpoint(af::thread_ref thread) const noexcept {
        return socket_.local_endpoint(thread);
    }

    [[nodiscard]] udp_socket &datagram_socket() noexcept {
        return socket_;
    }

    [[nodiscard]] const udp_socket &datagram_socket() const noexcept {
        return socket_;
    }

private:
    [[nodiscard]] bool normalize_threads(std::vector<af::thread_ref> &threads) const {
        if (!threads.empty()) {
            return threads.size() == 1U;
        }
        if (owner_ == nullptr || af::runtime::current() != owner_) {
            return false;
        }
        const af::runtime::thread_index current = af::runtime::current_thread_index();
        if (!owner_->valid_thread(current) ||
            owner_->thread_kind_of(current) != af::thread_kind::io) {
            return false;
        }
        try {
            threads.push_back(af::thread_ref(current));
        } catch (...) {
            return false;
        }
        return true;
    }

    af::runtime *owner_{nullptr};
    udp_socket socket_;
};

} // namespace af::net
