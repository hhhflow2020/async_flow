#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "af/net/tcp_client.hpp"
#include "af/net/udp_socket.hpp"

namespace af::net {

template <typename Runtime> using UnixConnectionRef = TcpConnectionRef<Runtime>;
template <typename Runtime> using UnixConnectionHandle = TcpConnectionHandle<Runtime>;
template <typename Runtime> using UnixDatagramSocketRef = UdpSocketRef<Runtime>;
template <typename Runtime> using UnixDatagramSocketHandle = UdpSocketHandle<Runtime>;

struct UnixStreamRuntimeConfig {
    std::size_t command_queue_capacity{TcpClientRuntimeConfig{}.command_queue_capacity};
};

struct UnixDatagramRuntimeConfig {
    std::size_t command_queue_capacity{UdpSocketRuntimeConfig{}.command_queue_capacity};
};

namespace detail {

[[nodiscard]] inline TcpServerConfig to_tcp_server_config(UnixStreamRuntimeConfig config) noexcept {
    static_cast<void>(config);
    return TcpServerConfig{};
}

[[nodiscard]] inline TcpClientRuntimeConfig
to_tcp_client_config(UnixStreamRuntimeConfig config) noexcept {
    return TcpClientRuntimeConfig{.command_queue_capacity = config.command_queue_capacity};
}

[[nodiscard]] inline UdpSocketRuntimeConfig
to_udp_socket_config(UnixDatagramRuntimeConfig config) noexcept {
    return UdpSocketRuntimeConfig{.command_queue_capacity = config.command_queue_capacity};
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

} // namespace af::net
