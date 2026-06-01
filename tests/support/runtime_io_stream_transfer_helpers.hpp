#pragma once

struct StreamSocketPair {
    af::UniqueFd first{};
    af::UniqueFd second{};
};

struct BlockingTcpConnection {
    af::UniqueFd listener{};
    af::UniqueFd client{};
    af::UniqueFd server{};
};

struct PipePair {
    af::UniqueFd read{};
    af::UniqueFd write{};
};

bool write_fd_all(int fd, const void *data, std::size_t size) {
    const auto *bytes = static_cast<const char *>(data);
    std::size_t offset = 0;
    while (offset < size) {
        const ssize_t n = ::write(fd, bytes + offset, size - offset);
        if (n > 0) {
            offset += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

bool create_temp_file_with_payload(af::UniqueFd &file, const char *prefix, const void *payload,
                                   std::size_t payload_size, bool rewind_to_start = true) {
    char path[128]{};
    const int written = std::snprintf(path, sizeof(path), "/tmp/%s-XXXXXX", prefix);
    if (written < 0 || static_cast<std::size_t>(written) >= sizeof(path)) {
        return false;
    }

    file.reset(::mkstemp(path));
    if (!file) {
        return false;
    }
    static_cast<void>(::unlink(path));
    if (!write_fd_all(file.get(), payload, payload_size)) {
        return false;
    }
    return !rewind_to_start || ::lseek(file.get(), 0, SEEK_SET) == 0;
}

bool create_stream_socket_pair(StreamSocketPair &sockets) {
    sockets = StreamSocketPair{};
    int fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        return false;
    }
    sockets.first.reset(fds[0]);
    sockets.second.reset(fds[1]);
    return true;
}

bool create_blocked_stream_socket_pair(StreamSocketPair &sockets) {
    return create_stream_socket_pair(sockets) && fill_until_blocked(sockets.first.get());
}

bool create_blocked_tcp_connection(BlockingTcpConnection &connection) {
    connection = BlockingTcpConnection{};

    int listener = -1;
    sockaddr_in address{};
    socklen_t address_size = sizeof(address);
    if (!create_tcp_listener(listener, address, address_size)) {
        return false;
    }
    connection.listener.reset(listener);

    connection.client.reset(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!connection.client) {
        return false;
    }
    const int rc =
        ::connect(connection.client.get(), reinterpret_cast<sockaddr *>(&address), address_size);
    if (rc != 0 && errno != EINPROGRESS) {
        return false;
    }

    connection.server.reset(accept_tcp_until_ready(connection.listener.get()));
    if (!connection.server) {
        return false;
    }

    int send_buffer = 4096;
    if (::setsockopt(connection.server.get(), SOL_SOCKET, SO_SNDBUF, &send_buffer,
                     static_cast<socklen_t>(sizeof(send_buffer))) != 0) {
        return false;
    }
    return fill_until_blocked(connection.server.get());
}

bool create_pipe_pair(PipePair &pipe) {
    pipe = PipePair{};
    int fds[2]{-1, -1};
    if (::pipe2(fds, O_NONBLOCK | O_CLOEXEC) != 0) {
        return false;
    }
    pipe.read.reset(fds[0]);
    pipe.write.reset(fds[1]);
    return true;
}
