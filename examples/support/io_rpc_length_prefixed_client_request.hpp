#if !defined(AF_EXAMPLE_IO_RPC_LENGTH_PREFIXED_CLIENT_FRAGMENT_INCLUDE)
#error "io_rpc_length_prefixed_client_request.hpp is a RpcClientTask implementation fragment"
#endif

af::TaskResult connect() {
    const af::IoStatus status = stream_.connect(
        *this,
        reinterpret_cast<const sockaddr*>(&server_),
        server_size_,
        connect_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready()) {
        return complete(status.failed() ? status.error : EIO);
    }

    const char request[] = "PING";
    request_size_ = sizeof(request) - 1U;
    std::memcpy(request_, request, request_size_);
    request_written_ = 0;
    request_header_written_ = 0;
    const std::uint32_t net_len = htonl(static_cast<std::uint32_t>(request_size_));
    std::memcpy(request_header_, &net_len, sizeof(net_len));

    state_ = State::SendRequestHeader;
    return again();
}

af::TaskResult send_request_header() {
    const af::IoStatus status = stream_.send_some(
        *this,
        request_header_ + request_header_written_,
        4U - request_header_written_,
        write_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || status.bytes == 0U) {
        return complete(status.failed() ? status.error : EIO);
    }
    request_header_written_ += status.bytes;
    if (request_header_written_ < 4U) {
        return again();
    }

    state_ = State::SendRequestBody;
    return again();
}

af::TaskResult send_request_body() {
    const af::IoStatus status = stream_.send_some(
        *this,
        request_ + request_written_,
        request_size_ - request_written_,
        write_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || status.bytes == 0U) {
        return complete(status.failed() ? status.error : EIO);
    }
    request_written_ += status.bytes;
    if (request_written_ < request_size_) {
        return again();
    }

    state_ = State::ReadResponseHeader;
    return again();
}
