#if !defined(AF_EXAMPLE_IO_RPC_LENGTH_PREFIXED_CLIENT_FRAGMENT_INCLUDE)
#error "io_rpc_length_prefixed_client_response.hpp is a RpcClientTask implementation fragment"
#endif

af::TaskResult read_response_header() {
    const af::IoStatus status = stream_.recv_some(
        *this,
        response_header_ + response_header_read_,
        4U - response_header_read_,
        read_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || status.bytes == 0U) {
        return complete(status.failed() ? status.error : EIO);
    }
    response_header_read_ += status.bytes;
    if (response_header_read_ < 4U) {
        return again();
    }

    std::uint32_t net_len = 0;
    std::memcpy(&net_len, response_header_, sizeof(net_len));
    response_size_ = static_cast<std::size_t>(ntohl(net_len));
    response_read_ = 0;
    if (response_size_ > kMaxFrameBytes) {
        return complete(EMSGSIZE);
    }

    state_ = State::ReadResponseBody;
    return again();
}

af::TaskResult read_response_body() {
    if (response_size_ == 0U) {
        return complete(0);
    }

    const af::IoStatus status = stream_.recv_some(
        *this,
        response_ + response_read_,
        response_size_ - response_read_,
        read_);
    if (status.pending()) {
        return pending();
    }
    if (!status.ready() || status.bytes == 0U) {
        return complete(status.failed() ? status.error : EIO);
    }
    response_read_ += status.bytes;
    if (response_read_ < response_size_) {
        return again();
    }

    const bool ok = response_size_ == 4U && std::memcmp(response_, "PONG", 4U) == 0;
    *response_ok_ = ok;
    return complete(0);
}
