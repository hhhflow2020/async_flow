#if !defined(AF_EXAMPLE_IO_RPC_LENGTH_PREFIXED_CLIENT_FRAGMENT_INCLUDE)
#error "io_rpc_length_prefixed_client_flow.hpp is a RpcClientTask implementation fragment"
#endif

af::TaskResult run() override {
    switch (state_) {
    case State::Connect:
        return connect();
    case State::SendRequestHeader:
        return send_request_header();
    case State::SendRequestBody:
        return send_request_body();
    case State::ReadResponseHeader:
        return read_response_header();
    case State::ReadResponseBody:
        return read_response_body();
    }
    return failed();
}

af::TaskResult complete(int error) {
    *error_ = error;
    *ok_ = true;
    return done();
}
