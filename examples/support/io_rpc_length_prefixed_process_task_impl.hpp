#if !defined(IO_RPC_LENGTH_PREFIXED_SERVER_FRAGMENT_INCLUDE)
#error "io_rpc_length_prefixed_process_task_impl.hpp is an RPC server implementation fragment"
#endif

inline bool RpcProcessTask::do_it(RpcServerTask* server) {
    server_ = server;
    return schedule(RpcThread::Logic_0);
}

inline af::TaskResult RpcProcessTask::run() {
    if (server_ == nullptr) {
        return done();
    }

    const char* request = server_->request_data();
    const std::size_t request_size = server_->request_size();
    if (request_size == 4U && std::memcmp(request, "PING", 4U) == 0) {
        server_->set_response("PONG", 4U);
    } else {
        server_->set_response(request, request_size);
    }

    if (!rpc_async::post(RpcThread::IO_0, server_)) {
        return done();
    }
    return done();
}
