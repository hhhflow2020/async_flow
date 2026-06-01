#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "io_rpc_length_prefixed_runtime.hpp"

#if defined(__linux__)

namespace io_rpc_length_prefixed_example {

class RpcClientTask final : public RpcTask {
public:
    explicit RpcClientTask(RpcTask::FactoryToken token) : RpcTask(token) {}

    bool do_it(
        int fd,
        sockaddr_in server,
        socklen_t server_size,
        bool* ok,
        int* error,
        bool* response_ok) {
        stream_.reset(RpcThread::IO_0, fd);
        server_ = server;
        server_size_ = server_size;
        ok_ = ok;
        error_ = error;
        response_ok_ = response_ok;
        return schedule(RpcThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Connect,
        SendRequestHeader,
        SendRequestBody,
        ReadResponseHeader,
        ReadResponseBody,
    };

#define AF_EXAMPLE_IO_RPC_LENGTH_PREFIXED_CLIENT_FRAGMENT_INCLUDE 1
#include "io_rpc_length_prefixed_client_flow.hpp"
#include "io_rpc_length_prefixed_client_request.hpp"
#include "io_rpc_length_prefixed_client_response.hpp"
#undef AF_EXAMPLE_IO_RPC_LENGTH_PREFIXED_CLIENT_FRAGMENT_INCLUDE

    State state_{State::Connect};
    af::TcpStream<RpcThread> stream_{};
    sockaddr_in server_{};
    socklen_t server_size_{sizeof(server_)};

    af::IoOpState connect_{};
    af::IoOpState write_{};
    af::IoOpState read_{};

    char request_header_[4]{};
    std::size_t request_header_written_{0};
    char request_[kMaxFrameBytes]{};
    std::size_t request_size_{0};
    std::size_t request_written_{0};

    char response_header_[4]{};
    std::size_t response_header_read_{0};
    char response_[kMaxFrameBytes]{};
    std::size_t response_size_{0};
    std::size_t response_read_{0};

    bool* ok_{nullptr};
    int* error_{nullptr};
    bool* response_ok_{nullptr};
};

} // namespace io_rpc_length_prefixed_example

#endif
