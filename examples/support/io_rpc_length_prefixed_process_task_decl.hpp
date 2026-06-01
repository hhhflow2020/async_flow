#pragma once

class RpcServerTask;

class RpcProcessTask final : public RpcTask {
public:
    explicit RpcProcessTask(RpcTask::FactoryToken token) : RpcTask(token) {}

    bool do_it(RpcServerTask* server);

private:
    af::TaskResult run() override;

    RpcServerTask* server_{nullptr};
};
