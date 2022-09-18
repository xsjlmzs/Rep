
#include <zmqpp/zmqpp.hpp>

#include "server.h"

namespace taas 
{
    Server::Server(Configuration* config, Connection* conn)
        :config_(config), conn_(conn)
    {
        storage_ = new Storage();
        epoch_manager_ = EpochManager::GetInstance();

        XLOGI("init server complete, start listen thread\n");
        std::thread listen_thread(&Server::Run, this);
        listen_thread.join();
    }

    Server::~Server() {}

    uint64_t Server::GenerateTid()
    {
        uint64_t ms = std::chrono::system_clock::now().time_since_epoch().count();
        return (ms << 2) + static_cast<uint64_t>(config_->node_id_);
    }

    void Server::Execute(Txn* txn, PB::Reply* reply)
    {
        for (size_t i = 0; i < txn->get_mp().txns(0).commands().size(); i++)
        {
            const PB::Command cmd = txn->get_mp().txns(0).commands(i);
            if (cmd.type() == PB::OpType::GET)
            {
                std::string value = storage_->get(cmd.key());
                reply->add_query_res(value.c_str());
            }
            else if (cmd.type() == PB::OpType::PUT)
            {
                storage_->put(cmd.key(), cmd.value());
            }
        }
    }

    void Server::Run()
    {
        while (true)
        {
            if (!conn_->client_reqs_.empty())
            {
                XLOGI("server has received a request\n");
                PB::MessageProto mp = conn_->client_reqs_.front();
                conn_->client_reqs_.pop();
                int epoch_no = epoch_manager_->GetPhysicalEpoch();
                uint64_t tid = GenerateTid();
                Txn* txn = new Txn(mp);
                
                txn->set_epoch_no(epoch_no);
                txn->set_tid(tid);
                PB::Reply reply;
                Execute(txn, &reply);
                delete txn;
                conn_->replies_.push(reply);
            }
        }
    }
} // namespace taas



