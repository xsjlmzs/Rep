
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

    void Server::Execute(const Txn& txn, PB::ClientReply* reply)
    {
        for (auto && cmd : txn.pb_txn_.commands())
        {
            if (cmd.type() == PB::OpType::GET)
            {
                std::string value = storage_->get(cmd.key());
                reply->add_query_set(value.c_str());
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
                PB::ClientRequest cr = conn_->client_reqs_.front();
                conn_->client_reqs_.pop();
                int start_epoch_no = epoch_manager_->GetPhysicalEpoch();
                uint64_t tid = GenerateTid();
                Txn txn = Txn(tid, start_epoch_no, cr.txn());
                // receive a client request and pack to txn

                PB::ClientReply reply;
                Execute(txn, &reply);
                conn_->replies_.push(reply);
            }
        }
    }
} // namespace taas



