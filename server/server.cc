
#include <zmqpp/zmqpp.hpp>

#include "server.h"

namespace taas 
{
    Server::Server(Configuration* config, Connection* conn)
        :config_(config), conn_(conn), deconstructor_invoked_(false)
    {
        storage_ = new Storage();
        epoch_manager_ = &EpochManager::GetInstance();
        client_ = new Client();

        XLOGI("init server complete, start sync all servers\n");

        HeartbeatAllServers();

        XLOGI("sync all servers complete\n");

        pack_thread_ = std::thread(&Server::Run, this);
        // listen_thread.join();
    }

    Server::~Server() 
    {
        deconstructor_invoked_ = true;
    }

    uint64_t Server::GenerateTid()
    {
        uint64_t ms = std::chrono::system_clock::now().time_since_epoch().count();
        return (ms << 3) + static_cast<uint64>(config_->node_id_); // *8 + node_id
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

    void Server::HeartbeatAllServers()
    {
        conn_->NewChannel("synchronization_sequencer_channel");
        PB::MessageProto sync_msg;
        sync_msg.set_type(PB::MessageProto_MessageType_HEARTBEAT);
        sync_msg.set_dest_channel("synchronization_sequencer_channel");
        sync_msg.set_src_node_id(config_->node_id_);
        for (std::map<int, Node*>::iterator iter = config_->all_nodes_.begin();
            iter != config_->all_nodes_.end(); ++iter)
        {
;
            if (iter->first == config_->node_id_)
            {
                continue;
            }
            sync_msg.set_dest_node_id(iter->first);
            conn_->Send(sync_msg);
        }
        
        int sync_server_cnt = 1;
        // sync_msg.Clear();
        while (sync_server_cnt < config_->all_nodes_.size())
        {
            if(conn_->GetMessage("synchronization_sequencer_channel", &sync_msg))
            {
                XLOGI("heartbeat received reply msg\n");
                sync_server_cnt++;
            }
        }
        // communicate complete
        conn_->DeleteChannel("synchronization_sequencer_channel");
    }

    void Server::WriteIntent(const PB::Txn& txn)
    {
        for (const auto &stat : txn.commands())
        {
            if (stat.type() == PB::OpType::PUT)
            {
                if (crdt_map_.count(stat.key()) &&  crdt_map_[stat.key()] < txn.txn_id())
                {
                    // frist wirite win
                }
                else
                {
                    crdt_map_[stat.key()] = txn.txn_id();
                }
            }
        }
    }

    bool Server::Validate(const PB::Txn& txn)
    {
        for (const auto &stat : txn.commands())
        {
            if (stat.type() == PB::OpType::PUT)
            {
                if (crdt_map_[stat.key()] == txn.txn_id())
                {
                    /* code */
                }
                else
                {
                    return false;
                }
            }
        }
        return true;
    }

    void Server::Run()
    {
        while (!deconstructor_invoked_)
        {
            double start_time = GetTime();
            uint64 cur_epoch;
            std::set<uint64> local_abort_txn_ids;
            cur_epoch = epoch_manager_->GetPhysicalEpoch();
            XLOGI("----- epoch %ld start -----\n", cur_epoch);
            while (GetTime() - start_time < epoch_manager_->GetEpochDuration())
            {

                PB::Txn *txn = new PB::Txn();
                client_->GetTxn(&txn);
                txn->set_txn_id(GenerateTid());
                txn->set_start_epoch(cur_epoch);
                local_txns.Push(*txn);
            }

            // process with all other shard peer

            conn_->NewChannel("atomic");
            conn_->NewChannel("abort_tid");
            DistributeAndHold();
            conn_->DeleteChannel("atomic");
            conn_->DeleteChannel("abort_tid");

            XLOGI("----- epoch %ld end -----\n", cur_epoch);
            
            epoch_manager_->AddPhysicalEpoch();
        } 
    }

    void Server::DistributeAndHold()
    {
        // <node_id, batch_txns>
        std::map<int, PB::MessageProto> batch_txns;
        for (std::map<int, Node*>::iterator iter = config_->all_nodes_.begin(); iter != config_->all_nodes_.end(); iter++)
        {
            PB::MessageProto mp;
            mp.set_src_node_id(config_->node_id_);
            // mp.set_src_channel("atomic");
            mp.set_dest_node_id(iter->first);
            mp.set_dest_channel("atomic");
            mp.set_type(PB::MessageProto_MessageType_BATCHTXNS);
            batch_txns[iter->first] = mp;
        }
        
        // distribute subtxn
        for (size_t i = 0; i < local_txns.Size(); i++)
        {
            PB::Txn* txn = new PB::Txn();
            bool res = local_txns.Pop(txn);

            std::map<int, PB::Txn> subtxns;
            for (size_t j = 0; j < txn->commands_size(); j++)
            {
                const PB::Command& single_cmd = txn->commands(j);
                int mds = config_->LookupPartition(single_cmd.key());
                if (subtxns.count(mds) == 0)
                {
                    PB::Txn subtxn;
                    subtxn.set_txn_id(txn->txn_id());
                    subtxn.set_start_epoch(txn->start_epoch());
                    subtxns[mds] = subtxn;
                }
                PB::Command* cmd_ptr = subtxns[mds].add_commands();
                cmd_ptr->CopyFrom(single_cmd);
                // cmd_ptr->set_type(single_cmd.type());
                // cmd_ptr->set_key(single_cmd.key());
                // cmd_ptr->set_value(single_cmd.value());
            }

            for(std::map<int, PB::Txn>::iterator iter = subtxns.begin(); iter != subtxns.end(); ++iter)
            {
                int node_id = iter->first;
                const PB::Txn& sub_txn = iter->second;
                batch_txns[node_id].mutable_batch_txns()->add_txns()->CopyFrom(subtxns[node_id]); 
            }
        }

        // Send to every peer node
        for (const auto& val: config_->all_nodes_)
        {
            conn_->Send(batch_txns[val.first]);
        }
        
        // recv & exec crdt merge
        std::vector<PB::MessageProto> cached_subtxns;
        std::set<uint64> aborted_txnid;
        int counter = 0;
        while (counter < config_->all_nodes_.size())
        {
            PB::MessageProto sub_txns;
            if (conn_->GetMessage("atomic", &sub_txns))
            {
                counter++;
                // write intent
                for (const PB::Txn& elem : sub_txns.batch_txns().txns())
                {
                    WriteIntent(elem);
                }
                cached_subtxns.push_back(std::move(sub_txns));
            }
        }
        // validate
        for (std::vector<PB::MessageProto>::iterator iter =  cached_subtxns.begin();
            iter != cached_subtxns.end(); ++iter)
        {
            for (const PB::Txn& txn : iter->batch_txns().txns())
            {
                if (!Validate(txn))
                {
                    aborted_txnid.insert(txn.txn_id());
                }
            }
        }
        
        // broadcast abort id
        for (std::map<int, Node*>::iterator iter = config_->all_nodes_.begin(); iter != config_->all_nodes_.end(); iter++)
        {
            PB::MessageProto mp;
            mp.set_src_node_id(config_->node_id_);
            mp.set_dest_node_id(iter->first);
            mp.set_dest_channel("abort_tid");
            mp.set_type(PB::MessageProto_MessageType_ABORTTIDS);

            for (const uint64 id : aborted_txnid)
            {
                mp.mutable_abort_tids()->add_txn_ids(id);
            }
            conn_->Send(mp);
        }
        // receive abort_tid set
        counter = 0;
        std::set<uint64> recv_abort_tids;
        while (counter < config_->all_nodes_.size())
        {
            PB::MessageProto sub_txns;
            if (conn_->GetMessage("abort_tid", &sub_txns))
            {
                for (const uint64 id : sub_txns.abort_tids().txn_ids())
                {
                    recv_abort_tids.insert(id);
                }
                counter++;
            }
        }
        // exec erase write set in map
        for (std::map<std::string, uint64>::iterator iter = crdt_map_.begin(); iter != crdt_map_.end(); )
        {
            if (recv_abort_tids.count(iter->second)) // has been aborted
            {
                crdt_map_.erase(iter++);
            }
            else
            {
                iter++;
            }
        }

        for (auto &&elem : crdt_map_)
        {
            XLOGI("record %s commit, its belong tid %ld\n", elem.first.c_str(), elem.second);
        }
    }

    void Server::Merge()
    {

    }
} // namespace taas



