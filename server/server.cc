
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

        LOG(INFO) << "Start Sync All Servers";

        HeartbeatAllServers();

        LOG(INFO) << "Sync Servers Complete";

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
            LOG(INFO) << "------ epoch "<< cur_epoch << " start ------";
            while (GetTime() - start_time < epoch_manager_->GetEpochDuration())
            {

                PB::Txn *txn = new PB::Txn();
                client_->GetTxn(&txn);
                txn->set_txn_id(GenerateTid());
                txn->set_start_epoch(cur_epoch);
                local_txns.Push(*txn);
            }

            LOG(INFO) << "epoch " << cur_epoch << "txns collected, start distribute and merge";
            // process with all other shard peer
            Work();

            LOG(INFO) << "------ epoch "<< cur_epoch << " end ------";
            
            epoch_manager_->AddPhysicalEpoch();
        } 
    }

    void Server::Distribute()
    {
        // <node_id, batch_txns>
        // sync in local replica
        LOG(INFO) << "Start Distribute";

        std::map<int, PB::MessageProto> batch_txns;
        for (std::map<int, Node*>::iterator iter = config_->all_nodes_.begin(); iter != config_->all_nodes_.end(); iter++)
        {
            if (iter->second->replica_id != config_->replica_id_)
            {
               continue;
            }
            PB::MessageProto mp;
            mp.set_src_node_id(config_->node_id_);
            mp.set_dest_node_id(iter->first);
            mp.set_dest_channel("Shard");
            mp.set_type(PB::MessageProto_MessageType_BATCHTXNS);
            batch_txns[iter->first] = mp;
        }

        // divide txn into subtxns
        for (size_t i = 0; i < local_txns.Size(); i++)
        {
            PB::Txn* txn = new PB::Txn();
            bool res = local_txns.Pop(txn);
            std::map<int, PB::Txn> subtxns; // <node_id, subtxn>

            for (size_t j = 0; j < txn->commands_size(); j++)
            {
                const PB::Command& single_cmd = txn->commands(j);
                int local_replica_id = config_->all_nodes_[config_->node_id_]->replica_id;
                // find responsible node for the key in local replica
                int mds = config_->LookupPartition(local_replica_id, single_cmd.key());
                if (subtxns.count(mds) == 0)
                {
                    PB::Txn subtxn;
                    subtxn.set_txn_id(txn->txn_id());
                    subtxn.set_start_epoch(txn->start_epoch());
                    subtxns[mds] = subtxn;
                }
                PB::Command* added_cmd_ptr = subtxns[mds].add_commands();
                added_cmd_ptr->CopyFrom(single_cmd);
            }

            // add subtxns to batch
            for(std::map<int, PB::Txn>::iterator iter = subtxns.begin(); iter != subtxns.end(); ++iter)
            {
                int node_id = iter->first;
                const PB::Txn& subtxn = iter->second;
                batch_txns[node_id].mutable_batch_txns()->add_txns()->CopyFrom(subtxn);
            }
        }

        // send msg to *all* peer node in replica

        for (const auto& val: config_->all_nodes_)
        {
            // mayby empty msg
            conn_->Send(batch_txns[val.first]);
        }

        LOG(INFO) << "send subtxns msg finish";
        // barrier : wait for all other msg arrive

        int counter = 0;
        PB::MessageProto* recv_subtxn = new PB::MessageProto();
        while (counter < config_->replica_size_)
        {
            if(conn_->GetMessage("Shard", recv_subtxn))
            {
                counter++;
                all_subtxns.Push(*recv_subtxn);
            }
        }

        LOG(INFO) << "Distribute Finish";
    }

    // send inregion subtxn to all other region's peer node
    void Server::Replicate()
    {
        LOG(INFO) << "Start Replicate";

        PB::MessageProto* send_msg_ptr = new PB::MessageProto();
        for (size_t i = 0; i < all_subtxns.Size(); i++)
        {
            PB::MessageProto* subtxn_ptr = new PB::MessageProto();
            while (!all_subtxns.Pop(subtxn_ptr))
            {
                sleep(100);
            }
            send_msg_ptr->mutable_batch_txns()->add_txns()->CopyFrom(*subtxn_ptr);
            all_subtxns.Push(*subtxn_ptr);
        }

        for (std::map<int, Node*>::iterator iter = config_->all_nodes_.begin(); iter != config_->all_nodes_.end(); iter++)
        {
            if (iter->second->replica_id != config_->replica_id_ && iter->second->partition_id == config_->partition_id_)
            {
                // broadcast to all other peer node
                PB::MessageProto mp(*send_msg_ptr);
                mp.set_src_node_id(config_->node_id_);
                mp.set_dest_node_id(iter->first);
                mp.set_dest_channel("Replica");
                mp.set_type(PB::MessageProto_MessageType_BATCHTXNS);
                conn_->Send(mp);
            }
        }
        
        // barrier : wait for all other msg arrive
        int counter = 1;
        PB::MessageProto* peer_subtxn = new PB::MessageProto();
        while (counter < config_->replica_num_)
        {
            if (conn_->GetMessage("Replica", peer_subtxn))
            {
                counter++;
                peer_subtxns.Push(*peer_subtxn);
            }
        }
        
        LOG(INFO) << "Replicate Finish";
    }

    // process crdt merge
    void Server::Merge()
    {
        std::vector<PB::Txn> local_and_remote_txns;
        // write intent
        while (!all_subtxns.Empty())
        {
            PB::MessageProto* subtxn = new PB::MessageProto();
            all_subtxns.Pop(subtxn);
            for (auto &&elem : subtxn->batch_txns().txns())
            {
                local_and_remote_txns.push_back(elem);
                WriteIntent(elem);
            }
        }
        
        while (!peer_subtxns.Empty())
        {
            PB::MessageProto* subtxn = new PB::MessageProto();
            peer_subtxns.Pop(subtxn);
            for (auto &&elem : subtxn->batch_txns().txns())
            {
                local_and_remote_txns.push_back(elem);
                WriteIntent(elem);
            }
        }
        
        // validate
        for (auto &&elem : local_and_remote_txns)
        {
            if (!Validate(elem))
            {
                aborted_txnid.insert(elem.txn_id());
            }
        }
        
        // broadcast abort id
        for (std::map<int, Node*>::iterator iter = config_->all_nodes_.begin(); iter != config_->all_nodes_.end(); iter++)
        {
            if (iter->second->replica_id != config_->replica_id_)
            {
                continue;
            }
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
        int counter = 0;
        std::set<uint64> recv_abort_tids;

        // barrier : wait for abort info arrive

        while (counter < config_->replica_size_)
        {
            PB::MessageProto *abort_msg = new PB::MessageProto();
            if (conn_->GetMessage("abort_tid", abort_msg))
            {
                for (const uint64 id : abort_msg->abort_tids().txn_ids())
                {
                    recv_abort_tids.insert(id);
                }
                counter++;
            }
        }
        // remove aborted txn
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
    }

    // worker
    void Server::Work()
    {
        Distribute();
        Replicate();
        Merge();
    }
} // namespace taas



