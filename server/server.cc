
#include <zmqpp/zmqpp.hpp>

#include "server.h"

namespace taas 
{
    Server::Server(Configuration* config, Connection* conn)
        :config_(config), conn_(conn), deconstructor_invoked_(false)
    {

        storage_ = new Storage();
        epoch_manager_ = &EpochManager::GetInstance();
        client_ = new Client(config, 0, 10000);
        thread_pool_ = new ThreadPool();
        thread_pool_->init();
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

    void Server::WriteIntent(const PB::Txn& txn, uint64 epoch)
    {
        for (const auto &stat : txn.commands())
        {
            if (stat.type() == PB::OpType::PUT)
            {
                if (crdt_map_[epoch].count(stat.key()) &&  crdt_map_[epoch][stat.key()] < txn.txn_id())
                {
                    // frist wirite win
                }
                else
                {
                    crdt_map_[epoch][stat.key()] = txn.txn_id();
                }
            }
        }
    }

    bool Server::Validate(const PB::Txn& txn, uint64 epoch)
    {
        for (const auto &stat : txn.commands())
        {
            if (stat.type() == PB::OpType::PUT)
            {
                if (crdt_map_[epoch][stat.key()] == txn.txn_id())
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
                client_->GetTxn(&txn, GenerateTid());
                txn->set_start_epoch(cur_epoch);
                local_txns_[cur_epoch].push_back(*txn);
                delete txn;
            }

            LOG(INFO) << local_txns_[cur_epoch].size() << " txns collected, start distribute and merge";
            // process with all other shard peer
            // worker
            thread_pool_->submit(std::bind(&Server::Work, this, cur_epoch));
            LOG(INFO) << "------ epoch "<< cur_epoch << " end ------";

            epoch_manager_->AddPhysicalEpoch();
        } 
    }

    std::vector<PB::MessageProto>* Server::Distribute(const std::vector<PB::Txn>& local_txns, uint64 epoch)
    {
        // <node_id, batch_txns>
        // sync in local replica
        LOG(INFO) << "Start Distribute";
        std::string channel = "Shard" + std::to_string(epoch);
        conn_->NewChannel(channel);
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
            mp.set_dest_channel(channel);
            mp.set_type(PB::MessageProto_MessageType_BATCHTXNS);
            batch_txns[iter->first] = mp;
        }

        // divide txn into subtxns
        for (size_t i = 0; i < local_txns.size(); i++)
        {
            PB::Txn txn ;
            txn = local_txns.at(i);
            std::map<int, PB::Txn> subtxns; // <node_id, subtxn>

            for (size_t j = 0; j < txn.commands_size(); j++)
            {
                const PB::Command& single_cmd = txn.commands(j);
                int local_replica_id = config_->all_nodes_[config_->node_id_]->replica_id;
                // find responsible node for the key in local replica
                int mds = config_->LookupPartition(local_replica_id, single_cmd.key());
                if (subtxns.count(mds) == 0)
                {
                    PB::Txn subtxn;
                    subtxn.set_txn_id(txn.txn_id());
                    subtxn.set_start_epoch(txn.start_epoch());
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
        PB::MessageProto recv_subtxn;
        std::vector<PB::MessageProto>* all_subtxns = new std::vector<PB::MessageProto>();
        while (counter < config_->replica_size_)
        {
            if(conn_->GetMessage(channel, &recv_subtxn))
            {
                counter++;
                all_subtxns->push_back(recv_subtxn);
            }
        }
        conn_->DeleteChannel(channel);
        LOG(INFO) << "Distribute Finish";
        return all_subtxns;
    }

    // send inregion subtxn to all other region's peer node
    std::vector<PB::MessageProto>*  Server::Replicate(const std::vector<PB::MessageProto>& all_subtxns, uint64 epoch)
    {
        LOG(INFO) << "Start Replicate";
        std::string channel = "Replica" + std::to_string(epoch);
        conn_->NewChannel(channel);
        PB::MessageProto* send_msg_ptr = new PB::MessageProto();
        for (size_t i = 0; i < all_subtxns.size(); i++)
        {
            PB::MessageProto subtxn;
            subtxn = all_subtxns.at(i);
            send_msg_ptr->mutable_batch_txns()->mutable_txns()->CopyFrom(subtxn.batch_txns().txns());
        }

        for (std::map<int, Node*>::iterator iter = config_->all_nodes_.begin(); iter != config_->all_nodes_.end(); iter++)
        {
            if (iter->second->replica_id != config_->replica_id_ && iter->second->partition_id == config_->partition_id_)
            {
                // broadcast to all other peer node
                PB::MessageProto mp(*send_msg_ptr);
                mp.set_src_node_id(config_->node_id_);
                mp.set_dest_node_id(iter->first);
                mp.set_dest_channel(channel);
                mp.set_type(PB::MessageProto_MessageType_BATCHTXNS);
                conn_->Send(mp);
            }
        }
        
        // barrier : wait for all other msg arrive
        int counter = 1;
        PB::MessageProto peer_subtxn;
        std::vector<PB::MessageProto>* peer_subtxns = new std::vector<PB::MessageProto>();
        while (counter < config_->replica_num_)
        {
            if (conn_->GetMessage(channel, &peer_subtxn))
            {
                counter++;
                peer_subtxns->push_back(peer_subtxn);
            }
        }
        conn_->DeleteChannel(channel);
        LOG(INFO) << "Replicate Finish";
        return peer_subtxns;
    }

    // process crdt merge
    std::vector<std::pair<std::string, std::string>>* Server::Merge(const std::vector<PB::MessageProto>& all_subtxns, const std::vector<PB::MessageProto>& peer_subtxns, uint64 epoch)
    {
        LOG(INFO) << "Start Merge";
        std::string channel = "abort_tid" + std::to_string(epoch);
        conn_->NewChannel(channel);
        std::vector<PB::Txn> local_and_remote_subtxns;
        // write intent
        for (auto &&subtxn : all_subtxns)
        {
            for (auto &&elem : subtxn.batch_txns().txns())
            {
                local_and_remote_subtxns.push_back(elem);
                WriteIntent(elem, epoch);
            }
        }
        
        for (auto &&subtxn : peer_subtxns)
        {
            for (auto &&elem : subtxn.batch_txns().txns())
            {
                local_and_remote_subtxns.push_back(elem);
                WriteIntent(elem, epoch);
            }
        }
        
        // validate
        std::set<uint64> local_aborted_tids;
        for (auto &&elem : local_and_remote_subtxns)
        {
            if (!Validate(elem, epoch))
            {
                local_aborted_tids.insert(elem.txn_id());
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
            mp.set_dest_channel(channel);
            mp.set_type(PB::MessageProto_MessageType_ABORTTIDS);

            for (const uint64 id : local_aborted_tids)
            {
                mp.mutable_abort_tids()->add_txn_ids(id);
            }
            conn_->Send(mp);
        }
        
        // receive abort_tid set
        int counter = 0;
        std::set<uint64> remote_abort_tids;

        // barrier : wait for abort info arrive

        while (counter < config_->replica_size_)
        {
            PB::MessageProto *abort_msg = new PB::MessageProto();
            if (conn_->GetMessage(channel, abort_msg))
            {
                for (const uint64 id : abort_msg->abort_tids().txn_ids())
                {
                    remote_abort_tids.insert(id);
                }
                counter++;
            }
        }

        // union local and remote abort txns' id
        for (uint64 id : local_aborted_tids)
        {
            remote_abort_tids.insert(id);
        }

        // remove aborted txn
        for (std::map<std::string, uint64>::iterator iter = crdt_map_[epoch].begin(); iter != crdt_map_[epoch].end(); )
        {
            if (remote_abort_tids.count(iter->second)) // has been aborted
            {
                crdt_map_[epoch].erase(iter++);
            }
            else
            {
                iter++;
            }
        }

        int win_counter = 0;
        std::vector<std::pair<std::string, std::string>> *win_subtxns = new std::vector<std::pair<std::string, std::string>>();
        for (auto &&subtxn : local_and_remote_subtxns)
        {
            if (!remote_abort_tids.count(subtxn.txn_id()))
            {
                win_counter ++;
                // dont contain the aborted txn id
                for (auto &&cmd : subtxn.commands())
                {
                    if (cmd.type() != PB::PUT)
                    {
                        continue;
                    }
                    std::pair<std::string, std::string> kv = std::make_pair(cmd.key(), cmd.value());
                    win_subtxns->emplace_back(kv);
                }
            }
        }
        conn_->DeleteChannel(channel);
        LOG(INFO) << "Merge Finish";
        LOG(INFO) << win_counter << " txns commit successfully";
        return win_subtxns;
    }

    // worker
    void Server::Work(uint64 epoch)
    {
        std::vector<PB::MessageProto> *all_subtxns, *peer_subtxns;
        std::vector<std::pair<std::string, std::string>> *win_subtxns;
        all_subtxns = Distribute(local_txns_[epoch], epoch);
        peer_subtxns = Replicate(*all_subtxns, epoch);
        win_subtxns = Merge(*all_subtxns, *peer_subtxns, epoch);
        // atomic write in storage all_subtxns + peer_subtxns
    }
} // namespace taas



