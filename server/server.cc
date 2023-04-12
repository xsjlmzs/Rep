
#include <zmqpp/zmqpp.hpp>

#include "server.h"

namespace taas 
{
    Server::Server(Configuration *config, Connection *conn, Client *client)
        :config_(config), conn_(conn), client_(client),deconstructor_invoked_(false)
    {
        local_server_id_ = config_->node_id_;
        storage_ = new Storage();
        epoch_manager_ = &EpochManager::GetInstance();
        thread_pool_ = new ThreadPool();
        thread_pool_->init();
        LOG(INFO) << "Start Sync All Servers";

        HeartbeatAllServers();

        LOG(INFO) << "Sync Servers Complete";

        worker_ = std::thread(&Server::Run, this);
        // listen_thread.join();
    }

    Server::~Server() 
    {
        deconstructor_invoked_ = true;
        delete storage_, thread_pool_;
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

    // exec read op and fill the 'value'
    void Server::ExecRead(PB::Txn& txn)
    {
        for (size_t i = 0; i < txn.commands().size(); i++)
        {
            PB::Command* cmd = txn.mutable_commands(i);
            if (cmd->type() == PB::OpType::GET)
            {
                std::string value = storage_->get(cmd->key());
                cmd->set_value(value);
            }
        }
    }

    void Server::ExecWrite(const PB::Txn& txn)
    {
        for (size_t i = 0; i < txn.commands().size(); i++)
        {
            const PB::Command& cmd = txn.commands(i);
            if (cmd.type() == PB::OpType::PUT)
            {
                storage_->put(cmd.key(), cmd.value());
            }
        }
    }

    void Server::BatchWrite(const std::vector<PB::Txn>* txns)
    {
        std::vector<std::pair<std::string, std::string>> kv_pairs;
        for (auto &&txn : *txns)
        {
            for (auto &&cmd : txn.commands())
            {
                kv_pairs.push_back(std::pair<std::string, std::string>(cmd.key(), cmd.value()));
            }
        }
        storage_->batch_put(kv_pairs);
    }

    void Server::HeartbeatAllServers()
    {
        std::string channel = "Heartbeat";
        conn_->NewChannel(channel);
        PB::MessageProto sync_msg;
        sync_msg.set_type(PB::MessageProto_MessageType_HEARTBEAT);
        sync_msg.set_dest_channel(channel);
        sync_msg.set_src_node_id(local_server_id_);
        for (std::map<uint32, Node*>::iterator iter = config_->all_nodes_.begin();
            iter != config_->all_nodes_.end(); ++iter)
        {
            uint32 remote_server_id = iter->first;
            if (remote_server_id == local_server_id_)
                continue;
            sync_msg.set_dest_node_id(remote_server_id);
            conn_->Send(sync_msg);
        }
        
        // waiting for the replies from rest servers of cluster
        int sync_server_cnt = 1;
        // sync_msg.Clear();
        while (sync_server_cnt < config_->all_nodes_.size())
        {
            if(conn_->GetMessage(channel, &sync_msg))
            {
                sync_server_cnt++;
            }
        }
        // sync complete
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
                    // exist earlier record
                }
                else
                {
                    // write intent successfully
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
            uint64 cur_epoch = epoch_manager_->GetPhysicalEpoch();
            LOG(INFO) << "------ epoch "<< cur_epoch << " start ------";
            while (GetTime() - start_time < epoch_manager_->GetEpochDuration())
            {

                PB::Txn *txn = new PB::Txn();
                client_->GetTxn(&txn, GenerateTid());
                txn->set_start_epoch(cur_epoch);
                txn->set_status(PB::TxnStatus::EXEC);
                txn->set_master_replica(config_->replica_id_);
                txn->set_master_node(local_server_id_);
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
        LOG(INFO) << "Start Distribute";
        std::string channel = "Shard_" + std::to_string(epoch);
        conn_->NewChannel(channel);
        std::map<uint32, PB::MessageProto> batch_subtxns;
        // prepare msg for all in-region peers
        for (std::map<uint32, Node*>::iterator iter = config_->all_nodes_.begin(); iter != config_->all_nodes_.end(); iter++)
        {
            if (iter->second->replica_id != config_->replica_id_)
               continue;
            uint32 remote_server_id = iter->first;
            PB::MessageProto mp;
            mp.set_src_node_id(local_server_id_);
            mp.set_dest_node_id(remote_server_id);
            mp.set_dest_channel(channel);
            mp.set_type(PB::MessageProto_MessageType_BATCHTXNS);
            batch_subtxns[remote_server_id] = mp;
        }

        // split txn into subtxns
        for (size_t i = 0; i < local_txns.size(); i++)
        {
            const PB::Txn& txn = local_txns.at(i);
            std::map<uint32, PB::Txn> subtxns; // <node_id, subtxn>
            for (size_t j = 0; j < txn.commands_size(); j++)
            {
                const PB::Command& stat = txn.commands(j);
                // find responsible node for the key in region
                int partition_id = config_->LookupPartition(stat.key());
                int machine_id = config_->LookupMachineID(partition_id);
                if (subtxns.count(machine_id) == 0)
                {
                    PB::Txn subtxn;
                    subtxn.set_txn_id(txn.txn_id());
                    subtxn.set_start_epoch(txn.start_epoch());
                    subtxn.set_status(txn.status());
                    // set responsible node as master
                    subtxn.set_master_replica(txn.master_replica());
                    subtxn.set_master_node(txn.master_node());
                    subtxns[machine_id] = subtxn;
                }
                subtxns[machine_id].add_commands()->CopyFrom(stat);
            }

            // compile subtxns to batch
            for(std::map<uint32, PB::Txn>::iterator iter = subtxns.begin(); iter != subtxns.end(); ++iter)
            {
                int remote_server_id = iter->first;
                const PB::Txn& subtxn = iter->second;
                batch_subtxns[remote_server_id].mutable_batch_txns()->add_txns()->CopyFrom(subtxn);
            }
        }
        
        // send batch_subtxns to all in-region peers
        for (std::map<uint32, PB::MessageProto>::iterator iter = batch_subtxns.begin(); iter != batch_subtxns.end(); ++iter)
        {
            conn_->Send(iter->second);
        }
        LOG(INFO) << "send subtxns msg finish";

        // barrier : wait for all other msg arrive
        int recv_msg_cnt = 0;
        PB::MessageProto recv_subtxn;
        std::vector<PB::MessageProto>* inregion_subtxns = new std::vector<PB::MessageProto>();
        while (recv_msg_cnt < config_->replica_size_)
        {
            if(conn_->GetMessage(channel, &recv_subtxn))
            {
                recv_msg_cnt++;
                inregion_subtxns->push_back(recv_subtxn);
            }
        }
        conn_->DeleteChannel(channel);
        LOG(INFO) << "Distribute Finish";
        return inregion_subtxns;
    }

    // send in-region subtxn to all other region's peer node
    std::vector<PB::MessageProto>*  Server::Replicate(const std::vector<PB::MessageProto>& inregion_subtxns, uint64 epoch)
    {
        LOG(INFO) << "Start Replicate";
        std::string channel = "Replica_" + std::to_string(epoch);
        conn_->NewChannel(channel);
        PB::MessageProto* send_msg_ptr = new PB::MessageProto();
        // union all in-region subtxns to a MessageProto
        for (size_t i = 0; i < inregion_subtxns.size(); i++)
        {
            const PB::MessageProto& subtxns = inregion_subtxns.at(i);
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                send_msg_ptr->mutable_batch_txns()->add_txns()->CopyFrom(subtxn);
            }
        }

        for (std::map<uint32, Node*>::iterator iter = config_->all_nodes_.begin(); iter != config_->all_nodes_.end(); iter++)
        {
            if (iter->second->replica_id != config_->replica_id_ && iter->second->partition_id == config_->partition_id_)
            {
                // broadcast to all other peer node
                uint32 remote_server_id = iter->first;
                PB::MessageProto mp(*send_msg_ptr);
                mp.set_src_node_id(local_server_id_);
                mp.set_dest_node_id(remote_server_id);
                mp.set_dest_channel(channel);
                mp.set_type(PB::MessageProto_MessageType_BATCHTXNS);
                conn_->Send(mp);
            }
        }
        delete send_msg_ptr;
        
        // barrier : wait for the rest msg from out-region's server
        int counter = 1; // except itself
        PB::MessageProto recv_subtxn;
        std::vector<PB::MessageProto>* outregion_subtxns = new std::vector<PB::MessageProto>();
        while (counter < config_->replica_num_)
        {
            // maybe empty
            if (conn_->GetMessage(channel, &recv_subtxn))
            {
                counter++;
                outregion_subtxns->push_back(recv_subtxn);
            }
        }
        conn_->DeleteChannel(channel);
        LOG(INFO) << "Replicate Finish";
        return outregion_subtxns;
    }

    // process crdt merge
    std::vector<PB::Txn>* Server::Merge(const std::vector<PB::MessageProto>& inregion_subtxns, const std::vector<PB::MessageProto>& outregion_subtxns, uint64 epoch)
    {
        LOG(INFO) << "Start Merge";
        std::string channel = "Merge" + std::to_string(epoch);
        conn_->NewChannel(channel);
        std::set<uint64> abort_subtxn_set;
        // write intent
        //  local txns
        for (auto &&subtxns : inregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                WriteIntent(subtxn, epoch);
            }
        }
        //  remote txns
        for (auto &&subtxns : outregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                WriteIntent(subtxn, epoch);
            }
        }

        // prepare reply msg for every in-region servers
        std::map<uint32, PB::MessageProto> batch_replies;
        for (std::map<uint32, Node*>::iterator iter = config_->all_nodes_.begin(); 
            iter != config_->all_nodes_.end(); ++iter)
        {
            // skip out-region nodes
            if (iter->second->replica_id != config_->replica_id_)
            {
                continue;
            }
            int remote_server_id = iter->first;
            PB::MessageProto mp;
            mp.set_src_node_id(local_server_id_);
            mp.set_dest_node_id(remote_server_id);
            mp.set_dest_channel(channel);
            mp.set_type(PB::MessageProto_MessageType_BATCHTXNS);
            batch_replies[remote_server_id] = mp;
        }
        
        // validate in-region txns 
        // only responsible for the in-region txn's reply
        for (auto &&subtxns : inregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                PB::Txn new_txn(subtxn);
                bool validate_res = Validate(new_txn, epoch);
                if (validate_res)
                {
                    ExecRead(new_txn);
                    new_txn.set_status(PB::COMMIT);
                }
                else
                {
                    new_txn.set_status(PB::ABORT);
                    abort_subtxn_set.insert(new_txn.txn_id());
                }
                uint32 remote_node_id = subtxns.src_node_id();
                batch_replies[remote_node_id].mutable_batch_txns()->add_txns()->CopyFrom(new_txn);
            }
        }
        
        // validate out-region txns
        for (auto &&subtxns : outregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                PB::Txn new_txn(subtxn);
                bool validate_res = Validate(new_txn, epoch);
                if (!validate_res)
                {
                    new_txn.set_status(PB::ABORT);
                    abort_subtxn_set.insert(new_txn.txn_id());
                }
                int remote_partition_id = subtxns.src_node_id() % config_->replica_size_;
                uint32 shadow_node_id = config_->LookupMachineID(remote_partition_id);
                batch_replies[shadow_node_id].mutable_batch_txns()->add_txns()->CopyFrom(new_txn);
            }
        }
        

        // send replies msg to in=region peers except itself
        for (std::map<uint32, PB::MessageProto>::iterator iter = batch_replies.begin();
            iter != batch_replies.end(); ++iter)
        {
            if(iter->first == config_->node_id_)
                continue;
            conn_->Send(iter->second);
        }
        
        int recv_msg_cnt = 1;
        std::vector<PB::MessageProto> recv_replies;

        // barrier : wait for reply msg arrive
        PB::MessageProto *reply_msg = new PB::MessageProto();
        while (recv_msg_cnt < config_->replica_size_)
        {
            if (conn_->GetMessage(channel, reply_msg))
            {
                recv_replies.push_back(*reply_msg);
                recv_msg_cnt++;
            }
        }
        delete reply_msg;

        // union abort set
        for (size_t i = 0; i < recv_replies.size(); i++)
        {
            for (auto &&subtxn : recv_replies[i].batch_txns().txns())
            {
                if (subtxn.status() == PB::TxnStatus::COMMIT)
                {
                    // return read result
                }
                else if (subtxn.status() == PB::TxnStatus::ABORT)
                {
                    abort_subtxn_set.insert(subtxn.txn_id());
                }
            }
        }
        
        std::vector<PB::Txn> *committable_subtxns = new std::vector<PB::Txn>();
        // write in
        for (auto &&subtxns : inregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                if (abort_subtxn_set.count(subtxn.txn_id()))
                {
                    // abort
                }
                else
                {
                    committable_subtxns->push_back(subtxn);
                }
            }
        }

        for (auto &&subtxns : outregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                if (abort_subtxn_set.count(subtxn.txn_id()))
                {
                    // abort
                }
                else
                {
                    committable_subtxns->push_back(subtxn);
                }
            }
        }
        
        conn_->DeleteChannel(channel);
        LOG(INFO) << "Merge Finish";
        return committable_subtxns;
    }

    // worker
    void Server::Work(uint64 epoch)
    {
        std::vector<PB::MessageProto> *inregion_subtxns, *outregion_subtxns;
        std::vector<PB::Txn> *committable_subtxns;
        // process distribute & collect all in-region subtxns
        inregion_subtxns = Distribute(local_txns_[epoch], epoch);
        // process replicate & collect all out-region subtxns
        outregion_subtxns = Replicate(*inregion_subtxns, epoch);
        // determinstic process merge
        // return value : kvs all will write in db 
        committable_subtxns = Merge(*inregion_subtxns, *outregion_subtxns, epoch);
        // atomic batch write in
        BatchWrite(committable_subtxns);

        // Check Correctness
        std::set<uint64> committed_tid_set;
        for (size_t i = 0; i < committable_subtxns->size(); i++)
        {
            const PB::Txn& txn = committable_subtxns->at(i);
            committed_tid_set.insert(txn.txn_id());
        }
        bool atomic_test = true;
        for (auto &&subtxns : *inregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                if (committed_tid_set.count(subtxn.txn_id()))
                {
                    // TxnStatus = COMMIT
                    for (auto &&stat : subtxn.commands())
                    {
                        if (stat.type() == PB::OpType::PUT)
                        {
                            std::string read_res = storage_->get(stat.key());
                            if (read_res != stat.value())
                            {
                                atomic_test = false;
                            }
                        }
                    }
                }
                else
                {
                    // TxnStatus = ABORT
                    for (auto &&stat : subtxn.commands())
                    {
                        if (stat.type() == PB::OpType::PUT)
                        {
                            std::string read_res = storage_->get(stat.key());
                            if (read_res == stat.value())
                            {
                                atomic_test = false;
                            }
                        }
                    }
                }
            }
        }
        LOG(ERROR) << "cant pass the subtxn's atomic test";
        
        delete inregion_subtxns, outregion_subtxns, committable_subtxns;
    }

    
} // namespace taas



