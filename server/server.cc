
#include "server.h"

extern int thread_num;
extern uint32 epoch_length;
extern uint64 run_epoch;
extern taas::Isolation isol;
extern uint32 limit_txns;
namespace taas 
{
    Server::Server(Configuration *config, Connection *conn, Client *client)
        :config_(config), conn_(conn), client_(client), isolation(isol), limit_epoch_(run_epoch), limit_txns_(limit_txns),deconstructor_invoked_(false)
    {
        local_server_id_ = config_->node_id_;
        storage_ = new Storage();
        epoch_manager_ = &EpochManager::GetInstance();
        thread_pool_ = new ThreadPool(thread_num);
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
        return (ms << 4) + static_cast<uint64>(config_->node_id_); // *8 + node_id
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
    void Server::ExecRead(PB::Txn* txn)
    {
        storage_->LockRead();
        for (size_t i = 0; i < txn->commands().size(); i++)
        {
            PB::Command* cmd = txn->mutable_commands(i);
            if (cmd->type() == PB::OpType::GET)
            {
                std::string value = storage_->get(cmd->key());
                cmd->set_value(value);
            }
        }
        storage_->UnlockRead();
    }

    void Server::ExecWrite(const PB::Txn& txn)
    {
        storage_->LockWrite();
        for (size_t i = 0; i < txn.commands().size(); i++)
        {
            const PB::Command& cmd = txn.commands(i);
            if (cmd.type() == PB::OpType::PUT)
            {
                storage_->put(cmd.key(), cmd.value());
            }
        }
        storage_->UnlockWrite();
    }

    void Server::BatchWrite(const std::vector<PB::Txn>* txns)
    {
        for (auto &&txn : *txns)
        {
            for (auto &&cmd : txn.commands())
            {
                if (cmd.type() == PB::OpType::PUT)
                {
                    storage_->put(cmd.key(), cmd.value());
                }
            }
        }
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
            usleep(100);
        }
        // sync complete
        conn_->DeleteChannel("Heartbeat");
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
                    // don't eliminate
                }
                else
                {
                    return false;
                }
            }
        }
        return true;
    }

    bool Server::ValidateReadSet(const PB::Txn& txn)
    {
        bool validate_res = true;
        std::string last_read_version = "";
        for (auto &&stat : txn.commands())
        {
            if (stat.type() == PB::OpType::GET)
            {
                std::string read_version = stat.value();
                storage_->LockRead();
                std::string cur_version = storage_->get(stat.key());
                storage_->UnlockRead();
                switch (isolation)
                {
                case kReadCommit:
                    validate_res = true;
                    break;
                case kRepeatableRead:
                    if (cur_version != read_version)
                        validate_res = false;
                    break;
                case kSnapshotIsolation:
                    if (last_read_version.empty())
                        last_read_version = read_version;
                    else if(last_read_version != read_version)
                        validate_res = false;
                    break;
                case kSerilizable: // not support
                    break;
                default:
                    break;
                }
                if (cur_version != stat.value())
                    return false;
            }
            if (!validate_res)
                break;
        }
        return validate_res;
    }

    void Server::Run()
    {
        launch_ts_ = GetTime();
        PB::Txn *txn = new PB::Txn();
        while (!deconstructor_invoked_)
        {
            uint64 start_time = GetTime();
            uint64 cur_epoch = epoch_manager_->GetPhysicalEpoch();
            
            // reach max running epoch, exit
            if (cur_epoch == limit_epoch_+1)
            {
                std::unique_lock<std::mutex> lk(cv_mutex_);
                cv_.wait(lk, [this]{return this->epoch_manager_->GetCommittedEpoch() == this->limit_epoch_; });
                lk.unlock();
                thread_pool_->shutdown();
                break;
            }
            
            // collect batch txns
            LOG(INFO) << "------ epoch "<< cur_epoch << " start ------";
            std::vector<PB::Txn> local_txns;
            while (GetTime() - start_time < epoch_manager_->GetEpochDuration())
            {
                if (local_txns_[cur_epoch].size() > limit_txns_)
                {
                    usleep(100);
                    continue;
                }
                
                client_->GetTxn(&txn, GenerateTid());
                txn->set_start_epoch(cur_epoch); // assume all txns are single epoch txn
                txn->set_end_epoch(cur_epoch);
                txn->set_status(PB::TxnStatus::PEND);
                txn->set_start_ts(GetTime());
                local_txns_[cur_epoch].push_back(*txn);
            }
            delete txn;

            LOG(INFO) << "epoch : " << cur_epoch << " " << local_txns_[cur_epoch].size() << " txns collected, start distribute and merge";
            // process with all other shard peer
            // worker
            thread_pool_->submit(std::bind(&Server::WorkFullReplica, this, cur_epoch));
            LOG(INFO) << "------ epoch "<< cur_epoch << " end ------";
            epoch_manager_->AddPhysicalEpoch();
        }
    }

    std::vector<PB::MessageProto>* Server::Distribute(const std::vector<PB::Txn>& local_txns, uint64 epoch)
    {
        std::string channel = "Shard_" + std::to_string(epoch);
        conn_->NewChannel(channel);
        std::map<uint32, PB::MessageProto> batch_subtxns;
        // prepare msg for sending to in region nodes
        for (std::map<uint32, Node*>::const_iterator iter = config_->all_nodes_.begin(); iter != config_->all_nodes_.end(); iter++)
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
                uint32 machine_id = config_->LookupMachineID(partition_id);
                if (subtxns.count(machine_id) == 0)
                {
                    PB::Txn subtxn;
                    subtxn.set_txn_id(txn.txn_id());
                    subtxn.set_start_epoch(txn.start_epoch());
                    subtxn.set_status(PB::TxnStatus::PEND);
                    subtxns[machine_id] = subtxn;
                }
                subtxns[machine_id].add_commands()->CopyFrom(stat);
            }

            // compile subtxns to batch
            for(std::map<uint32, PB::Txn>::iterator iter = subtxns.begin(); iter != subtxns.end(); ++iter)
            {
                uint32 remote_server_id = iter->first;
                const PB::Txn& subtxn = iter->second;
                PB::Txn* added_txn = batch_subtxns[remote_server_id].mutable_batch_txns()->add_txns();
                added_txn->CopyFrom(subtxn);
            }   
        }
        
        // send batch_subtxns to all in-region peers
        for (std::map<uint32, PB::MessageProto>::iterator iter = batch_subtxns.begin(); iter != batch_subtxns.end(); ++iter)
        {
            // iter->second.set_debug_info(std::to_string(epoch));
            conn_->Send(iter->second);
        }
        LOG(INFO) << "epoch : " << epoch << " have sent " << batch_subtxns.size() << " Distribute() msgs and barrier";
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
            else
            {
                usleep(100);
            }
        }
        LOG(INFO) << "epoch : " << epoch << " Distribute() barrier end"; 
        conn_->DeleteChannel(channel);
        return inregion_subtxns;
    }

    // send in-region subtxn to all other region's peer node
    std::vector<PB::MessageProto>*  Server::Replicate(const std::vector<PB::MessageProto>& inregion_subtxns, uint64 epoch)
    {
        std::string channel = "Replica_" + std::to_string(epoch);
        conn_->NewChannel(channel);
        PB::MessageProto* send_msg_ptr = new PB::MessageProto();
        // exec subtxn's read
        // union all in-region subtxns to a MessageProto
        for (size_t i = 0; i < inregion_subtxns.size(); i++)
        {
            const PB::MessageProto& subtxns = inregion_subtxns.at(i);
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                send_msg_ptr->mutable_batch_txns()->add_txns()->CopyFrom(subtxn);
            }
        }

        int sent_msg = 0;
        // send the whole subtxns in region to other replica's counterpart
        for (std::map<uint32, Node*>::const_iterator iter = config_->all_nodes_.begin(); iter != config_->all_nodes_.end(); iter++)
        {
            uint32 remote_replica_id = iter->second->replica_id;
            uint32 remote_partition_id = iter->second->partition_id;
            uint32 remote_server_id = iter->first;
            if (remote_replica_id != config_->replica_id_ && remote_partition_id == config_->partition_id_)
            {
                // broadcast to all other peer node
                uint32 remote_server_id = iter->first;
                PB::MessageProto mp(*send_msg_ptr);
                mp.set_src_node_id(local_server_id_);
                mp.set_dest_node_id(remote_server_id);
                mp.set_dest_channel(channel);
                mp.set_type(PB::MessageProto_MessageType_BATCHTXNS);
                conn_->Send(mp);
                sent_msg ++;
            }
        }
        delete send_msg_ptr;
        
        LOG(INFO) << "epoch : " << epoch << " Replicate() have sent " << sent_msg << " msgs and barrier";
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
            else
            {
                usleep(100);
            }
        }
        LOG(INFO) << "epoch : " << epoch << " Replicate() barrier end"; 
        conn_->DeleteChannel(channel);
        return outregion_subtxns;
    }

    // process crdt merge
    std::vector<PB::Txn>* Server::Merge(const std::vector<PB::MessageProto>& inregion_subtxns, const std::vector<PB::MessageProto>& outregion_subtxns, uint64 epoch)
    {
        std::string channel = "Merge_" + std::to_string(epoch);
        conn_->NewChannel(channel);
        std::set<uint64> abort_subtxn_set;
        // write intent for local txns and remote txns
        //  local txns
        for (auto &&subtxns : inregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                WriteIntent(subtxn, epoch);
            }
        }
        for (auto &&subtxns : outregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                WriteIntent(subtxn, epoch);
            }
        }

        // prepare reply messages for in-region servers
        std::map<uint32, PB::MessageProto> batch_replies;
        for (std::map<uint32, Node*>::const_iterator iter = config_->all_nodes_.begin(); 
            iter != config_->all_nodes_.end(); ++iter)
        {
            // skip out-region nodes
            int remote_replica = iter->second->replica_id;
            uint32 remote_server_id = iter->first;
            if (remote_replica == config_->replica_id_ )
            {
                PB::MessageProto mp;
                mp.set_src_node_id(local_server_id_);
                mp.set_dest_node_id(remote_server_id);
                mp.set_dest_channel(channel);
                mp.set_type(PB::MessageProto_MessageType_BATCHTXNS);
                batch_replies[remote_server_id] = mp;
            }
        }
        
        // validate in-region txns 
        // only responsible for the in-region txn's reply
        std::vector<PB::Txn>* pass_local_subtxns = new std::vector<PB::Txn>();
        for (auto &&subtxns : inregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                PB::Txn new_txn(subtxn);
                uint32 remote_node_id = subtxns.src_node_id();
                bool validate_res = Validate(subtxn, epoch);
                if (validate_res)
                {
                    new_txn.set_status(PB::COMMIT);
                    batch_replies[remote_node_id].mutable_batch_txns()->add_txns()->CopyFrom(new_txn);
                    pass_local_subtxns->push_back(new_txn);
                }
                else
                {
                    new_txn.set_status(PB::ABORT);
                    abort_subtxn_set.insert(new_txn.txn_id());
                    for (std::map<uint32, PB::MessageProto>::iterator iter = batch_replies.begin();
                        iter != batch_replies.end(); ++iter)
                    {
                        iter->second.mutable_batch_txns()->add_txns()->CopyFrom(new_txn);
                    }      
                }
            }
        }
        
        // validate out-region txns
        for (auto &&subtxns : outregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                PB::Txn new_txn(subtxn);
                bool validate_res = Validate(subtxn, epoch);
                // dont need to reply out-region nodes with read results
                if (!validate_res)
                {
                    new_txn.set_status(PB::ABORT);
                    abort_subtxn_set.insert(new_txn.txn_id());
                    for (std::map<uint32, PB::MessageProto>::iterator iter = batch_replies.begin();
                        iter != batch_replies.end(); ++iter)
                    {
                        iter->second.mutable_batch_txns()->add_txns()->CopyFrom(new_txn);
                    } 
                }
            }
        }
        
        // wait for complete of lastest epoch
        {
            std::unique_lock<std::mutex> lk(cv_mutex_);
            cv_.wait(lk, [this, epoch]{return this->epoch_manager_->GetCommittedEpoch() == epoch-1; });
            lk.unlock();
        }

        // validate read-set
        for (auto &&subtxn : *pass_local_subtxns)
        {
            if (!ValidateReadSet(subtxn))
            {
                PB::Txn new_txn(subtxn);
                new_txn.set_status(PB::ABORT);
                abort_subtxn_set.insert(new_txn.txn_id());
                for (std::map<uint32, PB::MessageProto>::iterator iter = batch_replies.begin();
                    iter != batch_replies.end(); ++iter)
                {
                    iter->second.mutable_batch_txns()->add_txns()->CopyFrom(new_txn);
                }
            }
        }
        delete pass_local_subtxns;

        // send replies messages to all in-region servers
        int sent_cnt = 0;
        for (std::map<uint32, PB::MessageProto>::iterator iter = batch_replies.begin();
            iter != batch_replies.end(); ++iter)
        {
            iter->second.set_debug_info(std::to_string(epoch));
            conn_->Send(iter->second);
            sent_cnt++;
        }
        
        LOG(INFO) << "epoch : " << epoch << " have sent " << sent_cnt << " Merge() msgs and barrier";
        int recv_msg_cnt = 0;
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
            else
            {
                usleep(100);
            }
        }
        delete reply_msg;
        LOG(INFO) << "epoch : " << epoch << " Merge() barrier end"; 
        // union abort set
        for (size_t i = 0; i < recv_replies.size(); i++)
        {
            for (auto &&subtxn : recv_replies[i].batch_txns().txns())
            {
                if (subtxn.status() == PB::TxnStatus::COMMIT)
                {
                    // returned read result
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
        return committable_subtxns;
    }

    bool Server::CheckAtomic(const PB::Txn& txn, bool committed)
    {
        int total_write_cnt = 0, success_write_cnt = 0;
        for (auto &&stat : txn.commands())
        {
            if (stat.type() == PB::OpType::PUT)
            {
                total_write_cnt ++;
                std::string query_val =  storage_->get(stat.key());
                if (query_val == stat.value())
                {
                    success_write_cnt ++;
                }
            }
        }
        if (committed)
            return total_write_cnt == success_write_cnt ? true : false;
        else
            return success_write_cnt ? false : true;
    }
    void Server::PrintStatistic(uint32 epoch)
    {
        std::string filename = "./report." + UInt32ToString(local_server_id_) + "." + UInt32ToString(epoch);
        std::ofstream file(filename);
        std::string report;
        uint64 cur_lantency = 0;
        uint32 cur_txn_cnt = 0;
        
        for (size_t i = 0; i < local_txns_[epoch].size(); i++)
        {
            if (local_txns_[epoch][i].status() != PB::TxnStatus::COMMIT)
            {
                continue;
            }
            
            uint64 single_latency = local_txns_[epoch][i].end_ts() - local_txns_[epoch][i].start_ts();
            cur_lantency += single_latency;
            cur_txn_cnt ++;
        }
        {
            std::lock_guard<std::mutex> lock(cnt_mutex_);
            done_txn_cnt_ += cur_txn_cnt;
            done_total_latency_ += cur_lantency;
            // txns per second
            report.append("avg_throught : " + UInt64ToString(done_txn_cnt_ * 1000L / (GetTime() - launch_ts_)) + "\n");
            report.append("avg_lantency : " + UInt64ToString(done_total_latency_ / done_txn_cnt_) + "\n");
        }

        file << report;
    }
    // worker
    void Server::Work(uint64 epoch)
    {
        std::vector<std::pair<uint64, uint64>> latencies;
        std::vector<PB::MessageProto> *inregion_subtxns, *outregion_subtxns;
        std::vector<PB::Txn> *committable_subtxns;
        std::vector<PB::Txn> initial_txns(local_txns_[epoch]);
        // process distribute & collect all in-region subtxns
        LOG(INFO) << "epoch : " << epoch << " Start Distribute";
        inregion_subtxns = Distribute(initial_txns, epoch);
        LOG(INFO) << "epoch : " << epoch << " Distribute Finish";
        // process replicate & collect all out-region subtxns
        LOG(INFO) << "epoch : " << epoch << " Start Replicate";
        // Exec Read instantly subtxn in shard node
        for (size_t i = 0; i < inregion_subtxns->size(); i++)
        {
            for (size_t j = 0; j < inregion_subtxns->at(i).batch_txns().txns_size(); j++)
            {
                PB::Txn* subtxn = inregion_subtxns->at(i).mutable_batch_txns()->mutable_txns(j);
                subtxn->set_status(PB::TxnStatus::EXEC);
                ExecRead(subtxn);
            }
        }
        // replicate subtxn and share 
        outregion_subtxns = Replicate(*inregion_subtxns, epoch);
        LOG(INFO) << "epoch : " << epoch << " Replicate Finish";
        // determinstic process merge
        // return value : kvs all will write in db 
        LOG(INFO) << "epoch : " << epoch << " Start Merge";
        committable_subtxns = Merge(*inregion_subtxns, *outregion_subtxns, epoch);
        LOG(INFO) << "epoch : " << epoch << " Merge Finish";
        // atomic batch write in
        for (size_t i = 0; i < local_txns_[epoch].size(); i++)
            local_txns_[epoch][i].set_end_ts(GetTime());
        
        PrintStatistic(epoch);
        std::set<uint64> committed_tid_set;
        for (size_t i = 0; i < committable_subtxns->size(); i++)
        {
            const PB::Txn& txn = committable_subtxns->at(i);
            committed_tid_set.insert(txn.txn_id());
        }
        storage_->LockWrite();
        BatchWrite(committable_subtxns);
        // Check Correctness
        #ifdef CHECK_ATOMIC
        bool atomic_test = true;
        for (auto &&subtxns : *inregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                bool part_res = CheckAtomic(subtxn, committed_tid_set.count(subtxn.txn_id()));
                atomic_test &= part_res;
                if (!part_res && committed_tid_set.count(subtxn.txn_id()))
                    LOG(ERROR) << "epoch : " << epoch << " committed but can't get again";
                else if(!part_res)
                    LOG(ERROR) << "epoch : " << epoch << " abort but can still get" << committed_tid_set.count(subtxn.txn_id());
            }
        }
        if (!atomic_test)
            LOG(ERROR) << "epoch : " << epoch << " inregion check failed";  
        for (auto &&subtxns : *outregion_subtxns)
        {
            for (auto &&subtxn : subtxns.batch_txns().txns())
            {
                bool part_res = CheckAtomic(subtxn, committed_tid_set.count(subtxn.txn_id()));
                atomic_test &= part_res;
            }
        }
        if (!atomic_test)
            LOG(ERROR) << "epoch : " << epoch << " cant pass the subtxn's atomic test";   
        #endif
        storage_->UnlockWrite();

        epoch_manager_->AddCommittedEpoch();
        cv_.notify_all();
        
        delete inregion_subtxns, outregion_subtxns, committable_subtxns;
    }

    void Server::WorkFullReplica(uint64 epoch)
    {
        // exec
        for (size_t i = 0; i < local_txns_[epoch].size(); i++)
        {
            PB::Txn *txn = &local_txns_[epoch][i];
            txn->set_status(PB::TxnStatus::EXEC);
            ExecRead(txn);
        }
        usleep(1000);
        // validate read-set
        std::vector<PB::Txn> *pass_rv_txns = new std::vector<PB::Txn>();
        for (size_t i = 0; i < local_txns_[epoch].size(); i++)
        {
            PB::Txn *txn = &local_txns_[epoch][i];
            if (!ValidateReadSet(*txn))
            {
                txn->set_status(PB::TxnStatus::ABORT);
            }
            else
            {
                pass_rv_txns->push_back(*txn);
            }
        }

        for (auto &&txn : *pass_rv_txns)
        {
            WriteIntent(txn, epoch);
        }
    
        std::string channel = "Replica_" + std::to_string(epoch);
        conn_->NewChannel(channel);
        PB::MessageProto* send_msg_ptr = new PB::MessageProto();
        for (auto && txn : *pass_rv_txns)
        {
            send_msg_ptr->mutable_batch_txns()->add_txns()->CopyFrom(txn);
        }
        int sent_msg = 0;
        // send the whole subtxns in region to other replica's counterpart
        for (std::map<uint32, Node*>::const_iterator iter = config_->all_nodes_.begin(); iter != config_->all_nodes_.end(); iter++)
        {
            uint32 remote_server_id = iter->first;
            if (remote_server_id != local_server_id_)
            {
                // broadcast to all other peer node
                PB::MessageProto mp(*send_msg_ptr);
                mp.set_src_node_id(local_server_id_);
                mp.set_dest_node_id(remote_server_id);
                mp.set_dest_channel(channel);
                mp.set_type(PB::MessageProto_MessageType_BATCHTXNS);
                conn_->Send(mp);
                sent_msg ++;
            }
        }
        delete send_msg_ptr;
        
        LOG(INFO) << "epoch : " << epoch << " Replicate() have sent " << sent_msg << " msgs and barrier";
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
            else
            {
                usleep(100);
            }
        }
        LOG(INFO) << "epoch : " << epoch << " Replicate() barrier end"; 
        conn_->DeleteChannel(channel);
       
        for (auto &&txns : *outregion_subtxns)
        {
            for (auto &&txn : txns.batch_txns().txns())
            {
                WriteIntent(txn, epoch);
            }
        }

        // validate
        for (size_t i = 0; i < local_txns_[epoch].size(); i++)
        {
            if(local_txns_[epoch][i].status() == PB::TxnStatus::ABORT)
                continue; 
            if (!Validate(local_txns_[epoch][i], epoch))
            {
                local_txns_[epoch][i].set_status(PB::TxnStatus::ABORT);
            }
            else
            {
                local_txns_[epoch][i].set_status(PB::TxnStatus::COMMIT);
            }
        }
        std::set<uint64> abort_subtxn_set; 
        for (auto &&txns : *outregion_subtxns)
        {
            for (auto &&txn : txns.batch_txns().txns())
            {
                if (!Validate(txn, epoch))
                {
                    abort_subtxn_set.insert(txn.txn_id());
                }
            }
        }

        for (size_t i = 0; i < local_txns_[epoch].size(); i++)
            local_txns_[epoch][i].set_end_ts(GetTime());
        PrintStatistic(epoch);
        
        std::vector<PB::Txn> *committed_txns = new std::vector<PB::Txn>;
        for (auto &&txn : local_txns_[epoch])
        {
            if (txn.status() == PB::TxnStatus::COMMIT)
            {
                committed_txns->push_back(txn);
            }
        }
        for (auto &&txns : *outregion_subtxns)
        {
            for (auto &&txn : txns.batch_txns().txns())
            {
                if (!abort_subtxn_set.count(txn.txn_id()))
                {
                    committed_txns->push_back(txn);
                }
            }
        }
        
        // wait for complete of lastest epoch
        {
            std::unique_lock<std::mutex> lk(cv_mutex_);
            cv_.wait(lk, [this, epoch]{return this->epoch_manager_->GetCommittedEpoch() == epoch-1; });
            lk.unlock();
        }

        storage_->LockWrite();
        BatchWrite(committed_txns);
        storage_->UnlockWrite();
        epoch_manager_->AddCommittedEpoch();
        cv_.notify_all();
        delete outregion_subtxns, committed_txns;
    }

    void Server::Join()
    {
        worker_.join();
    }
} // namespace taas



