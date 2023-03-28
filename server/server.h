#ifndef SERVER_H
#define SERVER_H

#include <vector>
#include <string>

#include "common.h"
#include "epoch.h"
#include "storage.h"
#include "txn.h"
#include "client.h"
#include "thread_pool.h"

namespace taas
{
    const uint16 kMaxEpoch = 1000;
    class Server
    {
    private:
        // list of servers
        Configuration* config_;
        Connection* conn_;
        EpochManager* epoch_manager_;
        Storage* storage_;
        Client* client_;
        ThreadPool* thread_pool_;

        bool deconstructor_invoked_;
        // for local merge <key, tid>
        std::map<std::string, uint64> crdt_map_[100];
        // local generate txn <epoch-id, tnxs>
        std::map<uint64, std::vector<PB::Txn> > local_txns_;
        // in-region all subtxns
        // AtomicQueue<PB::MessageProto> all_subtxns;
        // out-region all subtxns
        // AtomicQueue<PB::MessageProto> peer_subtxns;
        // record aborted txn id



        uint64_t GenerateTid();
        void HeartbeatAllServers();
        void Execute(const Txn& txn, PB::ClientReply* reply);

        void WriteIntent(const PB::Txn& txn, uint64 epoch);
        bool Validate(const PB::Txn& txn, uint64 epoch);

        std::thread pack_thread_;
        std::thread merge_thread_;
    public:
        Server(Configuration* config, Connection* conn);
        ~Server();
        void Run();
        std::vector<PB::MessageProto>* Distribute(const std::vector<PB::Txn>& local_txns, uint64 epoch);
        std::vector<PB::MessageProto>* Replicate(const std::vector<PB::MessageProto>& all_subtxns, uint64 epoch);
        // void DistributeAndHold();
        void Merge(const std::vector<PB::MessageProto>& all_subtxns, const std::vector<PB::MessageProto>& peer_subtxns, uint64 epoch);

        // worker
        void Work(uint64 epoch);
    };
} // namespace tass



#endif