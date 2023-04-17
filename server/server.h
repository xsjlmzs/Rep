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
        std::map<std::string, uint64> crdt_map_[kMaxEpoch];
        // local generate txn <epoch-id, tnxs>
        std::map<uint64, std::vector<PB::Txn>> local_txns_;

        uint32_t local_server_id_;

        uint64_t GenerateTid();
        void HeartbeatAllServers();
        void Execute(const Txn& txn, PB::ClientReply* reply);
        void ExecRead(PB::Txn& txn);
        void ExecWrite(const PB::Txn& txn);
        void BatchWrite(const std::vector<PB::Txn>* txns);
        
        void WriteIntent(const PB::Txn& txn, uint64 epoch);
        bool Validate(const PB::Txn& txn, uint64 epoch);

        bool CheckAtomic(const PB::Txn& txn, bool committed);
        void PrintStatistic(uint32 epoch);

        std::thread worker_;
    public:
        Server(Configuration *config, Connection *conn, Client *client);
        ~Server();
        void Run();
        std::vector<PB::MessageProto>* Distribute(const std::vector<PB::Txn>& local_txns, uint64 epoch);
        std::vector<PB::MessageProto>* Replicate(const std::vector<PB::MessageProto>& inregion_subtxns, uint64 epoch);
        std::vector<PB::Txn>* Merge(const std::vector<PB::MessageProto>& all_subtxns, const std::vector<PB::MessageProto>& peer_subtxns, uint64 epoch);

        // worker
        void Work(uint64 epoch);
        void Join();
    };
} // namespace tass



#endif