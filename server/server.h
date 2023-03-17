#ifndef SERVER_H
#define SERVER_H

#include <vector>
#include <string>

#include "common.h"
#include "epoch.h"
#include "storage.h"
#include "txn.h"
#include "client.h"

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

        bool deconstructor_invoked_;
        // for local merge <key, tid>
        std::map<std::string, uint64> crdt_map_;
        // local commit txn
        AtomicQueue<PB::Txn> local_txns;

        uint64_t GenerateTid();
        void HeartbeatAllServers();
        void Execute(const Txn& txn, PB::ClientReply* reply);

        void WriteIntent(const PB::Txn& txn);
        bool Validate(const PB::Txn& txn);

        std::thread pack_thread_;
        std::thread merge_thread_;
    public:
        Server(Configuration* config, Connection* conn);
        ~Server();
        void Run();
        void DistributeAndHold();
        void Merge();
    };
} // namespace tass



#endif