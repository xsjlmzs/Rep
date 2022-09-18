#ifndef SERVER_H
#define SERVER_H

#include <vector>
#include <string>

#include "common.h"
#include "epoch.h"
#include "storage.h"
#include "txn.h"

namespace taas
{
    class Server
    {
    private:
        // list of servers
        Configuration* config_;
        Connection* conn_;
        EpochManager* epoch_manager_;
        Storage* storage_;

        uint64_t GenerateTid();
        void Execute(Txn* txn, PB::Reply* reply);
    public:
        Server(Configuration* config, Connection* conn);
        ~Server();
        void Run();
    };
} // namespace tass



#endif