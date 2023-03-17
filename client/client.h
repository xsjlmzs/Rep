# ifndef CLIENT_H
# define CLIENT_H

#include <string>
#include <vector>

#include <zmqpp/zmqpp.hpp>

#include "common.h"
class Client
{
private:
    std::vector<Node> servers_;
    zmqpp::context cxt_;
    zmqpp::socket* client_socket_;
public:
    Client();
    ~Client();

    void Run();

    void SendClientRequest(const PB::Txn& txn);

    void LoadConfig(std::string filename);

    void GetTxn(PB::Txn** txn);

    uint32 key_range_begin_;
    uint32 key_range_end_;
    uint16 txn_max_size_;
};


#endif
