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

    void SendRawCmd(const std::vector<std::vector<std::string>>& commands);

    void SendClientRequest(const PB::Txn& txn);

    void LoadConfig(std::string filename);
};


#endif
