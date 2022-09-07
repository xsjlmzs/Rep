# ifndef CLIENT_H
# define CLIENT_H

#include <string>
#include <vector>

#include <zmqpp/zmqpp.hpp>

#include "common.h"

const std::string default_path = "./";

class Client
{
private:
    std::vector<Node> servers;
    zmqpp::context context;
public:
    Client(/* args */);
    ~Client();

    void Run();

    void LoadConfig(std::string filename, std::string path = default_path);

    void SendRawCmd(const std::vector<std::vector<std::string>>& commands);

    void SendPBTxn(const message::Txn txn);
};


#endif
