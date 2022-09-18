#include <iostream>

#include "server.h"

std::string instruction[]{"INVALID", "GET", "PUT", "DELETE"};

int main(int argc, char const *argv[])
{
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << "<node-id>" << std::endl;
        return 0;
    }
    
    Configuration *config = new Configuration(std::stoi(argv[1]), "../conf/server_ip.conf");
    Connection *conn = new Connection(config);
    taas::Server* server = new taas::Server(config, conn);
    return 0;
}
