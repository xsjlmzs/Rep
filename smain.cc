#include <iostream>

#include "server.h"

std::string instruction[]{"INVALID", "GET", "PUT", "DELETE"};

int main(int argc, char const *argv[])
{
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::GLOG_INFO);
    using google::INFO;
    using google::ERROR;
    using google::WARNING;
    using google::FATAL;

    if (argc < 2)
    {
        LOG(ERROR) << "Usage: " << argv[0] << "<node-id>" << std::endl;
        return 0;
    }

    
    Configuration *config = new Configuration(std::stoi(argv[1]), "../conf/server_ip.conf");
    Connection *conn = new Connection(config);

    Spin(1);

    taas::Server* server = new taas::Server(config, conn);
    usleep(100000000);

    google::ShutdownGoogleLogging();
    return 0;
}
