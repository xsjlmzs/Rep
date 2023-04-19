#include <getopt.h>

#include "server.h"

// global var
int node_id, warerhouse = 10, percent_mp = 10;
std::string config_path = "../conf/server_ip.conf";

std::string instruction[]{"INVALID", "GET", "PUT", "DELETE"};

int main(int argc, char *argv[])
{
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::GLOG_INFO);
    using google::INFO;
    using google::ERROR;
    using google::WARNING;
    using google::FATAL;

    // parse command line 
    while (true)
    {
        int option_index = 0;
        static struct option long_options[] = 
        {
            {"node_id",     required_argument, nullptr,    'n'},
            {"warehouse",   optional_argument, nullptr,    'w'},
            {"percent_mp",  optional_argument, nullptr,    'm'},
            {"config_path", optional_argument, nullptr,    'p'},
            { nullptr,      0,                 nullptr,     0 }
        };

        int c = getopt_long(argc, argv, "n:w::p::m::", long_options, &option_index);
        if (c == -1)
        {
            break;
        }
        
        switch (c)
        {
        case 'p':
            config_path = optarg;
            break;
        case 'm':
            percent_mp = std::stoi(optarg);
            break;
        case 'n':
            node_id = std::stoi(optarg);
            break;
        case 'w':
            warerhouse = std::stoi(optarg);
            break;
        case  0 :
            if (long_options[option_index].flag != nullptr)
                break;
            if (optarg)
                LOG(INFO) << "with arg " << optarg;
            break;
        default:
            break;
        }
    }
    
    LOG(INFO) << "node : " << node_id;
    LOG(INFO) << "warehouse : " << warerhouse;
    LOG(INFO) << "percent_mp : " << percent_mp;
    LOG(INFO) << "config_path : " << config_path;
    Configuration *config = new Configuration(node_id, config_path);
    Connection *conn = new Connection(config);
    Client *client = new Client(config, percent_mp, warerhouse);

    Spin(1);

    taas::Server* server = new taas::Server(config, conn, client);
    server->Join();     

    google::ShutdownGoogleLogging();
    delete config, conn, client;
    return 0;
}
