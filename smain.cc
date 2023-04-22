#include <getopt.h>

#include "server.h"

// global var
int node_id, warerhouse = 10, percent_mp = 10;
std::string config_path = "../conf/server_ip.conf";

std::string instruction[]{"INVALID", "GET", "PUT", "DELETE"};

int main(int argc, char *argv[])
{
    FLAGS_log_dir = "./";
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
    std::unique_ptr<Configuration> config(new Configuration(node_id, config_path));
    std::unique_ptr<Connection> conn(new Connection(config.get()));
    std::unique_ptr<Client> client(new Client(config.get(), percent_mp, warerhouse));

    Spin(1);

    std::unique_ptr<taas::Server> server(new taas::Server(config.get(), conn.get(), client.get()));
    server->Join();     

    google::ShutdownGoogleLogging();
    return 0;
}
