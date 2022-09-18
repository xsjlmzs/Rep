#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <vector>
#include <queue>
#include <fstream>
#include <zmqpp/zmqpp.hpp>

#include "message.pb.h"

#define XLOGE printf
#define XLOGI printf

struct Node
{
    int node_id;
    int replica_id;
    int partition_id; // not used

    std::string host;
    int port;

    void Print()
    {
        std::cout << "node_id = " << node_id << '\n'
                  << "replica_id = " << replica_id << '\n'
                  << "partition_id = " << partition_id << '\n'
                  << "host = " << host << '\n'
                  << "port = " << port << std::endl;
    }

    std::string GetSocket()
    {
        return host + ':' + std::to_string(port);
    }
};

class Configuration
{
private:
    /* data */
    int ReadFromFile(const std::string& name);
public:
    int node_id_;

    std::map<int, Node*> all_nodes_;
    Configuration(int node_id, const std::string filename);
    ~Configuration();
};

class Connection
{
private:
    Configuration* config_;

    zmqpp::context cxt_;
    int remote_port_; // listen in
    zmqpp::socket* remote_in_; // remote node in
    std::map<int, zmqpp::socket*> remote_out_;
    std::mutex* send_mutex_;

    int client_port_;
    zmqpp::socket* client_resp_;

    bool deconstructor_invoked_;

    std::thread::id listen_thread_id_;
public:
    Connection(Configuration* config);
    ~Connection();

    // void Run();
    bool GetMessage(PB::MessageProto* m); // non-blocking
    void ListenClientThread();
    void ListenRemoteThread();

    std::queue<PB::MessageProto> client_reqs_;
    std::queue<PB::MessageProto> merge_reqs_;
    std::queue<PB::Reply> replies_;
};

std::vector<std::string> split(const std::string&, char);
PB::Command PackCommand(std::string, std::string, std::string);
PB::OpType GetOpType(const std::string&);
bool OpenFile(const std::string&, std::ifstream&);



#endif