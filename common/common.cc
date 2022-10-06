#include <sstream>
#include <thread>


#include "common.h"

std::vector<std::string> split(const std::string &str, char delim) {
        std::stringstream ss(str);
        std::string item;
        std::vector<std::string> elems;
        while (std::getline(ss, item, delim)) {
            elems.push_back(item);
        }
        return elems;
}

PB::Command PackCommand(const std::string& op, const std::string& key, const std::string& value) {
    PB::OpType op_type = GetOpType(op);

    PB::Command command;
    command.set_key(key);
    command.set_value(value);
    
    return command;
}

PB::OpType GetOpType(const std::string& op) {
    PB::OpType op_type = PB::INVALID;
    
    if (op == "GET") {
        op_type = PB::GET;
    }
    else if (op == "PUT") {
        op_type = PB::PUT;
    }
    else if (op == "DELETE") {
        op_type = PB::DELETE;
    }
    
    return op_type;
}

bool OpenFile(const std::string& filename, std::ifstream& file) {
    XLOGI("the designated file is %s\n",filename.c_str());
    file.open(filename);
    if (file.fail()) {
        XLOGE("error when open the designated file\n");
        return false;
    }
    return true;
}

Configuration::Configuration(int node_id, const std::string filename)
    :node_id_(node_id) {
  ReadFromFile(filename);  
}

Configuration::~Configuration() {
}


int Configuration::ReadFromFile(const std::string& filename)
{
    std::ifstream file;
    if (!OpenFile(filename, file)) {
        XLOGE("Configuration open file error\n");
        return -1;
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0]=='#') {
            continue;
        }
        
        char buf[128];
        Node *node = new Node();
        sscanf(line.c_str(), "node%d=%s", &node->node_id, buf);
        
        char* tok;
        node->replica_id = atoi(strtok_r(buf, ":", &tok));
        node->host       =      strtok_r(NULL, ":", &tok);
        node->port       = atoi(strtok_r(NULL, ":", &tok));

        all_nodes_[node_id_] = node;
        node->Print();
    }
    return 0;
}

Connection::Connection(Configuration* config) : config_(config), cxt_(), deconstructor_invoked_(false) {
    remote_port_ = config_->all_nodes_[config_->node_id_]->port;
    remote_in_ = new zmqpp::socket(cxt_, zmqpp::socket_type::pull);
    std::string remote_endpoint = "tcp://*:" + std::to_string(remote_port_);
    remote_in_->bind(remote_endpoint);

    // port listen for client request 
    client_port_ = 9999;
    client_resp_ = new zmqpp::socket(cxt_, zmqpp::socket_type::reply);
    std::string client_endpoint = "tcp://*:" + std::to_string(client_port_);
    client_resp_->bind(client_endpoint);
    // build socket
    send_mutex_ = new std::mutex[config_->all_nodes_.size()];
    for (std::map<int, Node*>::const_iterator it = config->all_nodes_.begin();
         it != config->all_nodes_.end(); it++) {
        if (config->node_id_ != it->second->node_id) {
            remote_out_[it->second->node_id] = new zmqpp::socket(cxt_,zmqpp::socket_type::push);
            std::string endpoint = "tcp://" + it->second->host + std::to_string(it->second->port); 
            remote_out_[it->second->node_id]->connect(endpoint); 
        }
    }

    // start listen thread main
    std::thread listen_thread(&Connection::ListenClientThread, this);
    listen_thread.detach();
    listen_thread_id_ = listen_thread.get_id();
}

Connection::~Connection() {
    deconstructor_invoked_ = true;

    remote_in_->close();
    delete remote_in_;

    for (std::map<int, zmqpp::socket*>::iterator it = remote_out_.begin();
         it != remote_out_.end(); ++it) {
        it->second->close();
        delete it->second;
    }
}

PB::ClientRequest ZmqMsgToClientRequest(zmqpp::message& msg)
{
    std::string str_msg;
    msg >> str_msg;
    PB::ClientRequest cr;
    cr.ParseFromString(str_msg);
    return cr;
}

void Connection::ListenClientThread() {
    while (!deconstructor_invoked_) {
        zmqpp::message msg;
        if (client_resp_->receive(msg, false)) {
            PB::ClientRequest cr = ZmqMsgToClientRequest(msg);
            XLOGI("Connection has received a client request\n");
            client_reqs_.push(cr);
            while (true)
            {
                if(!replies_.empty())
                {
                    PB::ClientReply reply = replies_.front();
                    // std::cout << "query res =  " << reply.query_set().size() << std::endl;
                    replies_.pop();
                    std::string str_reply;
                    reply.SerializeToString(&str_reply);
                    zmqpp::message msg(str_reply);
                    client_resp_->send(msg);
                    break;
                }
            }
        }
    }
}

void Connection::ListenRemoteThread() {
    while (!deconstructor_invoked_) {
        zmqpp::message msg;
        if (remote_in_->receive(msg, false)) {

        }
    }
}

