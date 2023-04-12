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
    file.open(filename);
    if (file.fail()) {
        return false;
    }
    return true;
}

Configuration::Configuration(int node_id, const std::string filename)
    :node_id_(node_id) {
  LOG(INFO) << "Configure Construct Start";
  ReadFromFile(filename);
  replica_id_ = all_nodes_[node_id_]->replica_id;
  partition_id_ = all_nodes_[node_id_]->partition_id;
  replica_num_ = all_nodes_.rbegin()->second->replica_id + 1;
  if (all_nodes_.size() % replica_num_ != 0)
  {
    LOG(ERROR) << "replica size error";
  }
  replica_size_ = all_nodes_.size() / replica_num_;
  LOG(INFO) << "replica   id : " << replica_id_   ;
  LOG(INFO) << "partition id : " << partition_id_ ;
  LOG(INFO) << "replica num  : " << replica_num_  ;
  LOG(INFO) << "replica size : " << replica_size_ ;

  LOG(INFO) << "Configure Construct Finish";
}

Configuration::~Configuration() {
}

int Configuration::ReadFromFile(const std::string& filename)
{
    std::ifstream file;
    if (!OpenFile(filename, file)) {
        LOG(ERROR) << "Open File " << filename << "Failed";
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
        node->replica_id   = atoi(strtok_r(buf,  ":", &tok));
        node->partition_id = atoi(strtok_r(NULL, ":", &tok));
        node->host         =      strtok_r(NULL, ":", &tok);
        node->port         = atoi(strtok_r(NULL, ":", &tok));

        all_nodes_[node->node_id] = node;
        // replica_size.find(node->replica_id) == replica_size.end() ? replica_size[node->replica_id] = 1 : replica_size[node->replica_id]++;
        // node_ids[std::pair<int,int>(node->replica_id, node->partition_id)] = node->node_id;
        // node->Print();
    }
    return 0;
}

int Configuration::LookupPartition(const string& key)
{
    return StringToInt(key) % replica_size_;
}

uint32 Configuration::LookupMachineID(int partition_id)
{
    return replica_id_ * replica_size_ + partition_id;
}

// ---------------------------- Class Connection -------------------------------

Connection::Connection(Configuration* config) : config_(config), cxt_(), deconstructor_invoked_(false) {

    LOG(INFO) << "Connection Start Init";
    remote_port_ = config_->all_nodes_[config_->node_id_]->port;
    remote_in_ = new zmqpp::socket(cxt_, zmqpp::socket_type::pull);
    std::string remote_endpoint = "tcp://*:" + std::to_string(remote_port_);
    remote_in_->bind(remote_endpoint);

    // port listen for client request 
    // client_port_ = 9999;
    // client_resp_ = new zmqpp::socket(cxt_, zmqpp::socket_type::reply);
    // std::string client_endpoint = "tcp://*:" + std::to_string(client_port_);
    // client_resp_->bind(client_endpoint);

    // build socket
    send_mutex_ = new std::mutex[config_->all_nodes_.size()];
    for (std::map<uint, Node*>::const_iterator it = config->all_nodes_.begin();
         it != config->all_nodes_.end(); it++) {
        if (config->node_id_ != it->second->node_id) {
            remote_out_[it->second->node_id] = new zmqpp::socket(cxt_,zmqpp::socket_type::push);
            std::string endpoint = "tcp://" + it->second->host + ':' + std::to_string(it->second->port); 
            remote_out_[it->second->node_id]->connect(endpoint); 
        }
    }

    // alloc 
    new_channel_queue_ = new AtomicQueue<std::string>();
    delete_channel_queue_ = new AtomicQueue<std::string>();
    send_message_queue_ = new AtomicQueue<PB::MessageProto>();
    thread_ = std::thread(&Connection::Run, this);
    LOG(INFO) << "Connection Init Complete";
    return ;
}

Connection::~Connection() {

    LOG(INFO) << "Connection Deconstruct Start";
    deconstructor_invoked_ = true;

    thread_.join();

    remote_in_->close();
    delete remote_in_;

    for (std::map<int, zmqpp::socket*>::iterator it = remote_out_.begin();
         it != remote_out_.end(); ++it) {
        it->second->close();
        delete it->second;
    }

    channel_results_.Destroy();
    LOG(INFO) << "Connection Deconstruct Finish";
}

void Connection::NewChannel(std::string channel)
{
    new_channel_queue_->Push(channel);
    while (channel_results_.Count(channel) == 0)
    {
        usleep(100);
    }
}

void Connection::DeleteChannel(std::string channel)
{
    delete_channel_queue_->Push(channel);
    while(channel_results_.Count(channel) != 0)
    {
        usleep(100);
    }
}

void Connection::Run()
{

    std::string new_channel;
    zmqpp::message_t msg;
    while (!deconstructor_invoked_)
    {
        // new channel
        while (new_channel_queue_->Pop(&new_channel))
        {
            if (channel_results_.Count(new_channel) > 0)
            {
                // have existed channel
                continue;
            }
            
            AtomicQueue<PB::MessageProto>* channel_queue = new AtomicQueue<PB::MessageProto>();

            for (std::vector<PB::MessageProto>::iterator iter = undelivered_messages_[new_channel].begin();
                iter < undelivered_messages_[new_channel].end(); ++iter)
            {
                channel_queue->Push(*iter);
            }
            undelivered_messages_.erase(new_channel);
            channel_results_.Put(new_channel, channel_queue);
        }

        // delete channel
        while (delete_channel_queue_->Pop(&new_channel))
        {
            if (channel_results_.Count(new_channel) == 0)
            {
                continue;
            }
            delete channel_results_.Lookup(new_channel);
            channel_results_.Erase(new_channel);
            
        }
        
        // recv msg
        if (remote_in_->receive(msg, true))
        {
            PB::MessageProto mp;
            std::string msg_str;
            msg >> msg_str;
            mp.ParseFromString(msg_str);

            if (channel_results_.Count(mp.dest_channel()) == 0)
            {
                // haven't existed channel
                undelivered_messages_[mp.dest_channel()].push_back(mp);
            }
            else
            {
                channel_results_.Lookup(mp.dest_channel())->Push(mp);
            }
        }

        // send msg
        PB::MessageProto mp;
        if (send_message_queue_->Pop(&mp))
        {
            if (mp.dest_node_id() == config_->node_id_)
            {
                if (channel_results_.Count(mp.dest_channel()) == 0)
                {
                    undelivered_messages_[mp.dest_channel()].push_back(mp);
                }
                else
                {
                    channel_results_.Lookup(mp.dest_channel())->Push(mp);
                }
            }
            else
            {
                std::string mp_str;
                mp.SerializeToString(&mp_str);
                msg << mp_str;
                remote_out_[mp.dest_node_id()]->send(msg, false);
            }
        }
    }
}

bool Connection::GetMessage(const std::string& channel, PB::MessageProto* msg)
{
    if (channel_results_.Count(channel) == 0)
    {
        msg = nullptr;
        return false;
    }
    if (channel_results_.Lookup(channel)->Pop(msg))
    {
        return true;
    }
    else
    {
        return false;
    }
}

void Connection::Send(const PB::MessageProto& msg)
{
    PB::MessageProto mp;
    mp.CopyFrom(msg);
    send_message_queue_->Push(mp);
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


