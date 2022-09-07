
#include <random>

#include "client.h"
#include "message.pb.h"

Client::Client(/* args */)
{
}

Client::~Client()
{
}

message::Txn CmdToTxn(const std::vector<std::vector<std::string>>& cmds)
{
    message::Txn txn;
    for (const std::vector<std::string> &cmd : cmds)
    {
        message::Command* msg_cmd = txn.add_commands();
        if (cmd[0] == "GET")
        {
            msg_cmd->set_type(message::OpType::GET);
            msg_cmd->set_key(cmd[1]);
        }
        else if (cmd[0] == "PUT")
        {
            msg_cmd->set_type(message::OpType::PUT);
            msg_cmd->set_key(cmd[1]);
            msg_cmd->set_value(cmd[2]);
        }
        else
        {
            XLOGE("unsupport operation\n");
            continue;
        }
    }
    return txn;
}

void Client::Run() 
{
    std::vector<std::vector<std::string>> commands; 
    bool in_txn = false;
    while (true)
    {
        std::cout << "command>> ";

        std::string input;
        std::getline(std::cin, input);

        std::vector<std::string>&& command = split(input, ' ');
        
        if (command.empty())
        {
            XLOGE("commands is empty\n");
            continue;
        }

        if (command[0] == "GET")
        {
            std::string key = command[1];
            XLOGI("receive GET request, key is %s\n", key.c_str());
            commands.push_back(command);
        }
        else if (command[0] == "PUT")
        {
            std::string key = command[1];
            std::string value = command[2];
            XLOGI("receive PUT request, key is %s, value is %s\n", key.c_str(), value.c_str());
            commands.push_back(command);
        }
        else if (command[0] == "BEGIN")
        {
            XLOGI("receive BEGIN request, transaction begin\n");
            in_txn = true;
        }
        else if (command[0] == "END")
        {
            in_txn = false;
        }
        else if (command[0] == "DELETE")
        {
            //TODO :
            continue;
        }
        else if (command[0] == "EXIT")
        {
            break;
        }
        else
        {
            XLOGE("unsupport operation\n");
            continue;
        }

        if (!in_txn)
        {
            message::Txn &&txn = CmdToTxn(commands);
            SendPBTxn(txn);
            commands.clear();
        }
    }
    
}

void Client::LoadConfig(std::string filename ,std::string path)
{

    std::ifstream address;
    if (!OpenFile(filename, path, address))
    {
        XLOGE("load server config error\n");
        return ;
    }
    
    std::string ip_line;
    while (std::getline(address, ip_line))
    {
        std::vector<std::string> ip_port = split(ip_line, ':');
        if (ip_port.size() != 2)
        {
            XLOGE("error ip format");
        }
        else
        {
            std::string ip = ip_port[0]; int port = std::stoi(ip_port[1]);
            XLOGI("receive server ip : %s:%d\n", ip.c_str(), port);
            servers.push_back(Node{ip, port});
        }
    }
}

void Client::SendRawCmd(const std::vector<std::vector<std::string>>& commands)
{
    
}

void Client::SendPBTxn(const message::Txn txn)
{
    std::string str_txn;
    txn.SerializeToString(&str_txn);

    zmqpp::socket_type type = zmqpp::socket_type::req;
    zmqpp::socket socket(context, type);

    zmqpp::endpoint_t target_endpoint = servers[random()%servers.size()].to_string();

    XLOGI("target server: %s, msg content: %s\n",target_endpoint.c_str(), str_txn.c_str());

    socket.connect("tcp://" + target_endpoint);

    zmqpp::message req;
    req << str_txn;
    socket.send(req);
    
    // receive response
    zmqpp::message resp;
    socket.receive(resp);
    std::string resp_content;
    resp >> resp_content;
    std::cout << resp_content << std::endl;
    socket.close();
    
}