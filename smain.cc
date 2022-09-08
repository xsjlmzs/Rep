#include <iostream>

#include <zmqpp/zmqpp.hpp>

#include "message.pb.h"

std::string instruction[]{"INVALID", "GET", "PUT", "DELETE"};

int main(int argc, char const *argv[])
{
    std::cout<<"server start"<<std::endl;

    const std::string endpoint = "tcp://127.0.0.1:10000";

    zmqpp::context cxt;

    zmqpp::socket_type type = zmqpp::socket_type::reply;
    zmqpp::socket socket (cxt, type);

    
    socket.bind(endpoint);
    while (1) {
        // receive the message
        zmqpp::message req;
        // decompose the message 
        socket.receive(req);

        message::Txn txn;
        std::string str_txn;
        req >> str_txn;
        txn.ParseFromString(str_txn);

        for (auto &&cmd : txn.commands())
        {
            std::cout << txn.txn_id() << instruction[cmd.type()] << cmd.key() << cmd.value() << std::endl;
        }
        

        zmqpp::message resp(std::string("successful"));
        socket.send(resp);

        // std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    return 0;
}
