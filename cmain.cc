#include <iostream>

#include "client.h"

int main(int argc, char const *argv[])
{
    Client* client = new Client();
    client->LoadConfig("server_ip.txt", "../conf/");

    client->Run();


    // zmq example
    // const std::string endpoint = "tcp://localhost:10000";

    // zmqpp::context context;

    // // generate a push socket
    // zmqpp::socket_type type = zmqpp::socket_type::req;
    // zmqpp::socket socket (context, type);

    // // open the connection
    // std::cout << "Connecting to hello world server…" << std::endl;
    // socket.connect(endpoint);
    // int request_nbr;
    // for (request_nbr = 0; request_nbr != 10; request_nbr++) {
    //     // send a message
    //     std::cout << "Sending Hello " << request_nbr <<"…" << std::endl;
    //     zmqpp::message message;
    //     // compose a message from a string and a number
    //     message << "Hello";
    //     socket.send(message);
    //     std::string buffer;
    //     socket.receive(buffer);
        
    //     std::cout << "Received World " << request_nbr << std::endl;
    // }

    return 0;
}
