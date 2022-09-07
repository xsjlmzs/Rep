#include <iostream>

#include <zmqpp/zmqpp.hpp>

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
        std::string text;
        req >> text;

        std::cout << text << std::endl;

        zmqpp::message resp(std::string("successful"));
        socket.send(resp);

        // std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    return 0;
}
