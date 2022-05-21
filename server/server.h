#ifndef SERVER_H
#define SERVER_H

#include <string>

class Server
{
private:
    /* data */
    std::string host;
    
    unsigned int port;

    // 获得client的request
    

    void RunLoop()
public:
    Server(/* args */);
    ~Server();
};



#endif