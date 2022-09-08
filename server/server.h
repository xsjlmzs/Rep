#ifndef SERVER_H
#define SERVER_H

#include <string>

#include "common.h"

class Server
{
private:
    Node node;

    // 获得client的request
    void RunLoop();
public:
    Server(/* args */);
    ~Server();
};



#endif