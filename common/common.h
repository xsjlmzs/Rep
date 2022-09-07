#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <vector>
#include <fstream>

#include "message.pb.h"

#define XLOGE printf
#define XLOGI printf

std::vector<std::string> split(const std::string&, char);
message::Command PackCommand(std::string, std::string, std::string);
message::OpType GetOpType(const std::string&);
bool OpenFile(const std::string&, const std::string&, std::ifstream&);

struct Node
{
    std::string ip;
    int port;

    std::string to_string()
    {
        return ip+':'+std::to_string(port);
    }
};




#endif