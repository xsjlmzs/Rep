#include <sstream>

#include "common.h"

std::vector<std::string> split(const std::string &str, char delim)
{
        std::stringstream ss(str);
        std::string item;
        std::vector<std::string> elems;
        while (std::getline(ss, item, delim))
        {
            elems.push_back(item);
        }
        return elems;
}

message::Command PackCommand(std::string op, std::string key, std::string value)
{
    message::OpType op_type = GetOpType(op);

    message::Command command;
    command.set_key(key);
    command.set_value(value);
    
    return command;
}

message::OpType GetOpType(const std::string& op)
{
    message::OpType op_type = message::INVALID;
    
    if (op == "GET")
    {
        op_type = message::GET;
    }
    else if (op == "PUT")
    {
        op_type = message::PUT;
    }
    else if (op == "DELETE")
    {
        op_type = message::DELETE;
    }
    
    return op_type;
}

bool OpenFile(const std::string& filename, const std::string& path, std::ifstream& file)
{
    XLOGI("the designated file is %s\n",(path+filename).c_str());
    file.open(path + filename);
    if (file.fail())
    {
        XLOGE("error when open the designated file\n");
        return false;
    }
    return true;
}