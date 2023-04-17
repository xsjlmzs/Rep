#ifndef STORAGE_H
#define STORAGE_H

#include <map>
#include "common.h"
class Storage
{
private:
    std::map<std::string, std::string>* kvs_;
    std::mutex write_mtx_;
    std::mutex read_mtx_;
public:
    Storage(/* args */);
    ~Storage();
    void put(const std::string& key, const std::string& value);
    void batch_put(const std::vector<std::pair<std::string, std::string>>& kvs);
    std::string get(const std::string& key);
};



#endif