
#include "storage.h"
#include <utility>

Storage::Storage(/* args */)
{
    kvs_ = new std::map<std::string, std::string>();
}

Storage::~Storage()
{
}

void Storage::put(const std::string& key, const std::string& value)
{
    std::unique_lock<std::mutex> lock(mtx_);
    (*kvs_)[key] = value;
}

void Storage::batch_put(const std::vector<std::pair<std::string, std::string>>& kvs)
{
    std::unique_lock<std::mutex> lock(mtx_);
    for (auto &&kv : kvs)
    {
        (*kvs_)[kv.first] = kv.second;
    }
}

std::string Storage::get(const std::string& key)
{
    std::unique_lock<std::mutex> lock(mtx_);
    if (kvs_->find(key) == kvs_->end())
    {
        return "";
    }
    return (*kvs_)[key];
}