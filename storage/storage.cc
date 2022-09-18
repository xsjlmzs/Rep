
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
    (*kvs_)[key] = value;
}
std::string Storage::get(const std::string& key)
{
    if (kvs_->find(key) == kvs_->end())
    {
        return "";
    }
    return (*kvs_)[key];
}