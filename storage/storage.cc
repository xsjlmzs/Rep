
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

void Storage::LockWrite()
{
    write_mtx_.lock();
    read_mtx_.lock();
}
void Storage::UnlockWrite()
{
    read_mtx_.unlock();
    write_mtx_.unlock();
}
void Storage::LockRead()
{
    read_mtx_.lock();
}
void Storage::UnlockRead()
{
    read_mtx_.unlock();
}