#ifndef TXN_H
#define TXN_H

#include "message.pb.h"

class Txn
{
private:
    unsigned long long txn_id_;

    std::vector<message::Command> commands_;
    
public:
    std::vector<std::string> read_set;
    std::vector<std::string> write_set;

    void set_txn_id(unsigned long long txn_id);
    unsigned long long get_txn_id() const;

    void AddCommand(message::Command cmd);
    Txn(/* args */);
    ~Txn();
};


#endif