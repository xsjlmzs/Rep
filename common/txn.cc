#include "txn.h"

Txn::Txn(/* args */)
{
}

Txn::~Txn()
{
}

void Txn::set_txn_id(unsigned long long txn_id)
{
    txn_id_ = txn_id;
}

unsigned long long Txn::get_txn_id() const
{
    return txn_id_;
}

void Txn::AddCommand(message::Command cmd)
{
    commands_.push_back(cmd);
}