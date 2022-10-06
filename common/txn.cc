#include "txn.h"

namespace taas
{
    Txn::Txn(uint64_t tid, int start_epoch_no, const PB::Txn& txn)
     : tid_(tid), start_epoch_no_(start_epoch_no), pb_txn_(txn)
    {
        status_ = kPending;
    }   

    Txn::~Txn()
    {

    }

    void Txn::set_tid(int tid)
    {
        tid_ = tid;
    }
    int Txn::get_tid()
    {
        return tid_;
    }

    void Txn::set_start_epoch_no(int start_epoch_no)
    {
        start_epoch_no_ = start_epoch_no;
    }
    int Txn::get_epoch_tid()
    {
        return start_epoch_no_;
    }

} // namespace taas
