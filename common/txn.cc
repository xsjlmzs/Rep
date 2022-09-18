#include "txn.h"

namespace taas
{
    Txn::Txn(PB::MessageProto mp) : mp_(mp)
    {
        
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

    void Txn::set_epoch_no(int epoch_no)
    {
        epoch_no_ = epoch_no;
    }
    int Txn::get_epoch_tid()
    {
        return epoch_no_;
    }

    void Txn::set_mp(PB::MessageProto mp)
    {
        mp_ = mp;
    }
    PB::MessageProto Txn::get_mp()
    {
        return mp_;
    }

} // namespace taas
