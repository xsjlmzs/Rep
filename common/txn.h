#ifndef TXN_H
#define TXN_H

#include "message.pb.h"

namespace taas
{
    class Txn
    {
    private:
        uint64_t tid_;
        int epoch_no_;
        PB::MessageProto  mp_;
    public:
        Txn(PB::MessageProto mp);
        ~Txn();
        void set_tid(int tid);
        int get_tid();
        void set_epoch_no(int epoch_no);
        int get_epoch_tid();
        void set_mp(PB::MessageProto mp);
        PB::MessageProto get_mp();
    };
} // namespace taas


#endif