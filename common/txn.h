#ifndef TXN_H
#define TXN_H

#include "message.pb.h"

namespace taas
{
    enum TxnStatus 
    {
        kPending = 0,
        kAbort = 1,
        kCommit =2,
    };
    class Txn
    {
    private:
        uint64_t tid_;
        int start_epoch_no_;
        int commit_epoch_no;
    public:
        Txn(uint64_t tid, int start_epoch_no, const PB::Txn& txn);
        ~Txn();
        void set_tid(int tid);
        int get_tid();
        void set_start_epoch_no(int start_epoch_no);
        int get_epoch_tid();

        TxnStatus status_;
        PB::Txn pb_txn_;
    };
} // namespace taas


#endif