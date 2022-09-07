
#include "convert.h"

message::Txn TxnToPBTxn(const Txn* src_txn)
{
    message::Txn dst_txn;
    dst_txn.set_txn_id(src_txn->get_txn_id());
    // TODO
    return dst_txn;
}