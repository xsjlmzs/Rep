#ifndef TPCC_H
#define TPCC_h

#include <set>

#include "common.h"

#define DISTRICTS_PER_WAREHOUSE 10
#define CUSTOMERS_PER_DISTRICT 3000
#define NUMBER_OF_ITEMS 100000

class Tpcc
{
private:
    void GetRandomKeys(std::set<uint64>* keys, uint32 num_keys, uint32 key_start,
                     uint32 key_limit, uint32 part);
public:
    Configuration *config_;
    uint32 warehouses_per_node_;

    uint32 warehouse_end_;
    uint32 district_end_;
    uint32 customer_end_;
    uint32 item_end_;
    
    uint32 DBSize_;
    static const uint32 kRecordSize = 100;

    Tpcc(Configuration* ,uint32);
    PB::Txn* TpccTxnSP(uint64 txn_id, uint32 part);
    PB::Txn* TpccTxnMP(uint64 txn_id, uint32 part1, uint32 part2);

    ~Tpcc();
};

#endif