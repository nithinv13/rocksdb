//
// Created by daiyi on 2020/02/12.
// Levelled counter that can record some integers for each level

#ifndef PROJECT1_COUNTER_H
#define PROJECT1_COUNTER_H

#include "../db/dbformat.h"
#include <vector>

const int kNumLevels = 7;


class Counter {
    friend class CBModel_Learn;
private:
    std::vector<uint64_t> counts;
    std::vector<uint64_t> nums;
public:
    std::string name;

    // Counter() : counts(rocksdb::config::kNumLevels + 1, 0), nums(rocksdb::config::kNumLevels + 1, 0) {};
    Counter() : counts(kNumLevels + 1, 0), nums(kNumLevels + 1, 0) {};
    void Increment(int level, uint64_t n = 1);
    void Reset();
    void Report();
    int Sum();
    int NumSum();
};


#endif //PROJECT1_COUNTER_H
