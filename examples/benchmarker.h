#include <iostream>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/table.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/comparator.h"

#include <fstream>
#include <dirent.h>
#include <chrono>

using namespace std;
using namespace std::chrono;
using namespace ROCKSDB_NAMESPACE;


uint64_t ExtractInteger(const char* pos, size_t size) {
    char* temp = new char[size + 1];
    memcpy(temp, pos, size);
    temp[size] = '\0';
    uint64_t result = (uint64_t) atoll(temp);
    delete[] temp;
    return result;
}

class NumericalComparator : public Comparator {
public:
    NumericalComparator() = default;
    virtual const char* Name() const {return "adgMod:NumericalComparator";}
    virtual int Compare(const Slice& a, const Slice& b) const {
        uint64_t ia = ExtractInteger(a.data(), a.size());
        uint64_t ib = ExtractInteger(b.data(), b.size());
        if (ia < ib) return -1;
        else if (ia == ib) return 0;
        else return 1;
    }
    virtual void FindShortestSeparator(std::string* start, const Slice& limit) const { return; };
    virtual void FindShortSuccessor(std::string* key) const { return; };
};

class CustomComparator : public Comparator {
public:
    CustomComparator() = default;
    virtual const char* Name() const {return "adgMod:CustomComparator";}
    virtual int Compare(const Slice& a, const Slice& b) const {
        uint64_t ia = (uint64_t)(stoll(a.ToString().substr(0, 8)));
        uint64_t ib = (uint64_t)(stoll(b.ToString().substr(0, 8)));
        if (ia < ib) return -1;
        else if (ia == ib) return 0;
        else return 1;
    }
    virtual void FindShortestSeparator(std::string* start, const Slice& limit) const { return; };
    virtual void FindShortSuccessor(std::string* key) const { return; };
};


std::vector<std::string> write(DB* db, uint64_t num_entries = 1000000, int key_size = 8, bool pad = true, bool seq = true, int key_range = 1000000) {
    WriteOptions write_options;
    rocksdb::Status s;
    std::vector<std::string> written;
    for (uint64_t i = 0; i < num_entries; i++) {
        if (i % 10000 == 0) {
            cout << "Completed " << std::to_string(i) << " writes" << endl;
        }
        std::string key;
        if (seq) key =  std::to_string(i);
        else { 
            int rand = std::rand() % key_range;
            key = std::to_string(rand);
        }
        std::string result;
        if (pad) result = string(key_size - key.length(), '0') + key;
        else result = key;
        written.push_back(result);
        s = db->Put(write_options, Slice(result), Slice(result));
        if (!s.ok()) { 
            printf("Error in writing key %s", key.c_str());
            break;
        }
    }
    return written;
}

void read(DB* db, uint64_t num_entries = 1000000, bool use_learning = false, int key_size = true, bool pad = true, bool seq = true, int key_range = 1000000, std::vector<std::string> v = {}, bool random_write = false) {
    assert(!random_write || v.size() > 0);
    
    ReadOptions read_options;
    if (use_learning) {
        read_options.learned_get = true;
    }
    rocksdb::Status s;
    uint64_t operation_count = 0;
    uint64_t total_time;
    std::string value;
    uint64_t found = 0;
    for (uint64_t i = 2; i < num_entries; i++) {
        if (i % 10000 == 0) {
            cout << "Completed " << std::to_string(i) << " reads" << endl;
        }
        std::string key;
        if (seq) key = std::to_string(i);
        else {
            int rand = std::rand() % key_range;
            key = std::to_string(rand);
        }
        std::string result;
        if (pad) result = string(key_size - key.length(), '0') + key;
        else result = key;

        if (random_write) result = v[i];
        auto start = high_resolution_clock::now();
        s = db->Get(read_options, Slice(result), &value);
        auto stop = high_resolution_clock::now();
        cout << result << " " << value << endl;
        if (value == result) {
            found += 1;
        }
        assert(value == result);
        // if (value == result) {
            // std::cout << "found" << endl;
        // }
        uint64_t duration = static_cast<uint64_t>(duration_cast<microseconds>(stop - start).count());
        total_time += duration;
        operation_count += 1;
    }
    cout << "Total number of read operations: " << operation_count << endl;
    cout << "Keys found: " << found << endl;
    cout << "Total time taken: " << total_time << endl;
    cout << "Throughput: " << operation_count * 1000000 / total_time << endl;
    cout << "Average latency (us/op): " << total_time / operation_count << endl;
}
