#include "benchmarker.h"

string dbName = "/tmp/learnedDB";
int kTotalKeys = 100;

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


int main() {
    DB *db;
    Options options;
    options.write_buffer_size = 4 << 15;
    options.target_file_size_base = 4 << 20;
    // NumericalComparator numerical_comparator;
    // options.comparator = &numerical_comparator;
    CustomComparator custom_comparator;
    options.comparator = &custom_comparator;
    BlockBasedTableOptions block_based_options;
    block_based_options.model = kSimpleLR;
    // block_based_options.seg_cost = 200000;
    options.create_if_missing = true;
    options.compression = kNoCompression;
    // block_based_options.block_align = true;
    block_based_options.block_cache =
      NewLRUCache(static_cast<size_t>(64 * 1024 * 1024));
    options.table_factory.reset(
          NewBlockBasedTableFactory(block_based_options));
    IngestExternalFileOptions ifo;
    rocksdb::Status s = DB::Open(options, dbName, &db);

    bool random_write = true;
    auto written = write(db, 10000, 8, false, !random_write, 100000);
    read(db, 10000, true, 8, false, false, 100000, written, random_write);
    // measure_sizes();

    return 0;
}