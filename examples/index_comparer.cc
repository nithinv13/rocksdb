#include "benchmarker.h"

string dbName = "/tmp/learnedDB";
DB* db;
BlockBasedTableOptions block_based_options;

void write(DB* db, uint64_t num_entries = 1000000, 
int key_size = 8, int value_size = 100, bool pad = true, bool seq = true, int key_range = 1000000) {
    WriteOptions write_options;
    rocksdb::Status s;
    std::vector<std::string> written;
    for (uint64_t i = 0; i < num_entries; i++) {
        if (i % 10000 == 0) {
            cout << "Completed " << std::to_string(i) << " writes" << endl;
        }
        std::string key, value, final_key, final_value;
        if (seq) {
            key =  std::to_string(i);
            value = std::to_string(i);
        }
        else { 
            int rand = std::rand() % key_range;
            key = std::to_string(rand);
            value = key;
        }
        std::string result;
        if (pad) {
            final_key = string(key_size - key.length(), '0') + key;
            final_value = string(value_size - value.length(), '0') + value;
        }
        // written.push_back(final_key);
        s = db->Put(write_options, Slice(final_key), Slice(final_value));
        if (!s.ok()) { 
            printf("Error in writing key %s", key.c_str());
            break;
        }
    }
    // return written;
}

void measure_memory_usage(DB* db, std::ofstream& output_file) {
    std::string out;
    db->GetProperty("rocksdb.estimate-table-readers-mem", &out);
    size_t cache_usage = block_based_options.block_cache->GetUsage();
    size_t pinned_usage = block_based_options.block_cache->GetPinnedUsage();
    output_file << out << "," << cache_usage << "," << pinned_usage << "\n";
}

void read(DB* db, uint64_t num_entries = 1000000, bool use_learning = false, int key_size = 8, int value_size = 100,
 bool pad = true, bool seq = true, int key_range = 1000000, std::vector<std::string> v = {}, bool random_write = false) {
    assert(!random_write || v.size() > 0);
    std::ofstream output_file("/tmp/learnedDB/memory_usage.csv", std::ios_base::app | std::ios_base::out);
    output_file.precision(15);
    output_file << "Table_reader_usage," << "Cache_usage," << "Pinned_usage" << "\n";
    ReadOptions read_options;
    if (use_learning) {
        read_options.learned_get = true;
    } else {
        read_options.learned_get = false;
    }
    rocksdb::Status s;
    uint64_t operation_count = 0;
    uint64_t total_time;
    std::string value;
    uint64_t found = 0;
    for (uint64_t i = 2; i < num_entries; i++) {
        if (i % 500 == 0) {
            cout << "Completed " << std::to_string(i) << " reads" << endl;
            measure_memory_usage(db, output_file);
        }
        std::string key, val, final_key, final_value;
        if (seq) {
            key = std::to_string(i);
            val = std::to_string(i);
        }
        else {
            int rand = std::rand() % key_range;
            key = std::to_string(rand);
            val = key;
        }
        std::string result;
        if (pad) {
            final_key = string(key_size - key.length(), '0') + key;
            final_value = string(value_size - val.length(), '0') + val;
        } 

        if (random_write) result = v[i];
        auto start = high_resolution_clock::now();
        s = db->Get(read_options, Slice(final_key), &value);
        auto stop = high_resolution_clock::now();
        // cout << final_value << " " << value << endl;
        if (value == final_value) {
            found += 1;
        }
        // assert(value == result);
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

int main(int argc, char **argv) {
    // cout << sizeof(1.0) << " " << sizeof(uint32_t) << " " << sizeof(long double) << " " <<
    //  sizeof(std::vector<double>{1.0, 2.0, 3.0}) << endl;
    // return 0;
    cout << argc << endl;
    assert(argc == 4);
    dbName = argv[1];
    int index_type = stoi(argv[2]);
    int num_operations = stoi(argv[3]);
    std::string command = "rm -rf ";
    command = command.append(dbName).append("/*");
    int result = system(command.c_str());

    // rocksdb::DB *db;
    rocksdb::Options options;
    options.write_buffer_size = 4 << 20;
    options.target_file_size_base = 4 << 20;
    options.create_if_missing = true;
    options.compression = kNoCompression;
    // NumericalComparator numerical_comparator;
    // options.comparator = &numerical_comparator;
    CustomComparator custom_comparator;
    options.comparator = &custom_comparator;
    // BlockBasedTableOptions block_based_options;
    MetadataCacheOptions metadata_cache_options;
    // block_based_options.block_align = true;
    // block_based_options.cache_index_and_filter_blocks = true;
    bool learned_get = false;
    switch (index_type) {
        case 1:
            block_based_options.index_type = BlockBasedTableOptions::IndexType::kBinarySearch;
            // Set the below option only if cache_index_and_filter_blocks is true
            // metadata_cache_options.top_level_index_pinning = PinningTier::kAll;
            // block_based_options.pin_l0_filter_and_index_blocks_in_cache = true;
            break;
        case 2:
            block_based_options.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
            // Set the below option only if cache_index_and_filter_blocks is true
            block_based_options.pin_top_level_index_and_filter = true;
            break;
        case 3: 
            block_based_options.index_type = BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
            break;
        case 4:
            block_based_options.model = kGreedyPLR;
            block_based_options.use_learning = true;
            learned_get = true;
            break;
        case 5:
            block_based_options.model = kSimpleLR;
            block_based_options.use_learning = true;
            learned_get = true;
            break;
        case 6:
            block_based_options.model = kStatPLR;
            block_based_options.use_learning = true;
            learned_get = true;
            break;
        default:
            cout << "Option not available" << endl;
    }
    block_based_options.block_cache =  NewLRUCache(static_cast<size_t>(2 * 1024 * 1024));
    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    rocksdb::Status s = DB::Open(options, dbName, &db);

    int write_key_range = num_operations;
    int read_key_range = num_operations;
    write(db, num_operations, 8, 100, true, true, write_key_range);
    // db->Close();
    // DB::Open(options, dbName, &db);
    read(db, num_operations, learned_get, 8, 100, true, false, read_key_range);

    std::string out;
    db->GetProperty("rocksdb.estimate-table-readers-mem", &out);
    size_t cache_usage = block_based_options.block_cache->GetUsage();
    size_t pinned_usage = block_based_options.block_cache->GetPinnedUsage();
    cout << "Table reader memory usage: " << out << endl;
    cout << "Block cache usage: " << cache_usage << endl;
    cout << "Block cache pinned usage: " << pinned_usage << endl;

    // measure_sizes();
    // measure_memory_usage();

    return 0;
}