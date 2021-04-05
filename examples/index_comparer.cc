#include "benchmarker.h"
#include "rocksdb/filter_policy.h"

string dbName = "/tmp/learnedDB";
DB* db;
BlockBasedTableOptions block_based_options;

std::vector<std::string> write(DB* db, uint64_t num_entries = 1000000, 
int key_size = 8, int value_size = 100, bool pad = true, bool seq = true, int key_range = 1000000) {
    WriteOptions write_options;
    rocksdb::Status s;
    std::vector<std::string> written;
    for (uint64_t i = 0; i < num_entries; i++) {
        // if (i % 50000 == 0) {
        //     cout << "Completed " << std::to_string(i) << " writes" << endl;
        // }
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
        written.push_back(final_key);
        s = db->Put(write_options, Slice(final_key), Slice(final_value));
        if (!s.ok()) { 
            printf("Error in writing key %s", key.c_str());
            break;
        }
    }
    return written;
}

void measure_memory_usage(DB* db, std::ofstream& output_file) {
    std::string out;
    db->GetProperty("rocksdb.estimate-table-readers-mem", &out);
    size_t cache_usage = block_based_options.block_cache->GetUsage();
    size_t pinned_usage = block_based_options.block_cache->GetPinnedUsage();
    // size_t cache_usage = 0;
    // size_t pinned_usage = 0;
    output_file << out << "," << cache_usage << "," << pinned_usage << "\n";
}

std::vector<std::string> read(DB* db, uint64_t num_entries = 1000000, bool use_learning = false, int key_size = 8, int value_size = 100,
 bool pad = true, bool seq = true, int key_range = 1000000, std::vector<std::string> v = {}, bool random_write = false, bool previously_read = false, std::vector<std::string> already_read = {}) {
    assert(!random_write || v.size() > 0);
    std::ofstream output_file(dbName.append("/memory_usage.csv"), std::ios_base::app | std::ios_base::out);
    output_file.precision(15);
    output_file << "Table_reader_usage," << "Cache_usage," << "Pinned_usage" << "\n";
    ReadOptions read_options;
    std::vector<std::string> reading;
    if (use_learning) {
        read_options.learned_get = true;
    } else {
        read_options.learned_get = false;
    }
    rocksdb::Status s;
    uint64_t operation_count = 0;
    uint64_t total_time = 0;
    std::string value;
    uint64_t found = 0;
    for (uint64_t i = 0; i < num_entries; i++) {
        if (i % 50000 == 0) {
            cout << "Completed " << std::to_string(i) << " reads" << endl;
            // measure_memory_usage(db, output_file);
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
        if (random_write) { 
            key = v[i];
            val = key;
        }
        if (pad) {
            final_key = string(key_size - key.length(), '0') + key;
            final_value = string(value_size - val.length(), '0') + val;
        }
        if (previously_read) {
            final_key = already_read[i];
            final_value = final_key;
        }
        reading.push_back(final_key); 

        auto start = high_resolution_clock::now();
        s = db->Get(read_options, Slice(final_key), &value);
        auto stop = high_resolution_clock::now();
        // cout << final_value << " " << value << endl;
        if (value == final_value) {
            found += 1;
        }
        // assert(value == result);
        if (value != final_value) {
            cout << final_key << " : " << final_value << " " << value << endl;
        }
        uint64_t duration = static_cast<uint64_t>(duration_cast<microseconds>(stop - start).count());
        total_time += duration;
        operation_count += 1;
    }
    cout << "Total number of read operations: " << operation_count << endl;
    cout << "Keys found: " << found << endl;
    cout << "Total time taken: " << total_time << endl;
    cout << "Throughput: " << operation_count * 1000000 / total_time << endl;
    cout << "Average latency (us/op): " << total_time / operation_count << endl;

    return reading;
}

void parse_output(std::string& out) {
    std::string delimiter = "\n";
    size_t pos = 0;
    std::string token;
    while ((pos = out.find(delimiter)) != std::string::npos) {
        token = out.substr(0, pos);
        if (token.find("rocksdb.block.cache.data.miss COUNT") != std::string::npos
        || token.find("rocksdb.block.cache.data.hit COUNT") != std::string::npos
        || token.find("rocksdb.block.cache.index.miss COUNT") != std::string::npos
        || token.find("rocksdb.block.cache.index.hit COUNT") != std::string::npos
        || token.find("rocksdb.db.get.micros") != std::string::npos) {
            cout << token << endl;
        }
        out.erase(0, pos + delimiter.length());
    }
}

int main(int argc, char **argv) {
    // cout << sizeof(1.0) << " " << sizeof(uint32_t) << " " << sizeof(long double) << " " <<
    //  sizeof(std::vector<double>{1.0, 2.0, 3.0}) << endl;
    // return 0;
    assert(argc == 6);
    dbName = argv[1];
    int num_operations = stoi(argv[2]);
    int block_cache_size = stoi(argv[3]);
    int table_cache_size = stoi(argv[4]);
    int index_type = stoi(argv[5]);
    std::string command = "rm -rf ";
    command = command.append(dbName).append("/*");
    int result = system(command.c_str());
    // command = "sync; echo 3 > /proc/sys/vm/drop_caches";
    // result = system(command.c_str());
    cout << "Index type: " << index_type << " Block cache size: " << block_cache_size << " Table cache size: " << table_cache_size << endl;

    // rocksdb::DB *db;
    rocksdb::Options options;
    options.statistics = rocksdb::CreateDBStatistics();
    options.write_buffer_size = 4 << 20;
    options.target_file_size_base = 4 << 20;
    options.use_direct_reads = true;
    options.create_if_missing = true;
    // options.compression = kNoCompression;
    options.max_open_files = table_cache_size;
    // NumericalComparator numerical_comparator;
    // options.comparator = &numerical_comparator;
    CustomComparator custom_comparator;
    options.comparator = &custom_comparator;
    // BlockBasedTableOptions block_based_options;

    // // Block sizes will not be padded to 4096 bytes unless this is uncommented
    // block_based_options.block_align = true;
    // block_based_options.cache_index_and_filter_blocks = true;
    
    // // DO NOT ENABLE THIS AND POINTER to BLOCK CACHE at the same time
    block_based_options.no_block_cache = false;
    if (block_cache_size == 0) {
       block_based_options.no_block_cache = true; 
    }
    
    // block_based_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    bool learned_get = false;
    switch (index_type) {
        case 1:
            block_based_options.index_type = BlockBasedTableOptions::IndexType::kBinarySearch;
            // Set the below option only if cache_index_and_filter_blocks is true
            // block_based_options.metadata_cache_options.top_level_index_pinning = PinningTier::kAll;
            // block_based_options.metadata_cache_options.unpartitioned_pinning = PinningTier::kAll;
            // block_based_options.pin_l0_filter_and_index_blocks_in_cache = true;
            break;
        case 2:
            block_based_options.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
            // Set the below option only if cache_index_and_filter_blocks is true
            // block_based_options.pin_top_level_index_and_filter = true;
            // block_based_options.metadata_cache_options.partition_pinning = PinningTier::kAll;
            break;
        case 3: 
            block_based_options.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
            block_based_options.metadata_cache_options.partition_pinning = PinningTier::kAll;
            break;
        // case 4: 
        //     block_based_options.index_type = BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
        //     break;
        case 4:
            block_based_options.model = kGreedyPLR;
            // Learn only the first keys of each block:
            block_based_options.learn_blockwise = true;
            block_based_options.use_learning = true;
            learned_get = true;
            break;
        case 5:
            block_based_options.model = kSimpleLR;
            block_based_options.learn_blockwise = true;
            block_based_options.use_learning = true;
            learned_get = true;
            break;
        case 6:
            block_based_options.model = kStatPLR;
            block_based_options.learn_blockwise = true;
            block_based_options.seg_cost = 100000;
            block_based_options.use_learning = true;
            learned_get = true;
            break;
        default:
            cout << "Option not available" << endl;
    }
    if (block_based_options.no_block_cache == false) {
        block_based_options.block_cache =  NewLRUCache(static_cast<size_t>(block_cache_size), true, 0.2);
        block_based_options.block_cache_compressed =  NewLRUCache(static_cast<size_t>(100*1024*1024), true, 0.2);
    }
    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    rocksdb::Status s = DB::Open(options, dbName, &db);

    // For random writes, multipy num_operations by a constant like 10
    int write_key_range = num_operations;
    int read_key_range = num_operations;
    // For random writes, make seq = false
    // auto written = write(db, num_operations, 8, 100, true, false, write_key_range);
    auto written = write(db, num_operations, 8, 100, true, true, write_key_range);
    // db->Close();
    delete db;
    if (block_based_options.no_block_cache == false) {
        block_based_options.block_cache =  NewLRUCache(static_cast<size_t>(block_cache_size), true, 0.2);
        block_based_options.block_cache_compressed =  NewLRUCache(static_cast<size_t>(100*1024*1024), true, 0.2);
    }
    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    options.statistics = rocksdb::CreateDBStatistics();
    DB::Open(options, dbName, &db);
    // DB::OpenForReadOnly(options, dbName, &db);
    // db->SetDBOptions({{"max_open_files", "6400*1024"}});
    printf("Starting to read now\n");
    // rocksdb::DBImpl* db_impl = dynamic_cast<DBImpl*>(db);
    // if (db_impl != nullptr) {
    //     db_impl->TEST_table_cache()->hit_count_ = 0;
    // }
    // For reads from randomly written data, make random_writes = true
    auto reading = read(db, 50000, learned_get, 8, 100, true, false, read_key_range, written, true);

    std::string out;
    db->GetProperty("rocksdb.options-statistics", &out);
    parse_output(out);

    cout << "Reading second time" << endl;

    // options.statistics = rocksdb::CreateDBStatistics();
    // bool dont_care = false;
    // read(db, 50000, learned_get, 8, 100, true, false, read_key_range, written, dont_care, true, reading);

    db->GetProperty("rocksdb.estimate-table-readers-mem", &out);
    cout << "Table reader memory usage: " << out << endl;

    if (block_based_options.no_block_cache == false) {
        size_t cache_usage = block_based_options.block_cache->GetUsage();
        size_t pinned_usage = block_based_options.block_cache->GetPinnedUsage();
        // size_t compressed_cache_usage = block_based_options.block_cache_compressed->GetUsage();
        cout << "Block cache usage: " << cache_usage << endl;
        cout << "Block cache pinned usage: " << pinned_usage << endl;
        // cout << "Compressed block cache usage: " << compressed_cache_usage << endl;
    }
    db->GetProperty("rocksdb.options-statistics", &out);
    parse_output(out);
    cout << out << endl;

    // measure_sizes();
    // measure_memory_usage();

    return 0;
}