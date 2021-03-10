#include "benchmarker.h"

string dbName = "/tmp/learnedDB";

void measure_memory_usage() {
    return;
}

int main(int argc, char **argv) {
    cout << argc << endl;
    assert(argc == 2);
    int index_type = stoi(argv[1]);
    rocksdb::DB *db;
    rocksdb::Options options;
    options.write_buffer_size = 4 << 20;
    options.target_file_size_base = 4 << 20;
    options.create_if_missing = true;
    options.compression = kNoCompression;
    // NumericalComparator numerical_comparator;
    // options.comparator = &numerical_comparator;
    CustomComparator custom_comparator;
    options.comparator = &custom_comparator;
    BlockBasedTableOptions block_based_options;
    MetadataCacheOptions metadata_cache_options;
    // block_based_options.block_align = true;
    // block_based_options.cache_index_and_filter_blocks = true;
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
        // case 4:
        //     block_based_options.index_type = BlockBasedTableOptions::IndexType::kLearnedSearch;
        //     break;
        default:
            std::cout <<  "Option not available\n" << std::endl;
    }
    block_based_options.block_cache =  NewLRUCache(static_cast<size_t>(2 * 1024 * 1024));
    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    rocksdb::Status s = DB::Open(options, dbName, &db);

    write(db, 200000, 8, true, true, 200000);
    read(db, 2000, false, 8, true, true, 200000);

    std::string out;
    db->GetProperty("rocksdb.estimate-table-readers-mem", &out);
    size_t cache_usage = block_based_options.block_cache->GetUsage();
    size_t pinned_usage = block_based_options.block_cache->GetPinnedUsage();
    cout << "Table reader memory usage: " << out << endl;
    cout << "Block cache usage: " << cache_usage << endl;
    cout << "Block cache pinned usage: " << pinned_usage << endl;

    measure_sizes();
    // measure_memory_usage();

    return 0;
}