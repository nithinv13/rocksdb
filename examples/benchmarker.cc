#include "benchmarker.h"

string dbName = "/tmp/learnedDB";
int kTotalKeys = 100;

int main() {
    DB *db;
    Options options;
    options.write_buffer_size = 4 << 20;
    options.target_file_size_base = 4 << 20;
    // NumericalComparator numerical_comparator;
    // options.comparator = &numerical_comparator;
    CustomComparator custom_comparator;
    options.comparator = &custom_comparator;
    BlockBasedTableOptions block_based_options;
    options.create_if_missing = true;
    options.compression = kNoCompression;
    // block_based_options.block_align = true;
    block_based_options.block_cache =
      NewLRUCache(static_cast<size_t>(64 * 1024 * 1024));
    options.table_factory.reset(
          NewBlockBasedTableFactory(block_based_options));
    IngestExternalFileOptions ifo;
    rocksdb::Status s = DB::Open(options, dbName, &db);

    write(db, 200000, 8, true, true, 100000);
    read(db, 200000, true, 8, true, true, 100000);
    measure_sizes();

    return 0;
}