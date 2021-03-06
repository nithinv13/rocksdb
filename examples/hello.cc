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

#include <chrono>

using namespace std;
using namespace ROCKSDB_NAMESPACE;

string dbName = "/tmp/learnedDB";
int kTotalKeys = 100;

void write_ssts(SstFileWriter& sst_file_writer, string file_path, int start_key, int end_key) {
    rocksdb::Status s = sst_file_writer.Open(file_path);
    if (!s.ok()) {
        printf("Error while opening the file %s, Error: %s\n", file_path.c_str(), s.ToString().c_str());
        return;
    }

    for(int i = start_key; i < end_key; i++) {
        s = sst_file_writer.Put(rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i)));
        if (!s.ok()) {
            printf("%s\n", "SST write error");
        }
    }

    s = sst_file_writer.Finish();
}

void ingest_files(DB* db, IngestExternalFileOptions ifo, std::vector<string>& files) {
    Status s = db->IngestExternalFile(files, ifo);
    if (!s.ok()) {
        printf("%s\n", "Ingestion failure");
    }
}

void read_sst(Options& options, const std::string file_name) {
    options.compression = kSnappyCompression;
    SstFileReader reader(options);
    assert(reader.Open(file_name).ok());
    std::unique_ptr<Iterator> iter;
    {
        ReadOptions ropts;
        iter.reset(reader.NewIterator(ropts));
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        cout << iter->key().data() << " " << iter->value().ToString() << endl;
        iter->Next();
    }
}

void compact_files_helper(DB *db, std::vector<string>& input_files) {
    CompactionOptions c_options;
    db->CompactFiles(c_options, input_files, 1);
}

int main() {
    // cout << "Hi there" << endl;
    // assert(1 == 2 && "Hello");
    // cout << "Hi again" << endl;
    DB *db;
    Options options;
    BlockBasedTableOptions block_based_options;
    options.create_if_missing = true;
    block_based_options.block_align = true;
    options.table_factory.reset(
          NewBlockBasedTableFactory(block_based_options));
    IngestExternalFileOptions ifo;
    rocksdb::Status s = DB::Open(options, dbName, &db);
    s = db->Put(WriteOptions(), "key", "value");
    assert(s.ok());
    std::string value;
    s = db->Get(ReadOptions(), "key", &value);
    assert(s.ok());
    printf("%s:%s\n", "key", value.c_str());

    SstFileWriter sst_file_writer(EnvOptions(), options);
    string file_path = "/tmp/learnedDB/file1.sst";
    write_ssts(sst_file_writer, "/tmp/learnedDB/file1.sst", 100, 108);
    write_ssts(sst_file_writer, "/tmp/learnedDB/file2.sst", 200, 218);
    std::vector<std::string> input_files;
    input_files.push_back("/tmp/learnedDB/file1.sst");
    input_files.push_back("/tmp/learnedDB/file2.sst");
    //compact_files_helper(db, input_files);
    for (std::string file: input_files) {
        printf("%s\n", file.c_str());
    }
    ingest_files(db, ifo, input_files);

    // ReadOptions read_options = ReadOptions();
    // read_options.learned_get = true;
    // s = db->Get(read_options, rocksdb::Slice(std::to_string(102)), &value);
    // printf("%s\n", value.c_str());
    // read_sst(options, std::string("/tmp/learnedDB/file1.sst"));
    // read_sst(options, std::string("/tmp/learnedDB/file2.sst"));
    input_files.clear();
    input_files.insert(input_files.end(), {"/tmp/learnedDB/000007.sst", "/tmp/learnedDB/000008.sst"});
    rocksdb::CompactionOptions c_options;
    std::vector<string> output_files;
    s = db->CompactFiles(c_options, input_files, 1, -1, &output_files);
    assert(s.ok());
    ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);
    for (auto level : cf_meta.levels) {
        printf("%d\n", level.level);
        for (auto file : level.files) {
            printf("%s\n", file.name.c_str());
            read_sst(options, std::string("/tmp/learnedDB/").append(file.name));
        }
    }

    ReadOptions read_options = ReadOptions();
    read_options.learned_get = true;
    s = db->Get(read_options, rocksdb::Slice(std::to_string(99)), &value);
    printf("%s\n", value.c_str());
    return 0;
}