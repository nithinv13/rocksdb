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

string dbName = "/tmp/learnedDB";
int kTotalKeys = 100;

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


void write_seq(DB* db, uint64_t num_entries, int key_size) {
    WriteOptions write_options;
    rocksdb::Status s;
    for (uint64_t i = 0; i < num_entries; i++) {
        if (i % 10000 == 0) {
            cout << "Completed " << std::to_string(i) << " writes" << endl;
        }
        std::string key = std::to_string(i);
        string result = string(key_size - key.length(), '0') + key;
        // std::string result = key;
        s = db->Put(write_options, Slice(result), Slice(result));
        if (!s.ok()) { 
            printf("Error in writing key %s", key.c_str());
            break;
        }
    }
}

void read_seq(DB* db, uint64_t num_entries, bool use_learning, int key_size) {
    ReadOptions read_options;
    if (use_learning) {
        read_options.learned_get = true;
    }
    rocksdb::Status s;
    uint64_t operation_count = 0;
    uint64_t total_time;
    std::string value;
    for (uint64_t i = 2; i < num_entries; i++) {
        if (i % 10000 == 0) {
            cout << "Completed " << std::to_string(i) << " reads" << endl;
        }
        std::string key = std::to_string(i);
        std::string result = string(key_size - key.length(), '0') + key;
        // std::string result = key;
        auto start = high_resolution_clock::now();
        s = db->Get(read_options, Slice(result), &value);
        auto stop = high_resolution_clock::now();
        cout << value << " " << result << endl;
        assert(value == result);
        uint64_t duration = static_cast<uint64_t>(duration_cast<microseconds>(stop - start).count());
        total_time += duration;
        operation_count += 1;
    }
    cout << "Total number of read operations: " << operation_count << endl;
    cout << "Total time taken: " << total_time << endl;
    cout << "Throughput: " << operation_count * 1000000 / total_time << endl;
    cout << "Average latency (us/op): " << total_time / operation_count << endl;
}

bool endsWith (std::string const &fullstring, std::string const &ending) {
    if (fullstring.length() >= ending.length()) {
        return fullstring.compare(fullstring.length() - ending.length(), ending.length(), ending) == 0;
    } else {
        return false;
    }
}

void measure_sizes() {
    rocksdb::Options options;
    rocksdb::SstFileReader reader(options);
    DIR *dir;
    struct dirent *ent;
    if ((dir = opendir ("/tmp/learnedDB")) != NULL) {
        while ((ent = readdir (dir)) != NULL) {
            // printf ("%s\n", ent->d_name);
            std::string file_name = ent->d_name;
            std::string file_path = "/tmp/learnedDB/";
            file_path = file_path.append(file_name);
            if (endsWith(file_name, ".sst")) {
                cout << file_path << " " << endl;
                assert(reader.Open(file_path).ok());
                std::shared_ptr<const rocksdb::TableProperties> p = reader.GetTableProperties();
                cout << file_name << " " << p->index_size << " " << p->data_size << " " << p->num_data_blocks << endl;

                std::string learned_file_name(file_name);
                learned_file_name.erase(0, learned_file_name.find_first_not_of("0"));
                learned_file_name = learned_file_name.append(".txt");
                std::string learned_file_path = file_path.append(learned_file_name);
                // std::ifstream in(learned_file_name, std::ifstream::ate | std::ifstream::binary);
                // cout << learned_file_name << " " << in.tellg() << endl; 

                // FILE *p_file = NULL;
                // p_file = fopen(learned_file_name.c_str(),"rb");
                // fseek(p_file,0,SEEK_END);
                // int size = ftell(p_file);
                // fclose(p_file);
                // cout << learned_file_name << " " << size << endl;
            }
        }
        closedir (dir);
    } else {
        perror ("");
        return;
    }

    // ColumnFamilyMetaData cf_meta;
    // db->GetColumnFamilyMetaData(&cf_meta);
    // for (LevelMetaData level : cf_meta.levels) {
    //     for (FileMetaData* file : level.files) {
    //         file->
    //     }
    // }
}

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

    write_seq(db, 200000, 8);
    read_seq(db, 200000, true, 8);
    // measure_sizes();

    return 0;
}