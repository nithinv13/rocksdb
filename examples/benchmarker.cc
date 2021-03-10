#include "benchmarker.h"

string dbName = "/tmp/learnedDB";
int kTotalKeys = 100;

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
                cout << file_name << " " << p->num_data_blocks << " " << p->data_size << " " << p->index_size << " " << p->filter_size << endl;
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

    write(db, 200000, 8, true, true, 100000);
    read(db, 200000, true, 8, true, true, 100000);
    measure_sizes();

    return 0;
}