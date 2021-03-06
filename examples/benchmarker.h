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
#include <dirent.h>
#include <stdio.h>
#include <iterator>
#include <algorithm>
#include <random>

using namespace std;
using namespace std::chrono;
using namespace ROCKSDB_NAMESPACE;

// int key_size = 8;

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
        uint64_t ia = (uint64_t)(stoll(a.ToString().substr(0, key_size_changer)));
        uint64_t ib = (uint64_t)(stoll(b.ToString().substr(0, key_size_changer)));
        if (ia < ib) return -1;
        else if (ia == ib) return 0;
        else return 1;
    }
    virtual void FindShortestSeparator(std::string* start, const Slice& limit) const { return; };
    virtual void FindShortSuccessor(std::string* key) const { return; };
};

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

void removeObsoleteLearnedFiles(std::string directory) {
	vector<std::string> files;
	DIR *dir;
	struct dirent *entry;
	if ((dir = opendir(directory.c_str())) != NULL) {
		while ((entry = readdir(dir)) != NULL) {
			files.push_back(std::string(entry->d_name));
		}
		closedir(dir);
	}
	
	for (auto p = files.begin(); p != files.end(); p++) {
		std::string file_name = *p;
		size_t start_pos = file_name.find(".txt");
		if (start_pos != std::string::npos) {
			std::string new_file = file_name;
			new_file.replace(start_pos, new_file.length()-start_pos, ".sst");
			// cout << file_name << " " << new_file << endl;
			if (std::find(files.begin(), files.end(), new_file) == files.end()) {
                std::string temp_dir = directory;
                std::string file_delete = temp_dir.append("/").append(file_name);
                // cout << file_delete << endl;
				int i = remove(file_delete.c_str());
                temp_dir = directory;
                file_delete = temp_dir.append("/").append(file_name.replace(start_pos, file_name.length()-start_pos, ".offsets"));
                i = remove(file_delete.c_str());
                if (i != 0) {
                    cout << "Not able to delete file" << endl;
                }
			}
		}
	}
}