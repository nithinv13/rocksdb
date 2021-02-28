//  Copyright (c) 2013-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else
#include <rocksdb/db_bench_tool.h>
int main(int argc, char** argv) {
  return ROCKSDB_NAMESPACE::db_bench_tool(argc, argv);
}
#endif  // GFLAGS


// #include <iostream>
// #include <string>
// #include "rocksdb/db.h"
// #include "rocksdb/slice.h"
// #include "rocksdb/options.h"
// #include "rocksdb/env.h"
// #include "rocksdb/iterator.h"
// #include "rocksdb/sst_file_writer.h"
// #include "rocksdb/sst_file_reader.h"

// using namespace std;
// using namespace ROCKSDB_NAMESPACE;

// string dbName = "/tmp/learnedDB";
// int kTotalKeys = 100;

// void write_ssts(SstFileWriter& sst_file_writer, string file_path, int start_key, int end_key) {
//     rocksdb::Status s = sst_file_writer.Open(file_path);
//     if (!s.ok()) {
//         printf("Error while opening the file %s, Error: %s\n", file_path.c_str(), s.ToString().c_str());
//         return;
//     }

//     for(int i = start_key; i < end_key; i++) {
//         s = sst_file_writer.Put(rocksdb::Slice(std::to_string(i)), rocksdb::Slice(std::to_string(i)));
//     }

//     s = sst_file_writer.Finish();
// }

// void read_sst(Options& options, const std::string file_name) {
//     options.compression = kSnappyCompression;
//     SstFileReader reader(options);
//     assert(reader.Open(file_name).ok());
//     std::unique_ptr<Iterator> iter;
//     {
//         ReadOptions ropts;
//         iter.reset(reader.NewIterator(ropts));
//     }
//     iter->SeekToFirst();
//     while (iter->Valid()) {
//         cout << iter->key().data() << " " << iter->value().ToString() << endl;
//         iter->Next();
//     }
// }

// void compact_files_helper(DB *db, std::vector<string>& input_files) {
//     CompactionOptions c_options;
//     db->CompactFiles(c_options, input_files, 1);
// }

// int main() {
//     // cout << "Hi there" << endl;
//     // assert(1 == 2 && "Hello");
//     // cout << "Hi again" << endl;
//     DB *db;
//     Options options;
//     rocksdb::Status s = DB::Open(options, dbName, &db);

//     SstFileWriter sst_file_writer(EnvOptions(), options);
//     string file_path = "/tmp/learnedDB/file1.sst";
//     // write_ssts(sst_file_writer, "/tmp/learnedDB/file1.sst", 1, 1000);
//     // write_ssts(sst_file_writer, "/tmp/learnedDB/file2.sst", 900, 2000);
//     std::vector<std::string> input_files;
//     input_files.push_back("/tmp/learnedDB/file1.sst");
//     input_files.push_back("/tmp/learnedDB/file2.sst");
//     //compact_files_helper(db, input_files);
//     for (std::string file: input_files) {
//         printf("%s\n", file.c_str());
//     }
//     // read_sst(options, std::string("/tmp/learnedDB/file1.sst"));
//     // read_sst(options, std::string("/tmp/learnedDB/file2.sst"));
//     // rocksdb::CompactionOptions c_options;
//     // db->CompactFiles(c_options, input_files, 1);
//     ColumnFamilyMetaData cf_meta;
//     // db->GetColumnFamilyMetaData(&cf_meta);
//     // for (auto level : cf_meta.levels) {
//     //     for (auto file : level.files) {
//     //         printf("%s\n", file.name.c_str());
//     //     }
//     // }

//     return 0;
// }