//
// Taken from Bourbon source code
//

#ifndef ROCKSDB_LEARNED_INDEX_H
#define ROCKSDB_LEARNED_INDEX_H


#include <vector>
#include <cstring>
#include <atomic>
#include <unordered_map>
#include "rocksdb/slice.h"
#include "db/version_edit.h"
#include "port/likely.h"
#include "learning/plr.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/table.h"

using std::string;
using rocksdb::Slice;
using namespace ROCKSDB_NAMESPACE;

namespace adgMod {

    // double model_error = 8*128;

    class LearnedIndexData;

    // The structure for learned index. Could be a file model or a level model
    class LearnedIndexData {
    private:
        // predefined model error
        long double error;
        // some flags used in online learning to control the state of the model
        std::atomic<bool> learned;
        std::atomic<bool> aborted;
        std::atomic<bool> learning;

        std::ifstream input_file;
    public:
        // is the data of this model filled (ready for learning)
        bool filled;

        // Learned linear segments and some other data needed
        std::vector<Segment> segments;
        
        std::string min_key;
        std::string max_key;
        uint64_t size;

        // all keys in the file with offsets to be learned from
        std::vector<std::pair<std::string, key_type> > keys_with_offsets;

        std::vector<uint32_t> data_block_sizes;

        uint16_t block_size;
        uint32_t min_proper_block_size;

        int level;

        explicit LearnedIndexData() : error(2000.0), learned(false), aborted(false), learning(false),
            filled(false), level(0) {};
        LearnedIndexData(const LearnedIndexData& other) = default;

        LearnedIndexData& operator=(const LearnedIndexData &other) {
            segments = other.segments;
            min_key = other.min_key;
            max_key = other.max_key;
            size = other.size;
            keys_with_offsets = other.keys_with_offsets;
            data_block_sizes = other.data_block_sizes;
            error = other.error;

            return *this;
        }

        // Inference function. Return the predicted interval.
        // If the key is in the training set, the output interval guarantees to include the key
        // otherwise, the output is undefined!
        // If the output lower bound is larger than MaxPosition(), the target key is not in the file
        std::pair<uint64_t, uint64_t> GetPosition(const Slice& key) const;
        uint64_t MaxPosition() const;
        double GetError() const;
        size_t GetApproximateSize() const;
        
        // Learning function and checker (check if this model is available)
        std::vector<Segment> Learn(std::vector<std::pair<std::string, key_type> > input, Model model, std::string bound_key, long double seg_cost);
        bool Learned();

        // writing this model to disk and load this model from disk
        void WriteModel(const string& filename, std::vector<uint64_t> block_content_sizes);
        void ReadModel(const string& filename);
        
        // print model stats
        void ReportStats();
    };

    // an array storing all file models and provide similar access interface with multithread protection
    class FileLearnedIndexData {
    private:
        port::Mutex mutex;
        // std::vector<LearnedIndexData*> file_learned_index_data;
        std::unordered_map<std::string, std::vector<Segment> > file_to_segments;
    public:
        uint64_t watermark;
        bool Learned(FileMetaData* meta, int level);
        std::vector<std::string>& GetData(FileMetaData* meta);
        std::pair<uint64_t, uint64_t> GetPosition(const Slice& key, std::string file_name);
        void StoreSegments(std::string file_name, std::vector<Segment> segments);
        std::vector<Segment> GetSegments(std::string file_name);
        LearnedIndexData* GetModel(int number);
        void Report();
        ~FileLearnedIndexData();
    };

}

#endif
