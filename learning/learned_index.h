//
// Taken from Bourbon source code
//

#ifndef ROCKSDB_LEARNED_INDEX_H
#define ROCKSDB_LEARNED_INDEX_H


#include <vector>
#include <cstring>
#include "util.h"
#include <atomic>
#include "mod/plr.h"
#include "rocksdb/slice.h"
#include "rocksdb/version.h"

using std::string;
using rocksdb::Slice;
using namespace ROCKSDB_NAMESPACE;

namespace adgMod {

    double model_error = 8*128;

    class LearnedIndexData;

    // The structure for learned index. Could be a file model or a level model
    class LearnedIndexData {
    private:
        // predefined model error
        double error;
        // some flags used in online learning to control the state of the model
        std::atomic<bool> learned;
        std::atomic<bool> aborted;
        std::atomic<bool> learning;
    public:
        // is the data of this model filled (ready for learning)
        bool filled;

        // Learned linear segments and some other data needed
        std::vector<Segment> segments;
        Slice min_key;
        Slice max_key;
        uint64_t size;

    public:
        // all keys in the file with offsets to be learned from
        std::vector<std::pair<Slice, uint64_t> > keys_with_offsets;

        int level;

        explicit LearnedIndexData() : error(adgMod::model_error), learned(false), aborted(false), learning(false),
            filled(false), level(0) {};
        LearnedIndexData(const LearnedIndexData& other) = delete;

        // Inference function. Return the predicted interval.
        // If the key is in the training set, the output interval guarantees to include the key
        // otherwise, the output is undefined!
        // If the output lower bound is larger than MaxPosition(), the target key is not in the file
        std::pair<uint64_t, uint64_t> GetPosition(const Slice& key, std::vector<Segment> segments) const;
        uint64_t MaxPosition() const;
        double GetError() const;
        
        // Learning function and checker (check if this model is available)
        std::vector<Segment> Learn(std::vector<std::pair<Slice, uint64_t> > input);
        bool Learned();

        // writing this model to disk and load this model from disk
        void WriteModel(const string& filename);
        void ReadModel(const string& filename);
        
        // print model stats
        void ReportStats();
    };

    // an array storing all file models and provide similar access interface with multithread protection
    class FileLearnedIndexData {
    private:
        rocksdb::port::Mutex mutex;
        // std::vector<LearnedIndexData*> file_learned_index_data;
        std::map<std::string, std::vector<Segment> > file_to_segments;
    public:
        uint64_t watermark;
        bool Learned(FileMetaData* meta, int level);
        std::vector<std::string>& GetData(FileMetaData* meta);
        std::pair<uint64_t, uint64_t> GetPosition(const Slice& key, int file_num);
        void StoreSegments(std::string file_name, std::vector<Segment> segments);
        std::vector<Segment> GetSegments(FileMetaData* file);
        LearnedIndexData* GetModel(int number);
        void Report();
        ~FileLearnedIndexData();
    };

}

#endif
