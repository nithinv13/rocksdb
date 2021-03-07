//
// Taken from Bourbon code
//

#include <cstdint>
#include <cassert>
#include <utility>
#include <cmath>
#include <iostream>
#include <fstream>
#include "learning/learned_index.h"
#include "db/version_set.h"

namespace adgMod {

    int compare(std::string s1, std::string s2) {
        for (uint64_t i = 0; ; i++) {
            // if (debug == 1) {
            //     std::cout << s1[i] << "-" << s2[i] << "|\n";
            // }
            if (i == s1.size() && i == s2.size())
                return 0;
            else if (i == s1.size())
                return -1;
            else if (i == s2.size())
                return 1;
            else {              
                if (s1[i] < s2[i])
                    return -1;
                else if (s1[i] > s2[i])
                    return 1;
            }
        }
    }

    std::pair<uint64_t, uint64_t> LearnedIndexData::GetPosition(const Slice& target_key) const {
        assert(segments.size() > 1);

        // check if the key is within the model bounds
        std::string tgt = target_key.ToString();
        if (debug == 1) {
            std::cout << tgt.size() << " " << max_key.size() << " " << min_key.size() << " " << std::endl;
            std::cout << compare(tgt, min_key) << " " << compare(tgt, max_key) << std::endl;
        }
        if (compare(tgt, min_key) < 0) return std::make_pair(size, size);
        if (compare(tgt, max_key) > 0) return std::make_pair(size, size);
        
        if (debug == 1) {
            std::cout << "Bound check done\n";
        }
        // binary search between segments
        uint32_t left = 0, right = (uint32_t) segments.size() - 1;
        while (left != right - 1) {
            uint32_t mid = (right + left) / 2;
            if (compare(tgt, segments[mid].start_key) < 0) right = mid;
            else left = mid;
        }

        // calculate the interval according to the selected segment
        uint32_t shared = segments[left].shared;
        long double unshared_double = (long double)(stoll(target_key.ToString().substr(shared, shared + 8)));
        long double result = unshared_double * segments[left].k + segments[left].b;
        uint64_t lower = result - error > 0 ? (uint64_t) std::floor(result - error) : 0;
        uint64_t upper = (uint64_t) std::ceil(result + error);
        if (lower >= size) return std::make_pair(size, size);
        upper = upper < size ? upper : size - 1;
//                printf("%s %s %s\n", string_keys[lower].c_str(), string(target_x.data(), target_x.size()).c_str(), string_keys[upper].c_str());
//                assert(target_x >= string_keys[lower] && target_x <= string_keys[upper]);
        return std::make_pair(lower, upper);
    }

    uint64_t LearnedIndexData::MaxPosition() const {
        return size - 1;
    }

    double LearnedIndexData::GetError() const {
        return error;
    }

    // Actual function doing learning
    std::vector<Segment> LearnedIndexData::Learn(std::vector<std::pair<std::string, key_type> > input) {
        // FILL IN GAMMA (error)
        PLR plr = PLR(error);
        
        // Fill string key with offsets
        keys_with_offsets = input;

        // std::cout << "Inside LID\n";
        // for (auto val : keys_with_offsets) {
        //     std::cout << val.first.data() << std::endl; 
        // }
        
        // fill in some bounds for the model
        std::string temp = keys_with_offsets.back().first;
        min_key = keys_with_offsets.front().first;
        max_key = keys_with_offsets.back().first;
        size = keys_with_offsets.size();

        // actual training
        std::vector<Segment> segs = plr.train(keys_with_offsets, true);
        if (segs.empty()) return segs;
        // fill in a dummy last segment (used in segment binary search)
        segs.push_back((Segment) {temp, 0, 0, 0});
        segments = std::move(segs);

        learned.store(true);
        //string_keys.clear();
        return segs;
    }

    // general model checker
    bool LearnedIndexData::Learned() {
        if (learned.load()) {
            return true;
        } else return false;
    }

    void LearnedIndexData::WriteModel(const string &filename) {
        if (!learned.load()) return;

        std::ofstream output_file(filename);
        output_file.precision(15);
        for (Segment& item: segments) {
            output_file << item.start_key.data() << " " << item.shared << " " << item.k << " " << item.b << "\n";
        }
        output_file << "StartAcc" << " " << min_key << " " << max_key << " " << size << " " << level << " " << "\n";
    }

    void LearnedIndexData::ReadModel(const string &filename) {
        if (debug == 1) {
            printf("Reading file %s\n", filename.c_str());
        }
        std::ifstream input_file(filename);
        string start_key_data;

        if (!input_file.good()) return;
        while (true) {
            uint32_t shared;
            double k, b;
            input_file >> start_key_data;
            // std::string result = std::move(start_key_data);
            if (start_key_data == "StartAcc") break;
            input_file >> shared >> k >> b;
            Segment seg = Segment(start_key_data, shared, k, b);
            segments.push_back(seg);
        }
        // string min_key_str, max_key_str; 
        input_file >> min_key >> max_key >> size >> level;
        // min_key = Slice(min_key_str);
        // max_key = Slice(max_key_str);

        learned.store(true);
    }

    void FileLearnedIndexData::StoreSegments(std::string file_name, std::vector<Segment> segments) {
        file_to_segments[file_name] = segments;
        return;
    }

    std::vector<Segment> FileLearnedIndexData::GetSegments(std::string file_name) {
        // std::string file_name = std::to_string(file->fd.GetNumber());
        return file_to_segments[file_name];
    }

    bool FileLearnedIndexData::Learned(FileMetaData* meta = nullptr, int level = -1) {
        assert(meta == nullptr && level == -1);
        return false;
    }

    std::pair<uint64_t, uint64_t> FileLearnedIndexData::GetPosition(const Slice &key, std::string file_name) {
        auto segments = file_to_segments[file_name];
        LearnedIndexData lid;
        return lid.GetPosition(key);
    }

    FileLearnedIndexData::~FileLearnedIndexData() {
        rocksdb::MutexLock l(&mutex);
        for (auto pointer: file_to_segments) {
            delete &pointer;
        }
    }
}
