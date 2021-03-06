//
// Taken from Bourbon code
//

#include <cstdint>
#include <cassert>
#include <utility>
#include <cmath>
#include <iostream>
#include <fstream>
#include <string>
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

    uint64_t ExtractInteger(const char* pos, size_t size) {
        char* temp = new char[size + 1];
        memcpy(temp, pos, size);
        temp[size] = '\0';
        uint64_t result = (uint64_t) atoll(temp);
        delete[] temp;
        return result;
    }

    int Numericcompare(const std::string a, const std::string b) {
        uint64_t ia = ExtractInteger(a.data(), a.size());
        uint64_t ib = ExtractInteger(b.data(), b.size());
        if (ia < ib) return -1;
        else if (ia == ib) return 0;
        else return 1;
    }

    int Customcompare(const std::string a, const std::string b) {
        // printf("Key size in customcompare %d\n", key_size);
        uint64_t ia = (uint64_t)(stoll(a.substr(0, key_size_changer)));
        uint64_t ib = (uint64_t)(stoll(b.substr(0, key_size_changer)));
        if (ia < ib) return -1;
        else if (ia == ib) return 0;
        else return 1;
    }

    std::pair<uint64_t, uint64_t> LearnedIndexData::GetPosition(const Slice& target_key, bool learn_block_num) const {
        assert(segments.size() > 1);

        // check if the key is within the model bounds
        std::string tgt = target_key.ToString();
        // if (debug == 1) {
        //     std::cout << tgt.size() << " " << max_key.size() << " " << min_key.size() << " " << std::endl;
        //     std::cout << compare(tgt, min_key) << " " << compare(tgt, max_key) << std::endl;
        // }
        if (Customcompare(tgt, min_key) < 0) return std::make_pair(size, size);
        if (Customcompare(tgt, max_key) > 0) return std::make_pair(size, size);
        
        // if (debug == 1) {
        //     std::cout << "Bound check done, error : " << error << "\n";
        // }
        // binary search between segments
        uint32_t left = 0, right = (uint32_t) segments.size() - 1;
        while (left != right - 1) {
            uint32_t mid = (right + left) / 2;
            if (Customcompare(tgt, segments[mid].start_key) < 0) right = mid;
            else left = mid;
        }

        // // calculate the interval according to the selected segment
        // uint32_t shared = segments[left].shared;
        // long double unshared_double = (long double)(stoll(target_key.ToString().substr(shared, shared + 8)));
        // long double result = unshared_double * segments[left].k + segments[left].b;
        // printf("Key size in learned_index.cpp %d\n", key_size);
        long double result = (long double)(stoll(target_key.ToString().substr(0, key_size_changer))) * segments[left].k + segments[left].b;
        
        // std::cout << "Pred? : " << result << std::endl;
        if (learn_block_num) {
            assert(!simLR_bounds.empty());
            int idx = simLR_bounds.size()-1;
            for (size_t i = 0; i < simLR_bounds.size()-1; i++) {
                if (result < simLR_bounds[i] - 1e-3) {
                    idx = i;
                    break;
                }
            }
            // std::cout << "Cutoff : " << simLR_bounds[idx] << "\n";
            return {-1, idx-1};
        }
        // if (debug == 1) {
        //     std::cout << "GetPosition point : " << target_key.ToString().substr(0, key_size_changer) << " " << segments[left].k << " " << segments[left].b << " \n";
        // }
        if (left != segments.size()-2) {
            long double next_block_offset = (long double)(stoll(segments[left+1].start_key.substr(0, key_size_changer))) * segments[left+1].k + segments[left+1].b;
            if (next_block_offset < result)
                // result -= data_block_sizes[data_block_sizes.size()-2]; // always exists since one block of data and one props block
                result = next_block_offset - min_proper_block_size;
        }
        // else {
        //     long double curr_block_offset = (long double)(stoll(segments[left].start_key.substr(0, 8))) * segments[left].k + segments[left].b;
        //     if (curr_block_offset + data_block_sizes[data_block_sizes.size()-2] < result)
        //         // result -= data_block_sizes[data_block_sizes.size()-2]; // always exists since one block of data and one props block
        //         result = curr_block_offset;
        // }

        uint64_t lower = result - error > 0 ? (uint64_t) std::floor(result - error) : 0;
        uint64_t upper = (uint64_t) std::ceil(result + error);
        // if (lower >= file_size) return std::make_pair(size, size);
        // upper = upper < size ? upper : size - 1;
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

    size_t LearnedIndexData::GetApproximateSize() const {
        // size_t seg_consts = 2*sizeof(double) + sizeof(long double) + sizeof(uint32_t);
        size_t seg_consts = 2*sizeof(double);
        if (!learned.load()) {
            return 0;
        } else {
            size_t res = 0;
            for (size_t i = 0; i < segments.size(); i++) {
                res += segments[i].start_key.size() + seg_consts;
            }
            res += (sizeof(uint16_t) + sizeof(double))*data_block_sizes.size();
            return res;
        }
    }

    // Actual function doing learning
    std::vector<Segment> LearnedIndexData::Learn(std::vector<std::pair<std::string, key_type> > input, Model model, std::string bound_key, long double seg_cost) {
        
        // Fill string key with offsets
        keys_with_offsets = input;

        // std::cout << "Inside LID\n";
        // for (auto val : keys_with_offsets) {
        //     std::cout << val.first.data() << std::endl; 
        // }
        
        // fill in some bounds for the model
        // std::string temp = keys_with_offsets.back().first;
        
        min_key = keys_with_offsets.front().first;
        // max_key = keys_with_offsets.back().first;
        max_key = bound_key;
        size = keys_with_offsets.size();

        // actual training
        std::vector<Segment> segs;
        // FILL IN GAMMA (error)
        PLR plr = PLR(error);
        SLSR slsr = SLSR();
        SimLR simLR = SimLR();

        switch(model) {
        case kGreedyPLR:
            segs = plr.train(keys_with_offsets, true);
            break;

        case kStatPLR:
            assert(seg_cost != -1);
            segs = slsr.train(keys_with_offsets, seg_cost);
            break;

        case kSimpleLR:
            segs = simLR.train(keys_with_offsets, simLR_bounds);
            break;

        default:
            exit(1);

        }
        
        if (segs.empty()) return segs;
        // fill in a dummy last segment (used in segment binary search)
        segs.push_back((Segment) {bound_key, 0, 0.0, 0, 0});
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

    void LearnedIndexData::WriteModel(const string &filename, std::vector<uint64_t> block_content_sizes, bool learn_block_num) {
        if (!learned.load()) return;

        std::ofstream output_file(filename);
        output_file.precision(15);
        for (Segment& item: segments) {
            output_file << item.start_key.data() << " " << item.shared << " " << item.error << " " << item.k << " " << item.b << "\n";
        }
        output_file << "Sizes\n";
        // std::cout << "Block content size : " << block_content_sizes.size() << std::endl;
        // for (auto sz : block_content_sizes) {
        //     output_file << sz << "\n";
        // }
        for (size_t i = 0; i < block_content_sizes.size(); i++) {
            if (learn_block_num)
                output_file << block_content_sizes[i] << " " << simLR_bounds[i] << "\n";
            else
                output_file << block_content_sizes[i] << "\n";;
        }
        output_file << "2441139" << " " << min_key << " " << max_key << " " << size << " " << level << " " << "\n";
        output_file.close();
    }

    void LearnedIndexData::ReadModel(const string &filename, bool learn_block_num) {
        // if (debug == 1) {
        //     printf("Reading file : %s\n", filename.c_str());
        // }
        
        if (!input_file.is_open()) {
            input_file.open(filename, std::ifstream::in);
            // std::cout << "Re-open file\n";
        }
        else
            std::cout << "No need to open " << filename << std::endl;

        // std::cout << "Model file opened\n";

        string start_key_data;
        long double err;

        if (!input_file.good()) return;
        // if (debug == 1)
        //     printf("Start reading file\n");
        while (true) {
            uint32_t shared;
            double k, b;
            input_file >> start_key_data;
            if (start_key_data.compare("Sizes") == 0) {
                // if (debug == 1) printf("All segments read\n");
                break;
            }
            input_file >> shared >> err >> k >> b;
            Segment seg = Segment(start_key_data, shared, err, k, b);
            segments.push_back(seg);
            error = std::max((double)error, (double)err);
        }
        while (true) {
            uint32_t data_block_size;
            double pred;
            input_file >> data_block_size;
            if (data_block_size == 2441139) break;
            if (learn_block_num) input_file >> pred;
            data_block_sizes.push_back(data_block_size);
            if (learn_block_num) simLR_bounds.push_back(pred);
        }
        // string min_key_str, max_key_str; 
        input_file >> min_key >> max_key >> size >> level;
        // min_key = Slice(min_key_str);
        // max_key = Slice(max_key_str);
        for (size_t i = 0; i < data_block_sizes.size()-2; i++) {
            min_proper_block_size = std::min(min_proper_block_size, data_block_sizes[i]);
        }
        input_file.close();
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
        return lid.GetPosition(key, false);
    }

    FileLearnedIndexData::~FileLearnedIndexData() {
        rocksdb::MutexLock l(&mutex);
        for (auto pointer: file_to_segments) {
            delete &pointer;
        }
    }
}
