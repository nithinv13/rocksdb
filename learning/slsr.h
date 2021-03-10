#include <vector>
#include "rocksdb/slice.h"
#include "rocksdb/rocksdb_namespace.h"

using namespace ROCKSDB_NAMESPACE;

typedef uint64_t key_type; 

class Segment {
public:
    Segment(std::string _start_key, double _error, double _slope, double _intercept) : start_key(_start_key), error(_error), slope(_slope), intercept(_intercept) {}
    std::string start_key; // all keys >= start_key in this segment
    long double error;
    long double slope;
    long double intercept;
    
    std::string ToString() {
        std::string result;
        result.append("\nStart key:").append(start_key.c_str());
        result.append("\nslope:").append(std::to_string(slope));
        result.append("\nintercept:").append(std::to_string(intercept));
        return result;
    }
};

class SLSR {
    std::vector<long double> cum_x, cum_y, cum_xy, cum_x2; // for inferring optimal segments
    std::vector<Segment> segments;
    std::vector<std::vector<long double> > slope, intercept, error; // var[i][j] concerns points i to j
    std::vector<long double> optimal, opt_segment;
    long size;
public:
    SLSR();
    std::vector<Segment> train(std::vector<std::pair<std::string, key_type> >& keys);
};

class SimLR {
    std::vector<Segment> segments;
    long size;
public:
    SimLR();
    std::vector<Segment> train(std::vector<std::pair<std::string, key_type> >& keys);
};