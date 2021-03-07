#include <string>
#include <vector>
#include <deque>
#include "rocksdb/slice.h"
#include "rocksdb/rocksdb_namespace.h"

using namespace ROCKSDB_NAMESPACE;

typedef uint64_t key_type; 

// Code modified from https://github.com/RyanMarcus/plr

struct point {
    std::string key;
    uint32_t shared;
    long double x;
    long double y;

    point() = default;
    point(std::string key_, uint32_t shared_, long double x_, long double y_) {
        key = key_;
        shared = shared_;
        x = x_;
        y = y_;
    }

    void ToString() {
        printf("key:%s, shared:%ud, x:%Lf, y:%Lf", key.c_str(), shared, x, y);
    }
};

struct line {
    double a;
    double b;
};

class Segment {
public:
    Segment(std::string _start_key, uint32_t _shared, double _k, double _b) : start_key(_start_key), shared(_shared), k(_k), b(_b) {}
    std::string start_key;
    uint32_t shared;
    double k;
    double b;

    std::string ToString() {
        std::string result;
        result.append("\nStart key:").append(start_key.c_str());
        result.append("\nShared:").append(std::to_string(shared));
        result.append("\nk:").append(std::to_string(k));
        result.append("\nb:").append(std::to_string(b));
        return result;
    }
};

double get_slope(struct point p1, struct point p2);
struct line get_line(struct point p1, struct point p2);
struct point get_intersetction(struct line l1, struct line l2);

bool is_above(struct point pt, struct line l);
bool is_below(struct point pt, struct line l);

struct point get_upper_bound(struct point pt, double gamma);
struct point get_lower_bound(struct point pt, double gamma);


class GreedyPLR {
private:
    std::string state;
    double gamma;
    struct point last_pt;
    struct point s0;
    struct point s1;
    struct line rho_lower;
    struct line rho_upper;
    struct point sint;
    std::string current_shared;

    void setup();
    Segment current_segment();
    Segment process__(struct point pt, struct point last_pt);

public:
    GreedyPLR(double gamma);
    Segment process(const struct point& pt);
    Segment finish();
};

class PLR {
private:
    double gamma;
    std::vector<Segment> segments;

public:
    PLR(double gamma);
    std::vector<Segment> train(std::vector<std::pair<std::string, key_type> >& keys, bool file);
//    std::vector<double> predict(std::vector<double> xx);
//    double mae(std::vector<double> y_true, std::vector<double> y_pred);
};
