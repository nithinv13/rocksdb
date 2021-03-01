#include <string>
#include <vector>
#include <deque>
#include "rocksdb/slice.h"

using namespace ROCKSDB_NAMESPACE;

// Code modified from https://github.com/RyanMarcus/plr

struct point {
    Slice key;
    uint32_t shared;
    long double x;
    long double y;

    point() = default;
    point(Slice key_, uint32_t shared_, long double x_, long double y_) {
        key = key_;
        shared = shared_;
        x = x_;
        y = y_;
    }
};

struct line {
    double a;
    double b;
};

class Segment {
public:
    Segment(Slice _start_key, uint32_t _shared, double _k, double _b) : start_key(_start_key), shared(_shared), k(_k), b(_b) {}
    Slice start_key;
    uint32_t shared;
    double k;
    double b;
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
    std::vector<Segment> train(std::vector<std::pair<Slice, uint64_t> >& keys, bool file);
//    std::vector<double> predict(std::vector<double> xx);
//    double mae(std::vector<double> y_true, std::vector<double> y_pred);
};
