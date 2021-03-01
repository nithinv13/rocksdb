#include "learning/plr.h"
#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

// Code modified from https://github.com/RyanMarcus/plr

double
get_slope(struct point p1, struct point p2) {
    return (p2.y - p1.y) / (p2.x - p1.x);
}

struct line
get_line(struct point p1, struct point p2) {
    double a = get_slope(p1, p2);
    double b = -a * p1.x + p1.y;
    struct line l{.a = a, .b = b};
    return l;
}

struct point
get_intersetction(struct line l1, struct line l2) {
    double a = l1.a;
    double b = l2.a;
    double c = l1.b;
    double d = l2.b;
    struct point p {"", 0, (d - c) / (a - b), (a * d - b * c) / (a - b)};
    return p;
}

bool
is_above(struct point pt, struct line l) {
    return pt.y > l.a * pt.x + l.b;
}

bool
is_below(struct point pt, struct line l) {
    return pt.y < l.a * pt.x + l.b;
}

struct point
get_upper_bound(struct point pt, double gamma) {
    struct point p {pt.key, pt.shared, pt.x, pt.y + gamma};
    return p;
}

struct point
get_lower_bound(struct point pt, double gamma) {
    struct point p {pt.key, pt.shared, pt.x, pt.y - gamma};
    return p;
}

GreedyPLR::GreedyPLR(double gamma_) {
    this->state = "need2";
    this->gamma = gamma_;
}

int counter = 0;

Segment
GreedyPLR::process(const struct point& pt) {
    this->last_pt = pt;
    if (this->state.compare("need2") == 0) {
        this->s0 = pt;
        this->state = "need1";
    } else if (this->state.compare("need1") == 0) {
        this->s1 = pt;
        setup();
        this->state = "ready";
    } else if (this->state.compare("ready") == 0) {
        return process__(pt, this->last_pt);
    } else {
        // impossible
        std::cout << "ERROR in process" << std::endl;
    }
    Segment s = {"", 0, 0, 0};
    return s;
}

void
GreedyPLR::setup() {
    this->rho_lower = get_line(get_upper_bound(this->s0, this->gamma),
                               get_lower_bound(this->s1, this->gamma));
    this->rho_upper = get_line(get_lower_bound(this->s0, this->gamma),
                               get_upper_bound(this->s1, this->gamma));
    this->sint = get_intersetction(this->rho_upper, this->rho_lower);
}

Segment
GreedyPLR::current_segment() {
    // uint64_t segment_start = this->s0.x;
    Slice segment_start = this->s0.key;
    uint32_t shared = this->s0.shared;
    double avg_slope = (this->rho_lower.a + this->rho_upper.a) / 2.0;
    double intercept = -avg_slope * this->sint.x + this->sint.y;
    Segment s = {segment_start, shared, avg_slope, intercept};
    return s;
}

Segment
GreedyPLR::process__(struct point pt, struct point last_pt_) {
    std::string last_string = last_pt_.key.ToString().substr(0, last_pt_.shared);
    std::string current_string = pt.key.ToString().substr(0, pt.shared);
    if (!(is_above(pt, this->rho_lower) && is_below(pt, this->rho_upper)) || (last_string.compare(current_string) != 0)) {
        Segment prev_segment = current_segment();
        this->s0 = pt;
        this->state = "need1";
        return prev_segment;
    }

    struct point s_upper = get_upper_bound(pt, this->gamma);
    struct point s_lower = get_lower_bound(pt, this->gamma);
    if (is_below(s_upper, this->rho_upper)) {
        this->rho_upper = get_line(this->sint, s_upper);
    }
    if (is_above(s_lower, this->rho_lower)) {
        this->rho_lower = get_line(this->sint, s_lower);
    }
    Segment s = {"", 0, 0, 0};
    return s;
}

Segment
GreedyPLR::finish() {
    Segment s = {"", 0, 0, 0};
    if (this->state.compare("need2") == 0) {
        this->state = "finished";
        return s;
    } else if (this->state.compare("need1") == 0) {
        this->state = "finished";
        s.start_key = this->s0.key;
        s.shared = 0;
        s.k = 0;
        s.b = this->s0.y;
        return s;
    } else if (this->state.compare("ready") == 0) {
        this->state = "finished";
        return current_segment();
    } else {
        std::cout << "ERROR in finish" << std::endl;
        return s;
    }
}

PLR::PLR(double gamma_) {
    this->gamma = gamma_;
}

std::vector<uint32_t> get_min_shared(std::vector<std::pair<Slice, uint64_t> >& keys, uint32_t shared_threshold) {
    size_t size = keys.size();
    Slice first_key;
    std::vector<uint32_t> minn_shared;
    for (size_t i = 0; i < size; i++) {
        if (i == 0) {
            first_key = keys[i].first;
            minn_shared.push_back(static_cast<uint32_t>(keys[i].first.size()));
            continue;
        }
        size_t shared = keys[i].first.difference_offset(first_key);
        if (shared < shared_threshold) {
            first_key = keys[i].first;
            minn_shared.push_back(keys[i].first.size());
            continue;
        } else {
            minn_shared.push_back(std::min(static_cast<uint32_t>(shared), minn_shared.back()));
        }
    }

    for (int i = size-2; i >= 0; i--) {
        if (minn_shared[i] != keys[i].first.size() && minn_shared[i+1] != keys[i+1].first.size()) {
            minn_shared[i] = std::min(minn_shared[i], minn_shared[i+1]);
        }
    }
    return minn_shared;
}

point get_unshared_point(const Slice& key, uint64_t offset, uint32_t minn_shared) {
    std::string x_str = key.ToString().substr(minn_shared, minn_shared + 8);
    long double x = (long double) stoll(x_str);
    return point(key, minn_shared, x, offset);
}

std::vector<Segment>
PLR::train(std::vector<std::pair<Slice, uint64_t> >& keys, bool file_level_learning=true) {
    assert(file_level_learning);
        
    GreedyPLR plr(this->gamma);
    size_t size = keys.size();
    std::vector<uint32_t> minn_shared = get_min_shared(keys, 8);

    for (size_t i = 0; i < size; ++i) {
        point p = get_unshared_point(keys[i].first, keys[i].second, minn_shared[i]);
        Segment seg = plr.process(p);
        if (seg.start_key != "" ||
            seg.shared != 0 ||
            seg.k != 0 ||
            seg.b != 0) {
            this->segments.push_back(seg);
        }
    }

    Segment last = plr.finish();
    if (last.start_key != "" ||
        last.shared != 0 ||
        last.k != 0 ||
        last.b != 0) {
        this->segments.push_back(last);
    }

    return this->segments;
}
