#include "learning/slsr.h"
#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
#include <limits>
#define inf std::numeric_limits<double>::infinity()

SLSR::SLSR() {
    cum_x.push_back(0);
    cum_y.push_back(0);
    cum_xy.push_back(0);
    cum_x2.push_back(0);
}

std::vector<Segment> SLSR::train(std::vector<std::pair<std::string, key_type> >& keys) {

    size = keys.size();
    cum_x.resize(size+1);
    cum_y.resize(size+1);
    cum_xy.resize(size+1);
    cum_x2.resize(size+1);
	
	slope.resize(size+1);
	intercept.resize(size+1);
	error.resize(size+1);

	optimal.resize(size+1);
	opt_segment.resize(size+1);

	for (int i = 0; i <= size; i++) {
		slope[i].resize(size+1);
		intercept[i].resize(size+1);
		error[i].resize(size+1);
	}

	long long x_sum, y_sum, xy_sum, x2_sum, num, denom;
	long interval, i, j, k;

    for (j = 1; j <= size; j++)	{
		long double key = stoll(keys[j-1].first.substr(0, 8));
		long double off = keys[j-1].second;
		cum_x[j] = cum_x[j-1] + key;
		cum_y[j] = cum_y[j-1] + off;
		cum_xy[j] = cum_xy[j-1] + key * off;
		cum_x2[j] = cum_x2[j-1] + off * off;
		
		for (i = 1; i <= j; i++) {
			interval = j - i + 1;
			x_sum = cum_x[j] - cum_x[i-1];
			y_sum = cum_y[j] - cum_y[i-1];
			xy_sum = cum_xy[j] - cum_xy[i-1];
			x2_sum = cum_x2[j] - cum_x2[i-1];
			
			num = interval * xy_sum - x_sum * y_sum;
			if (num == 0)
				slope[i][j] = 0.0;
			else {
				denom = interval * x2_sum - x_sum * x_sum;
				slope[i][j] = (denom == 0) ? inf : (num / double(denom));				
			}
            
			intercept[i][j] = (y_sum - slope[i][j] * x_sum) / double(interval);
            
           	for (k = i, error[i][j] = 0.0; k <= j; k++)	{
            	long double err = (long double)keys[k-1].second - slope[i][j] * stoll(keys[k-1].first.substr(0, 8)) - intercept[i][j];
            	error[i][j] += err * err;
            }
		}
	}

	// find the cost of the optimal solution
	optimal[0] = opt_segment[0] = 0;
	long double min;
	for (j = 1; j <= size; j++)	{
		for (i = 1, min = inf, k = 0; i <= j; i++)	{
			long double tmp = error[i][j] + optimal[i-1];
			if (tmp < min)	{
				min = tmp;
				k = i;
			}
		}
		optimal[j] = min + 100;
		opt_segment[j] = k;
	}

	if (debug == 1) {
		printf("\nAn optimal solution :\n");
	}
	
	for (i = size, j = opt_segment[size]; i > 0; i = j-1, j = opt_segment[i])	{
		segments.push_back(Segment(keys[j-1].first, error[j][i], slope[j][i], intercept[j][i]));
		if (debug == 1)
			printf("Segment (y = %lf * x + %lf) from points %ld to %ld with square error %lf.\n", 
				(double)slope[j][i], (double)intercept[j][i], j, i, (double)error[j][i]);
	}

	return segments;    
}


SimLR::SimLR() {

}

std::vector<Segment> SimLR::train(std::vector<std::pair<std::string, key_type> >& keys) {
	
	size = keys.size();
	long double mean_x = 0, mean_y = 0, cum_xy = 0, cum_x2 = 0;
	for (int i = 0; i < size; i++) {
		long double key = stoll(keys[i].first.substr(0, 8));
		mean_x += key;
		mean_y += (long double)keys[i].second;
		cum_x2 += key * key;
		cum_xy += key * (long double)keys[i].second;
	}
	mean_x /= size;
	mean_y /= size;
	long double S_xy = cum_xy - size * mean_x * mean_y;
	long double S_xx = cum_x2 - size * mean_x * mean_x;
	long double slope = S_xy / S_xx;
	long double intercept = mean_y - slope * mean_x;
	
	long double error = 0;
	for (int i = 0; i < size; i++) {
		long double key = stoll(keys[i].first.substr(0, 8));
		auto pred = slope * key + intercept;
		auto err = abs(keys[i].second - pred);
		// std::cout << err << " " ;
		error = std::max((double)err, (double)error);
	}
	segments.push_back(Segment(keys[0].first, error, slope, intercept));


	std::cout << std::endl << error << std::endl;
	
	return segments;
}