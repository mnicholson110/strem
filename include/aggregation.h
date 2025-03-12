#ifndef strem_aggregation_h
#define strem_aggregation_h

#include <stdbool.h>
#include <uthash.h>

// this need to be more generic. char* key?
typedef struct
{
    int key;
    int count;
    double value;
    UT_hash_handle hh;
} accumulator_t;

// aggregation functions
// max, min, sum, count, avg, ?
// filter function, windowing functions, etc

#endif
