#ifndef strem_aggregation_h
#define strem_aggregation_h

#include <json-c/json.h>
#include <stdbool.h>
#include <uthash.h>

// this need to be more generic. char* key?
typedef struct
{
    const char *key;
    int count;
    double value;
    UT_hash_handle hh;
} accumulator_t;

const char *serialize(accumulator_t *entry);

// aggregation functions
// max, min, sum, count, avg, ?
// filter function, windowing functions, etc

#endif
