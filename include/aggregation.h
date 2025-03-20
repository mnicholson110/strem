#ifndef strem_aggregation_h
#define strem_aggregation_h

#include "../include/output.h"
#include <json-c/json.h>
#include <stdbool.h>
#include <uthash.h>

typedef enum transform
{
    SUM,
    AVG,
    MAX,
    MIN,
    CONST
} transform_t;

typedef union accumulator_value {
    int num;
    double dub;
    const char *str;
    bool boolean;
} accumulator_value_t;

typedef struct accumulator
{
    const char *key;
    int count;
    int values_len;
    accumulator_value_t *values;
    UT_hash_handle hh;
} accumulator_t;

const char *serialize(accumulator_t *entry, kafka_output_t *output);

// aggregation functions
// max, min, sum, count, avg, ?
// filter function, windowing functions, etc

#endif
