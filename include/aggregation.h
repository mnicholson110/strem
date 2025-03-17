#ifndef strem_aggregation_h
#define strem_aggregation_h

#include <json-c/json.h>
#include <stdbool.h>
#include <uthash.h>

typedef enum transform
{
    SUM,
    AVG,
    MAX,
    MIN
} transform_t;

typedef union accumulator_value {
    int num;
    double dub;
    const char *str;
} accumulator_value_t;

typedef struct accumulator
{
    const char *key;
    int count;
    // is this needed?
    // const char **fields;
    accumulator_value_t *values;
    UT_hash_handle hh;
} accumulator_t;

const char *serialize(accumulator_t *entry);
accumulator_t *deserialize(json_object *object);
accumulator_t *sum(accumulator_t *entry, double value);

// aggregation functions
// max, min, sum, count, avg, ?
// filter function, windowing functions, etc

#endif
