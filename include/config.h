#ifndef strem_config_h
#define strem_config_h

#include "../include/json_parser.h"
#include <librdkafka/rdkafka.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdlib.h>
#include <uthash.h>

#define FREE_PTR_ARRAY(ptr_array, len)    \
    do                                    \
    {                                     \
        for (int i = 0; i < (len); i++)   \
        {                                 \
            free((void *)(ptr_array)[i]); \
        }                                 \
    } while (0)

typedef enum filter_on_type
{
    EQ,
    GT,
    LT,
    GTE,
    LTE,
    NEQ
} filter_on_type_t;

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
    pthread_mutex_t lock;
    UT_hash_handle hh;
} accumulator_t;

extern pthread_rwlock_t state_lock;

typedef struct strem_config
{
    int input_fields_len;
    const char **input_fields;
    int filter_on_fields_len;
    const char **filter_on_fields;
    filter_on_type_t *filter_on_types;
    accumulator_value_t *filter_on_values;
    const char *output_topic;
    const char *output_key;
    int output_fields_len;
    const char **output_fields;
    json_type *output_types;
} strem_config_t;

strem_config_t *initConfig();
bool applyFilter(json_object *obj, strem_config_t *config);
void freeConfig(strem_config_t *config);

#endif
