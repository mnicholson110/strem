#ifndef strem_kafka_input_h
#define strem_kafka_input_h

#include "aggregation.h"
#include "json_parser.h"

#include <librdkafka/rdkafka.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef enum filter_on_type
{
    EQ,
    GT,
    LT,
    GTE,
    LTE,
    NEQ
} filter_on_type_t;

typedef struct kafka_input
{
    rd_kafka_t *consumer;
    rd_kafka_conf_t *config;
    rd_kafka_resp_err_t error;
    char errstr[512];
    int input_fields_len;
    const char **input_fields;
    int filter_on_fields_len;
    const char **filter_on_fields;
    filter_on_type_t *filter_on_types;
    accumulator_value_t *filter_on_values;
} kafka_input_t;

kafka_input_t *initKafkaInput();
char *pollMessage(kafka_input_t *input);
void freeKafkaInput(kafka_input_t *input);
bool applyFilter(json_object *obj, kafka_input_t *input);

#endif
