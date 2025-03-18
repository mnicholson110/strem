#ifndef strem_kafka_output_h
#define strem_kafka_output_h

#include <json-c/json.h>
#include <librdkafka/rdkafka.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef struct kafka_output
{
    rd_kafka_t *producer;
    rd_kafka_conf_t *config;
    rd_kafka_resp_err_t error;
    char errstr[512];
    const char *output_topic;
    const char *output_key;
    int output_fields_len;
    const char **output_fields;
    json_type *output_types;
} kafka_output_t;

kafka_output_t *initKafkaOutput();
void produceMessage(kafka_output_t *output, const char *key, const char *value);
void freeKafkaOutput(kafka_output_t *output);

#endif
