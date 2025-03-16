#ifndef strem_kafka_output_h
#define strem_kafka_output_h

#include "config.h"
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
} kafka_output_t;

kafka_output_t *initKafkaOutput(strem_config_t *config);
void produceMessage(kafka_output_t *output, const char *key, const char *value);

#endif
