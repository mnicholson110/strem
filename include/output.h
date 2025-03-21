#ifndef strem_kafka_output_h
#define strem_kafka_output_h

#include "config.h"

typedef struct kafka_output
{
    rd_kafka_t *producer;
    rd_kafka_conf_t *config;
    rd_kafka_resp_err_t error;
    char errstr[512];
} kafka_output_t;

kafka_output_t *initKafkaOutput();
void produceMessage(kafka_output_t *output, strem_config_t *config, const char *key, const char *value);
void freeKafkaOutput(kafka_output_t *output);

typedef enum transform
{
    SUM,
    AVG,
    MAX,
    MIN,
    CONST
} transform_t;

const char *serialize(accumulator_t *entry, strem_config_t *config);
#endif
