#ifndef strem_kafka_input_h
#define strem_kafka_input_h

#include "config.h"

typedef struct kafka_input
{
    rd_kafka_t *consumer;
    rd_kafka_conf_t *config;
    rd_kafka_resp_err_t error;
    char errstr[512];
} kafka_input_t;

kafka_input_t *initKafkaInput(strem_config_t *config);
char *pollMessage(kafka_input_t *input);
void freeKafkaInput(kafka_input_t *input);

#endif
