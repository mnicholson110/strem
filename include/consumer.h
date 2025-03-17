#ifndef strem_kafka_input_h
#define strem_kafka_input_h

#include <librdkafka/rdkafka.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef struct kafka_input
{
    rd_kafka_t *consumer;
    rd_kafka_conf_t *config;
    rd_kafka_resp_err_t error;
    char errstr[512];
    char *bootstrap_servers;
    char *group_id;
    char *auto_offset_reset;
    char *topic;
    // char **fields;
} kafka_input_t;

kafka_input_t *initKafkaInput();
char *pollMessage(kafka_input_t *input);

#endif
