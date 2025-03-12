#ifndef strem_kafka_input_h
#define strem_kafka_input_h

#include <librdkafka/rdkafka.h>
#include <stdbool.h>
#include <stdlib.h>

typedef struct kafka_input
{
    rd_kafka_t *consumer;
    rd_kafka_conf_t *config;
    rd_kafka_resp_err_t error;
    char errstr[512];
    const char *bootstrap_servers;
    const char *group_id;
    const char *auto_offset_reset;
} kafka_input_t;

kafka_input_t initKafkaInput(const char *bootstrap_servers, const char *group_id, const char *auto_offset_reset);
bool kafkaInputSubscribe(kafka_input_t *input, const char *topic);
char *pollMessage(kafka_input_t *input);

#endif
