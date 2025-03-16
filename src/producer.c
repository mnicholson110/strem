#include "../include/producer.h"

kafka_output_t *initKafkaOutput(strem_config_t *config)
{
    kafka_output_t *output = (kafka_output_t *)malloc(sizeof(kafka_output_t));

    // need to add error handling here
    rd_kafka_conf_set(output->config, "bootstrap.servers", config->output_bootstrap_servers, output->errstr, sizeof(output->errstr));
    output->producer = rd_kafka_new(RD_KAFKA_PRODUCER, output->config, output->errstr, sizeof(output->errstr));
    output->output_topic = strdup(config->output_topic);

    if (!output->producer)
    {
        fprintf(stderr, "Failed to create producer: %s\n", output->errstr);
        exit(EXIT_FAILURE);
    }

    return output;
}

void produceMessage(kafka_output_t *output, const char *key, const char *value)
{
    size_t len = strlen(value);
    if (len == 0)
    {
        rd_kafka_poll(output->producer, 0);
        return;
    }

    size_t key_len = strlen(key);

retry:
    output->error = rd_kafka_producev(
        output->producer,
        RD_KAFKA_V_TOPIC(output->output_topic),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_KEY(key, key_len),
        RD_KAFKA_V_VALUE(value, len),
        RD_KAFKA_V_OPAQUE(NULL),
        RD_KAFKA_V_END);

    if (output->error)
    {
        if (output->error == RD_KAFKA_RESP_ERR__QUEUE_FULL)
        {
            rd_kafka_poll(output->producer, 1000);
            goto retry;
        }
    }
    rd_kafka_poll(output->producer, 0);
}
