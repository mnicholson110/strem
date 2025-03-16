#include "../include/consumer.h"

kafka_input_t *initKafkaInput(strem_config_t *config)
{
    kafka_input_t *input = (kafka_input_t *)malloc(sizeof(kafka_input_t));

    input->consumer = NULL;
    input->config = rd_kafka_conf_new();

    rd_kafka_conf_set(input->config, "bootstrap.servers", config->input_bootstrap_servers, input->errstr, sizeof(input->errstr));
    rd_kafka_conf_set(input->config, "group.id", config->input_group_id, input->errstr, sizeof(input->errstr));
    rd_kafka_conf_set(input->config, "auto.offset.reset", config->input_auto_offset_reset, input->errstr, sizeof(input->errstr));

    input->consumer = rd_kafka_new(RD_KAFKA_CONSUMER, input->config, input->errstr, sizeof(input->errstr));
    if (!input->consumer)
    {
        fprintf(stderr, "Failed to create consumer: %s\n", input->errstr);
        exit(EXIT_FAILURE);
    }

    rd_kafka_topic_partition_list_t *sub = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(sub, config->input_topic, RD_KAFKA_PARTITION_UA);

    input->error = rd_kafka_subscribe(input->consumer, sub);
    rd_kafka_topic_partition_list_destroy(sub);

    if (input->error)
    {
        fprintf(stderr, "Failed to subscribe to topic: %s\n", rd_kafka_err2str(input->error));
        rd_kafka_destroy(input->consumer);
        free((void *)input);
        return NULL;
    }

    return input;
}

char *pollMessage(kafka_input_t *input)
{
    rd_kafka_poll_set_consumer(input->consumer);

    rd_kafka_message_t *message;
    message = rd_kafka_consumer_poll(input->consumer, 500);

    if (!message)
    {
        return NULL;
    }

    if (message->err)
    {
        if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
        {
            fprintf(stdout, "End of partition reached\n");
        }
        else
        {
            fprintf(stderr, "Consumer error: %s\n", rd_kafka_message_errstr(message));
        }
        rd_kafka_message_destroy(message);
        return NULL;
    }

    char *payload_copy = strdup(message->payload);

    rd_kafka_message_destroy(message);
    return payload_copy;
}
