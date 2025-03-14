#include "../include/consumer.h"
#include <string.h>

kafka_input_t initKafkaInput(const char *bootstrap_servers, const char *group_id, const char *auto_offset_reset)
{
    kafka_input_t input =
        {
            .consumer = NULL,
            .config = rd_kafka_conf_new(),
            .bootstrap_servers = bootstrap_servers,
            .group_id = group_id,
            .auto_offset_reset = auto_offset_reset,
        };

    rd_kafka_conf_set(input.config, "bootstrap.servers", input.bootstrap_servers, input.errstr, sizeof(input.errstr));
    rd_kafka_conf_set(input.config, "group.id", input.group_id, input.errstr, sizeof(input.errstr));
    rd_kafka_conf_set(input.config, "auto.offset.reset", input.auto_offset_reset, input.errstr, sizeof(input.errstr));

    input.consumer = rd_kafka_new(RD_KAFKA_CONSUMER, input.config, input.errstr, sizeof(input.errstr));
    if (!input.consumer)
    {
        fprintf(stderr, "Failed to create consumer: %s\n", input.errstr);
        exit(EXIT_FAILURE);
    }

    return input;
}

bool kafkaInputSubscribe(kafka_input_t *input, const char *topic)
{
    rd_kafka_topic_partition_list_t *sub = rd_kafka_topic_partition_list_new(3);
    rd_kafka_topic_partition_list_add(sub, topic, RD_KAFKA_PARTITION_UA);

    input->error = rd_kafka_subscribe(input->consumer, sub);
    rd_kafka_topic_partition_list_destroy(sub);

    if (input->error)
    {
        fprintf(stderr, "Failed to subscribe to topic: %s\n", rd_kafka_err2str(input->error));
        rd_kafka_destroy(input->consumer);
        return false;
    }

    return true;
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

    char *payload_copy = (char *)malloc(message->len + 1);
    if (!payload_copy)
    {
        fprintf(stderr, "Failed to allocate memory for message payload\n");
        rd_kafka_message_destroy(message);
        return NULL;
    }
    memcpy(payload_copy, message->payload, message->len);
    payload_copy[message->len] = '\0';

    rd_kafka_message_destroy(message);
    return payload_copy;
}
