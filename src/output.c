#include "../include/output.h"

kafka_output_t *initKafkaOutput()
{
    kafka_output_t *output = (kafka_output_t *)malloc(sizeof(kafka_output_t));

    const char *bootstrap_servers = getenv("OUTPUT_BOOTSTRAP_SERVERS");
    if (bootstrap_servers == NULL || strlen(bootstrap_servers) == 0)
    {
        fprintf(stdout, "No OUTPUT_BOOTSTRAP_SERVERS specified. Using INPUT_BOOTSTRAP_SERVERS instead.\n");
        bootstrap_servers = getenv("INPUT_BOOTSTRAP_SERVERS");
        if (bootstrap_servers == NULL)
        {
            fprintf(stderr, "INPUT_BOOTSTRAP_SERVERS must be set.\n");
            exit(EXIT_FAILURE);
        }
    }

    const char *output_topic = getenv("OUTPUT_TOPIC");
    if (output_topic == NULL)
    {
        fprintf(stderr, "OUTPUT_TOPIC must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        output->output_topic = strdup(output_topic);
    }

    const char *output_key = getenv("OUTPUT_KEY");
    if (output_key == NULL)
    {
        fprintf(stderr, "OUTPUT_KEY must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        output->output_key = strdup(output_key);
    }

    const char *output_fields = getenv("OUTPUT_FIELDS");
    if (output_fields == NULL)
    {
        fprintf(stderr, "OUTPUT_FIELDS must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        output->output_fields = NULL;
        output->output_fields_len = 0;
        char *tmp_output_fields = strdup(output_fields);
        char *saveptr = NULL;
        char *token = strtok_r(tmp_output_fields, ":", &saveptr);
        while (token != NULL)
        {
            const char **tokens = realloc(output->output_fields, sizeof(char *) * (output->output_fields_len + 1));
            output->output_fields = tokens;
            output->output_fields[output->output_fields_len] = strdup(token);
            output->output_fields_len++;
            token = strtok_r(NULL, ":", &saveptr);
        }
        free(tmp_output_fields);
    }

    // need to add error handling here
    output->config = rd_kafka_conf_new();
    rd_kafka_conf_set(output->config, "bootstrap.servers", bootstrap_servers, output->errstr, sizeof(output->errstr));
    output->producer = rd_kafka_new(RD_KAFKA_PRODUCER, output->config, output->errstr, sizeof(output->errstr));

    if (!output->producer)
    {
        fprintf(stderr, "Failed to create producer: %s\n", output->errstr);
        exit(EXIT_FAILURE);
    }

    fprintf(stdout, "OUTPUT_BOOTSTRAP_SERVERS: %s\n", bootstrap_servers);
    fprintf(stdout, "OUTPUT_TOPIC: %s\n", output->output_topic);
    fprintf(stdout, "OUTPUT_KEY: %s\n", output->output_key);

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
        RD_KAFKA_V_VALUE((void *)value, len),
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

void freeKafkaOutput(kafka_output_t *output)
{
    rd_kafka_flush(output->producer, 1000);
    rd_kafka_destroy(output->producer);
    free((void *)output->output_topic);
    for (int i = 0; i < output->output_fields_len; ++i)
    {
        free((void *)output->output_fields[i]);
    }
    free(output);
    return;
}
