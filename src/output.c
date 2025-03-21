#include "../include/output.h"

kafka_output_t *initKafkaOutput()
{
    kafka_output_t *output = (kafka_output_t *)malloc(sizeof(kafka_output_t));

    output->producer = NULL;
    output->config = NULL;

    const char *bootstrap_servers = getenv("OUTPUT_BOOTSTRAP_SERVERS");
    if (bootstrap_servers == NULL || strlen(bootstrap_servers) == 0)
    {
        fprintf(stderr, "No OUTPUT_BOOTSTRAP_SERVERS specified. Using INPUT_BOOTSTRAP_SERVERS instead.\n");
        bootstrap_servers = getenv("INPUT_BOOTSTRAP_SERVERS");
        if (bootstrap_servers == NULL)
        {
            fprintf(stderr, "INPUT_BOOTSTRAP_SERVERS must be set.\n");
            exit(EXIT_FAILURE);
        }
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

    return output;
}

void produceMessage(kafka_output_t *output, strem_config_t *config, const char *key, const char *value)
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
        RD_KAFKA_V_TOPIC(config->output_topic),
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
    free(output);
    return;
}

const char *serialize(accumulator_t *entry, strem_config_t *config)
{
    json_object *obj = json_object_new_object();

    for (int i = 0; i < entry->values_len; ++i)
    {
        switch (config->output_types[i])
        {
        case json_type_int:
            json_object_object_add(obj, config->output_fields[i], json_object_new_int(entry->values[i].num));
            break;
        case json_type_double:
            json_object_object_add(obj, config->output_fields[i], json_object_new_double(entry->values[i].dub));
            break;
        case json_type_string:
            json_object_object_add(obj, config->output_fields[i], json_object_new_string(entry->values[i].str));
            break;
        default:
            break;
        }
    }

    json_object_object_add(obj, "count", json_object_new_int(entry->count));

    const char *json_str = json_object_to_json_string(obj);
    const char *res = strdup(json_str);

    json_object_put(obj);

    // printf("%s\n", res);
    return res;
}
