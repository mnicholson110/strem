#include "../include/input.h"
#include <stdio.h>

kafka_input_t *initKafkaInput()
{
    kafka_input_t *input = (kafka_input_t *)malloc(sizeof(kafka_input_t));

    input->consumer = NULL;
    input->config = rd_kafka_conf_new();
    input->input_fields = NULL;
    input->filter_on_fields = NULL;
    input->filter_on_types = NULL;
    input->filter_on_values = NULL;

    const char *bootstrap_servers = getenv("INPUT_BOOTSTRAP_SERVERS");
    if (bootstrap_servers == NULL)
    {
        fprintf(stderr, "INPUT_BOOTSTRAP_SERVERS must be set.\n");
        exit(EXIT_FAILURE);
    }

    const char *group_id = getenv("INPUT_GROUP_ID");
    if (group_id == NULL)
    {
        fprintf(stderr, "INPUT_GROUP_ID must be set.\n");
        exit(EXIT_FAILURE);
    }

    const char *auto_offset_reset = getenv("INPUT_AUTO_OFFSET_RESET");
    if (auto_offset_reset == NULL)
    {
        fprintf(stderr, "INPUT_AUTO_OFFSET_RESET must be set.\n");
        exit(EXIT_FAILURE);
    }

    const char *input_topic = getenv("INPUT_TOPIC");
    if (input_topic == NULL)
    {
        fprintf(stderr, "INPUT_TOPIC must be set.\n");
        exit(EXIT_FAILURE);
    }

    rd_kafka_conf_set(input->config, "bootstrap.servers", bootstrap_servers, input->errstr, sizeof(input->errstr));
    rd_kafka_conf_set(input->config, "group.id", group_id, input->errstr, sizeof(input->errstr));
    rd_kafka_conf_set(input->config, "auto.offset.reset", auto_offset_reset, input->errstr, sizeof(input->errstr));

    input->consumer = rd_kafka_new(RD_KAFKA_CONSUMER, input->config, input->errstr, sizeof(input->errstr));
    if (!input->consumer)
    {
        fprintf(stderr, "Failed to create consumer: %s\n", input->errstr);
        exit(EXIT_FAILURE);
    }

    // parse input_fields (json pointers);
    const char *input_fields = getenv("INPUT_FIELDS");
    if (input_fields == NULL)
    {
        fprintf(stderr, "INPUT_FIELDS must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        input->input_fields = NULL;
        input->input_fields_len = 0;
        char *tmp_input_fields = strdup(input_fields);
        char *saveptr = NULL;
        char *token = strtok_r(tmp_input_fields, ":", &saveptr);
        while (token != NULL)
        {
            const char **tokens = realloc(input->input_fields, sizeof(char *) * (input->input_fields_len + 1));
            input->input_fields = tokens;
            input->input_fields[input->input_fields_len] = strdup(token);
            input->input_fields_len++;
            token = strtok_r(NULL, ":", &saveptr);
        }
        free(tmp_input_fields);
    }

    const char *filter_on_fields = getenv("FILTER_ON_FIELDS");
    if (filter_on_fields != NULL && strlen(filter_on_fields) > 0)
    {
        input->filter_on_fields = NULL;
        input->filter_on_fields_len = 0;
        char *tmp_filter_on_fields = strdup(filter_on_fields);
        char *saveptr = NULL;
        char *token = strtok_r(tmp_filter_on_fields, ":", &saveptr);
        while (token != NULL)
        {
            const char **tokens = realloc(input->filter_on_fields, sizeof(char *) * (input->filter_on_fields_len + 1));
            input->filter_on_fields = tokens;
            input->filter_on_fields[input->filter_on_fields_len] = strdup(token);
            input->filter_on_fields_len++;
            token = strtok_r(NULL, ":", &saveptr);
        }
        free(tmp_filter_on_fields);
    }

    const char *filter_on_types = getenv("FILTER_ON_TYPES");
    if (filter_on_types != NULL && strlen(filter_on_types) > 0)
    {
        input->filter_on_types = malloc(sizeof(filter_on_type_t *) * input->filter_on_fields_len);
        char *tmp_filter_on_types = strdup(filter_on_types);
        char *saveptr = NULL;
        char *token = strtok_r(tmp_filter_on_types, ":", &saveptr);
        for (int i = 0; i < input->filter_on_fields_len; ++i)
        {
            if (strcmp(token, "eq") == 0 || strcmp(token, "EQ") == 0)
            {
                input->filter_on_types[i] = EQ;
            }
            else if (strcmp(token, "lt") == 0 || strcmp(token, "LT") == 0)
            {
                input->filter_on_types[i] = LT;
            }
            else if (strcmp(token, "gt") == 0 || strcmp(token, "GT") == 0)
            {
                input->filter_on_types[i] = GT;
            }
            else if (strcmp(token, "lte") == 0 || strcmp(token, "LTE") == 0)
            {
                input->filter_on_types[i] = LTE;
            }
            else if (strcmp(token, "gte") == 0 || strcmp(token, "GTE") == 0)
            {
                input->filter_on_types[i] = GTE;
            }
            else if (strcmp(token, "neq") == 0 || strcmp(token, "NEQ") == 0)
            {
                input->filter_on_types[i] = NEQ;
            }
            token = strtok_r(NULL, ":", &saveptr);
        }
        free(tmp_filter_on_types);
    }

    rd_kafka_topic_partition_list_t *sub = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(sub, input_topic, RD_KAFKA_PARTITION_UA);

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

void freeKafkaInput(kafka_input_t *input)
{
    rd_kafka_consumer_close(input->consumer);
    rd_kafka_destroy(input->consumer);
    for (int i = 0; i < input->input_fields_len; ++i)
    {
        free((void *)input->input_fields[i]);
    }
    for (int i = 0; i < input->filter_on_fields_len; ++i)
    {
        free((void *)input->filter_on_fields[i]);
    }
    if (input->filter_on_values != NULL)
    {
        free((void *)input->filter_on_types);
        free((void *)input->filter_on_values);
    }
    free(input);
    return;
}

bool applyFilter(json_object *obj, kafka_input_t *input)
{
    for (int i = 0; i < input->filter_on_fields_len; ++i)
    {
        json_object *filter_target = jsonGetNestedValue(obj, input->filter_on_fields[i]);
        if (!filter_target)
        {
            continue;
        }

        switch (json_object_get_type(filter_target))
        {
        case json_type_boolean:
            switch (input->filter_on_types[i])
            {
            case EQ:
                if ((input->filter_on_values[i].boolean && !json_object_get_boolean(filter_target)) ||
                    (!input->filter_on_values[i].boolean && json_object_get_boolean(filter_target)))
                {
                    return false;
                }
                break;
            case NEQ:
                if ((input->filter_on_values[i].boolean && json_object_get_boolean(filter_target)) ||
                    (!input->filter_on_values[i].boolean && !json_object_get_boolean(filter_target)))
                {
                    return false;
                }
                break;
            default:
                break;
            }
            break;
        case json_type_int:
            switch (input->filter_on_types[i])
            {
            case EQ:
                if (json_object_get_int(filter_target) != input->filter_on_values[i].num)
                {
                    return false;
                }
                break;
            case NEQ:
                if (json_object_get_int(filter_target) == input->filter_on_values[i].num)
                {
                    return false;
                }
                break;
            case GT:
                if (json_object_get_int(filter_target) <= input->filter_on_values[i].num)
                {
                    return false;
                }
                break;
            case LT:
                if (json_object_get_int(filter_target) >= input->filter_on_values[i].num)
                {
                    return false;
                }
                break;
            case GTE:
                if (json_object_get_int(filter_target) < input->filter_on_values[i].num)
                {
                    return false;
                }
                break;
            case LTE:
                if (json_object_get_int(filter_target) > input->filter_on_values[i].num)
                {
                    return false;
                }
                break;
            default:
                break;
            }
            break;
        case json_type_double:
            switch (input->filter_on_types[i])
            {
            case EQ:
                if (json_object_get_double(filter_target) != input->filter_on_values[i].dub)
                {
                    return false;
                }
                break;
            case NEQ:
                if (json_object_get_double(filter_target) == input->filter_on_values[i].dub)
                {
                    return false;
                }
                break;
            case GT:
                if (json_object_get_double(filter_target) <= input->filter_on_values[i].dub)
                {
                    return false;
                }
                break;
            case LT:
                if (json_object_get_double(filter_target) >= input->filter_on_values[i].dub)
                {
                    return false;
                }
                break;
            case GTE:
                if (json_object_get_double(filter_target) < input->filter_on_values[i].dub)
                {
                    return false;
                }
                break;
            case LTE:
                if (json_object_get_double(filter_target) > input->filter_on_values[i].dub)
                {
                    return false;
                }
                break;
            default:
                break;
            }
            break;
        case json_type_string:
            switch (input->filter_on_types[i])
            {
            case EQ:
                if (strcmp(input->filter_on_values[i].str, json_object_get_string(filter_target)) != 0)
                {
                    return false;
                }
                break;
            case NEQ:
                if (strcmp(input->filter_on_values[i].str, json_object_get_string(filter_target)) == 0)
                {
                    return false;
                }
                break;
            default:
                break;
            }
            break;
        default:
            break;
        }
        json_object_put(filter_target);
    }
    return true;
}
