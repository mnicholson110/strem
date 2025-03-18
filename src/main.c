#include <json-c/json_object.h>
#include <signal.h>
#include <stdbool.h>
#include <stdlib.h>
#include <uthash.h>

#include "../include/aggregation.h"
#include "../include/input.h"
#include "../include/json_parser.h"
#include "../include/output.h"

volatile sig_atomic_t run = true;

void sigterm_handle(int sig)
{
    run = false;
}

int main()
{
    signal(SIGINT, sigterm_handle);
    signal(SIGTERM, sigterm_handle);

    kafka_input_t *input = initKafkaInput();
    kafka_output_t *output = initKafkaOutput();

    char *message = NULL;
    accumulator_t *state = NULL;
    accumulator_t *entry = NULL;
    const char *out_message = NULL;
    bool set_output_types = false;
    bool filter;

    while (run)
    {
        filter = false;

        message = pollMessage(input);

        if (!message)
        {
            continue;
        }

        json_object *root_obj = json_tokener_parse(message);
        if (root_obj)
        {
            if (!set_output_types)
            {
                output->output_types = malloc(sizeof(json_type *) * input->input_fields_len);
                input->filter_on_values = malloc(sizeof(accumulator_value_t *) * input->filter_on_fields_len);
            }
            const char *key = jsonGetCValue(const char *, root_obj, output->output_key);
            entry = NULL;
            HASH_FIND_STR(state, key, entry);
            if (!entry)
            {
                entry = (accumulator_t *)malloc(sizeof(accumulator_t));
                entry->key = key;
                entry->count = 1;
                entry->values = malloc(sizeof(accumulator_value_t) * input->input_fields_len);
                entry->values_len = input->input_fields_len;

                if (!set_output_types)
                {
                    const char *filter_on_values = getenv("FILTER_ON_VALUES");
                    if (filter_on_values != NULL && strlen(filter_on_values) > 0)
                    {
                        char *tmp_filter_on_values = strdup(filter_on_values);
                        char *saveptr = NULL;
                        char *token = strtok_r(tmp_filter_on_values, ":", &saveptr);
                        for (int i = 0; i < input->filter_on_fields_len; ++i)
                        {
                            json_object *filter_target = jsonGetNestedValue(root_obj, input->filter_on_fields[i]);
                            switch (json_object_get_type(filter_target))
                            {
                            case json_type_int:
                                input->filter_on_values[i].num = atoi(token);
                                break;
                            case json_type_double:
                                input->filter_on_values[i].dub = atof(token);
                                break;
                            case json_type_string:
                                input->filter_on_values[i].str = strdup(token);
                                break;
                            default:
                                break;
                            }
                            token = strtok_r(NULL, ":", &saveptr);
                        }
                        free(tmp_filter_on_values);
                    }
                }

                // apply filters
                for (int i = 0; i < input->filter_on_fields_len; ++i)
                {
                    json_object *filter_target = jsonGetNestedValue(root_obj, input->filter_on_fields[i]);
                    switch (json_object_get_type(filter_target))
                    {
                    case json_type_int:
                        // TODO
                        break;
                    case json_type_double:
                        // TODO
                        break;
                    case json_type_string:
                        switch (input->filter_on_types[i])
                        {
                        case EQ:
                            if (strcmp(input->filter_on_values[i].str, json_object_get_string(filter_target)) != 0)
                            {
                                filter = true;
                            }
                        default:
                            break;
                        }
                        break;
                    default:
                        break;
                    }
                }
                if (!filter)
                {
                    for (int i = 0; i < input->input_fields_len; ++i)
                    {
                        json_object *target = jsonGetNestedValue(root_obj, input->input_fields[i]);
                        switch (json_object_get_type(target))
                        {
                        case json_type_int:
                            entry->values[i].num = json_object_get_int(target);
                            if (!set_output_types)
                            {
                                output->output_types[i] = json_type_int;
                            }
                            break;
                        case json_type_double:
                            entry->values[i].dub = json_object_get_double(target);
                            if (!set_output_types)
                            {
                                output->output_types[i] = json_type_double;
                            }
                            break;
                        case json_type_string:
                            entry->values[i].str = strdup(json_object_get_string(target));
                            if (!set_output_types)
                            {
                                output->output_types[i] = json_type_string;
                            }
                            break;
                        default:
                            break;
                        }
                        json_object_put(target);
                    }
                    if (!set_output_types)
                    {
                        set_output_types = true;
                    }
                    HASH_ADD_STR(state, key, entry);
                }
                else
                {
                    for (int i = 0; i < input->input_fields_len; ++i)
                    {
                        json_object *target = jsonGetNestedValue(root_obj, input->input_fields[i]);
                        switch (json_object_get_type(target))
                        {
                        case json_type_int:
                            entry->values[i].num += json_object_get_int(target);
                            break;
                        case json_type_double:
                            entry->values[i].dub += json_object_get_double(target);
                            break;
                        case json_type_string:
                            break;
                        default:
                            break;
                        }
                        json_object_put(target);
                    }
                    entry->count++;
                }
                out_message = serialize(entry, output);
                produceMessage(output, entry->key, out_message);
                free((void *)out_message);
            }
            json_object_put(root_obj);
        }
        else
        {
            fprintf(stdout, "No root_obj\n");
            continue;
        }
        free(message);
    }

    accumulator_t *tmp;
    entry = NULL;
    HASH_ITER(hh, state, entry, tmp)
    {
        HASH_DEL(state, entry);
        free(entry);
    }

    freeKafkaInput(input);
    freeKafkaOutput(output);

    return 0;
}
