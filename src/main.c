#include "../include/config.h"
#include "../include/input.h"
#include "../include/json_parser.h"
#include "../include/output.h"

void initOutputTypesAndFilters(strem_config_t *config, json_object *root_obj);

volatile sig_atomic_t run = true;

void sigterm_handle(int sig)
{
    run = false;
}

int main()
{
    signal(SIGINT, sigterm_handle);
    signal(SIGTERM, sigterm_handle);

    strem_config_t *config = initConfig();

    kafka_input_t *input = initKafkaInput(config);
    kafka_output_t *output = initKafkaOutput();

    char *message = NULL;
    accumulator_t *state = NULL;
    accumulator_t *entry = NULL;
    const char *out_message = NULL;
    bool first_message = true;
    bool filter = false;

    while (run)
    {
        message = pollMessage(input);

        if (!message)
        {
            continue;
        }

        json_object *root_obj = json_tokener_parse(message);
        if (root_obj)
        {
            if (first_message)
            {
                initOutputTypesAndFilters(config, root_obj);
                first_message = false;
            }

            filter = applyFilter(root_obj, config);

            if (filter)
            {
                entry = NULL;
                const char *key = jsonGetCValue(const char *, root_obj, config->output_key);
                HASH_FIND_STR(state, key, entry);
                if (!entry)
                {
                    entry = (accumulator_t *)malloc(sizeof(accumulator_t));
                    entry->key = key;
                    entry->count = 1;
                    entry->values = malloc(sizeof(accumulator_value_t) * config->input_fields_len);
                    entry->values_len = config->input_fields_len;
                    for (int i = 0; i < config->input_fields_len; ++i)
                    {
                        json_object *target = jsonGetNestedValue(root_obj, config->input_fields[i]);
                        switch (json_object_get_type(target))
                        {
                        case json_type_int:
                            entry->values[i].num = json_object_get_int(target);
                            break;
                        case json_type_double:
                            entry->values[i].dub = json_object_get_double(target);
                            break;
                        case json_type_string:
                            entry->values[i].str = strdup(json_object_get_string(target));
                            break;
                        default:
                            break;
                        }
                        json_object_put(target);
                    }
                    HASH_ADD_STR(state, key, entry);
                }
                else
                {
                    // this should be substituted with transformation logic
                    for (int i = 0; i < config->input_fields_len; ++i)
                    {
                        json_object *target = jsonGetNestedValue(root_obj, config->input_fields[i]);
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
                out_message = serialize(entry, config);
                produceMessage(output, config, entry->key, out_message);
                free((void *)out_message);
            }
            json_object_put(root_obj);
        }
        else
        {
            fprintf(stderr, "No root_obj\n");
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

    freeConfig(config);
    freeKafkaInput(input);
    freeKafkaOutput(output);

    return 0;
}

void initOutputTypesAndFilters(strem_config_t *config, json_object *root_obj)
{
    config->output_types = malloc(sizeof(json_type *) * config->input_fields_len);
    config->filter_on_values = malloc(sizeof(accumulator_value_t *) * config->filter_on_fields_len);

    const char *filter_on_values = getenv("FILTER_ON_VALUES");
    if (filter_on_values != NULL && strlen(filter_on_values) > 0)
    {
        char *tmp_filter_on_values = strdup(filter_on_values);
        char *saveptr = NULL;
        char *token = strtok_r(tmp_filter_on_values, ":", &saveptr);

        for (int i = 0; i < config->filter_on_fields_len; ++i)
        {
            json_object *filter_target = jsonGetNestedValue(root_obj, config->filter_on_fields[i]);

            switch (json_object_get_type(filter_target))
            {
            case json_type_int:
                config->filter_on_values[i].num = atoi(token);
                break;
            case json_type_double:
                config->filter_on_values[i].dub = atof(token);
                break;
            case json_type_string:
                config->filter_on_values[i].str = strdup(token);
                break;
            default:
                break;
            }
            token = strtok_r(NULL, ":", &saveptr);
        }
        free(tmp_filter_on_values);
    }

    for (int i = 0; i < config->input_fields_len; ++i)
    {
        json_object *target = jsonGetNestedValue(root_obj, config->input_fields[i]);
        switch (json_object_get_type(target))
        {
        case json_type_int:
            config->output_types[i] = json_type_int;
            break;
        case json_type_double:
            config->output_types[i] = json_type_double;
            break;
        case json_type_string:
            config->output_types[i] = json_type_string;
            break;
        default:
            break;
        }
        json_object_put(target);
    }
}
