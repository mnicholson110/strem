#include "../include/config.h"

strem_config_t *initConfig()
{
    strem_config_t *config = (strem_config_t *)malloc(sizeof(strem_config_t));

    config->input_fields = NULL;
    config->filter_on_fields = NULL;
    config->filter_on_types = NULL;
    config->filter_on_values = NULL;
    config->output_topic = NULL;
    config->output_key = NULL;
    config->output_fields = NULL;
    config->output_types = NULL;

    const char *input_fields = getenv("INPUT_FIELDS");
    if (input_fields == NULL)
    {
        fprintf(stderr, "INPUT_FIELDS must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        config->input_fields = NULL;
        config->input_fields_len = 0;
        char *tmp_input_fields = strdup(input_fields);
        char *saveptr = NULL;
        char *token = strtok_r(tmp_input_fields, ":", &saveptr);
        while (token != NULL)
        {
            const char **tokens = realloc(config->input_fields, sizeof(char *) * (config->input_fields_len + 1));
            config->input_fields = tokens;
            config->input_fields[config->input_fields_len] = strdup(token);
            config->input_fields_len++;
            token = strtok_r(NULL, ":", &saveptr);
        }
        free(tmp_input_fields);
    }

    const char *filter_on_fields = getenv("FILTER_ON_FIELDS");
    if (filter_on_fields != NULL && strlen(filter_on_fields) > 0)
    {
        config->filter_on_fields = NULL;
        config->filter_on_fields_len = 0;
        char *tmp_filter_on_fields = strdup(filter_on_fields);
        char *saveptr = NULL;
        char *token = strtok_r(tmp_filter_on_fields, ":", &saveptr);
        while (token != NULL)
        {
            const char **tokens = realloc(config->filter_on_fields, sizeof(char *) * (config->filter_on_fields_len + 1));
            config->filter_on_fields = tokens;
            config->filter_on_fields[config->filter_on_fields_len] = strdup(token);
            config->filter_on_fields_len++;
            token = strtok_r(NULL, ":", &saveptr);
        }
        free(tmp_filter_on_fields);
    }

    const char *filter_on_types = getenv("FILTER_ON_TYPES");
    if (filter_on_types != NULL && strlen(filter_on_types) > 0)
    {
        config->filter_on_types = malloc(sizeof(filter_on_type_t *) * config->filter_on_fields_len);
        char *tmp_filter_on_types = strdup(filter_on_types);
        char *saveptr = NULL;
        char *token = strtok_r(tmp_filter_on_types, ":", &saveptr);
        for (int i = 0; i < config->filter_on_fields_len; ++i)
        {
            if (strcmp(token, "eq") == 0 || strcmp(token, "EQ") == 0)
            {
                config->filter_on_types[i] = EQ;
            }
            else if (strcmp(token, "lt") == 0 || strcmp(token, "LT") == 0)
            {
                config->filter_on_types[i] = LT;
            }
            else if (strcmp(token, "gt") == 0 || strcmp(token, "GT") == 0)
            {
                config->filter_on_types[i] = GT;
            }
            else if (strcmp(token, "lte") == 0 || strcmp(token, "LTE") == 0)
            {
                config->filter_on_types[i] = LTE;
            }
            else if (strcmp(token, "gte") == 0 || strcmp(token, "GTE") == 0)
            {
                config->filter_on_types[i] = GTE;
            }
            else if (strcmp(token, "neq") == 0 || strcmp(token, "NEQ") == 0)
            {
                config->filter_on_types[i] = NEQ;
            }
            token = strtok_r(NULL, ":", &saveptr);
        }
        free(tmp_filter_on_types);
    }

    const char *output_topic = getenv("OUTPUT_TOPIC");
    if (output_topic == NULL)
    {
        fprintf(stderr, "OUTPUT_TOPIC must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        config->output_topic = strdup(output_topic);
    }

    const char *output_key = getenv("OUTPUT_KEY");
    if (output_key == NULL)
    {
        fprintf(stderr, "OUTPUT_KEY must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        config->output_key = strdup(output_key);
    }

    const char *output_fields = getenv("OUTPUT_FIELDS");
    if (output_fields == NULL)
    {
        fprintf(stderr, "OUTPUT_FIELDS must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        config->output_fields = NULL;
        config->output_fields_len = 0;
        char *tmp_output_fields = strdup(output_fields);
        char *saveptr = NULL;
        char *token = strtok_r(tmp_output_fields, ":", &saveptr);
        while (token != NULL)
        {
            const char **tokens = realloc(config->output_fields, sizeof(char *) * (config->output_fields_len + 1));
            config->output_fields = tokens;
            config->output_fields[config->output_fields_len] = strdup(token);
            config->output_fields_len++;
            token = strtok_r(NULL, ":", &saveptr);
        }
        free(tmp_output_fields);
    }

    return config;
}

bool applyFilter(json_object *obj, strem_config_t *config)
{
    for (int i = 0; i < config->filter_on_fields_len; ++i)
    {
        json_object *filter_target = jsonGetNestedValue(obj, config->filter_on_fields[i]);
        if (!filter_target)
        {
            continue;
        }

        switch (json_object_get_type(filter_target))
        {
        case json_type_boolean:
            switch (config->filter_on_types[i])
            {
            case EQ:
                if ((config->filter_on_values[i].boolean && !json_object_get_boolean(filter_target)) ||
                    (!config->filter_on_values[i].boolean && json_object_get_boolean(filter_target)))
                {
                    return false;
                }
                break;
            case NEQ:
                if ((config->filter_on_values[i].boolean && json_object_get_boolean(filter_target)) ||
                    (!config->filter_on_values[i].boolean && !json_object_get_boolean(filter_target)))
                {
                    return false;
                }
                break;
            default:
                break;
            }
            break;
        case json_type_int:
            switch (config->filter_on_types[i])
            {
            case EQ:
                if (json_object_get_int(filter_target) != config->filter_on_values[i].num)
                {
                    return false;
                }
                break;
            case NEQ:
                if (json_object_get_int(filter_target) == config->filter_on_values[i].num)
                {
                    return false;
                }
                break;
            case GT:
                if (json_object_get_int(filter_target) <= config->filter_on_values[i].num)
                {
                    return false;
                }
                break;
            case LT:
                if (json_object_get_int(filter_target) >= config->filter_on_values[i].num)
                {
                    return false;
                }
                break;
            case GTE:
                if (json_object_get_int(filter_target) < config->filter_on_values[i].num)
                {
                    return false;
                }
                break;
            case LTE:
                if (json_object_get_int(filter_target) > config->filter_on_values[i].num)
                {
                    return false;
                }
                break;
            default:
                break;
            }
            break;
        case json_type_double:
            switch (config->filter_on_types[i])
            {
            case EQ:
                if (json_object_get_double(filter_target) != config->filter_on_values[i].dub)
                {
                    return false;
                }
                break;
            case NEQ:
                if (json_object_get_double(filter_target) == config->filter_on_values[i].dub)
                {
                    return false;
                }
                break;
            case GT:
                if (json_object_get_double(filter_target) <= config->filter_on_values[i].dub)
                {
                    return false;
                }
                break;
            case LT:
                if (json_object_get_double(filter_target) >= config->filter_on_values[i].dub)
                {
                    return false;
                }
                break;
            case GTE:
                if (json_object_get_double(filter_target) < config->filter_on_values[i].dub)
                {
                    return false;
                }
                break;
            case LTE:
                if (json_object_get_double(filter_target) > config->filter_on_values[i].dub)
                {
                    return false;
                }
                break;
            default:
                break;
            }
            break;
        case json_type_string:
            switch (config->filter_on_types[i])
            {
            case EQ:
                if (strcmp(config->filter_on_values[i].str, json_object_get_string(filter_target)) != 0)
                {
                    return false;
                }
                break;
            case NEQ:
                if (strcmp(config->filter_on_values[i].str, json_object_get_string(filter_target)) == 0)
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

void freeConfig(strem_config_t *config)
{
    FREE_PTR_ARRAY(config->input_fields, config->input_fields_len);
    free(config->input_fields);
    FREE_PTR_ARRAY(config->filter_on_fields, config->filter_on_fields_len);
    free(config->filter_on_fields);
    free(config->filter_on_types);
    free(config->filter_on_values);
    free((void *)config->output_topic);
    free((void *)config->output_key);
    FREE_PTR_ARRAY(config->output_fields, config->output_fields_len);
    free(config->output_fields);
    free(config->output_types);
    free(config);
}
