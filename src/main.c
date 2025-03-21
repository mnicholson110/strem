#include "../include/config.h"
#include "../include/input.h"
#include "../include/json_parser.h"
#include "../include/output.h"

volatile sig_atomic_t run = true;

void sigterm_handle(int sig)
{
    run = false;
}

pthread_rwlock_t state_lock = PTHREAD_RWLOCK_INITIALIZER;

typedef struct thread_args
{
    strem_config_t *config;
    kafka_output_t *output;
    accumulator_t **state;
} thread_args_t;

void initOutputTypesAndFilters(strem_config_t *config, json_object *root_obj);
void *consumerThread(void *arg);

int main()
{
    signal(SIGINT, sigterm_handle);
    signal(SIGTERM, sigterm_handle);

    strem_config_t *config = initConfig();
    kafka_output_t *output = initKafkaOutput();
    accumulator_t *state = NULL;

    const int num_threads = 3;
    pthread_t threads[num_threads];
    thread_args_t args = {
        .config = config,
        .output = output,
        .state = &state};

    for (int i = 0; i < num_threads; ++i)
    {
        if (pthread_create(&threads[i], NULL, consumerThread, &args) != 0)
        {
            fprintf(stderr, "Failed to created thread %i\n", i);
            exit(EXIT_FAILURE);
        }
    }

    for (int i = 0; i < num_threads; ++i)
    {
        pthread_join(threads[i], NULL);
    }

    accumulator_t *tmp = NULL;
    accumulator_t *entry = NULL;
    HASH_ITER(hh, state, entry, tmp)
    {
        HASH_DEL(state, entry);
        free((void *)entry->key);
        free(entry->values);
        free(entry);
    }

    freeConfig(config);
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

void *consumerThread(void *arg)
{
    thread_args_t *args = (thread_args_t *)arg;

    kafka_input_t *input = initKafkaInput(args->config);
    char *message = NULL;
    accumulator_t *entry = NULL;
    const char *out_message = NULL;
    bool first_message = true;

    while (run)
    {
        message = pollMessage(input);

        if (!message)
        {
            continue;
        }

        json_object *root_obj = json_tokener_parse(message);
        if (!root_obj)
        {
            fprintf(stderr, "Failed to parse message: %s\n", message);
            free(message);
            continue;
        }

        if (first_message)
        {
            pthread_rwlock_wrlock(&state_lock);
            if (!args->config->output_types)
            {
                initOutputTypesAndFilters(args->config, root_obj);
            }
            pthread_rwlock_unlock(&state_lock);
            first_message = false;
        }

        if (applyFilter(root_obj, args->config))
        {
            const char *key = jsonGetStringValue(root_obj, args->config->output_key);
            if (!key)
            {
                continue;
            }

            pthread_rwlock_wrlock(&state_lock);
            HASH_FIND_STR(*args->state, key, entry);
            if (!entry)
            {
                entry = (accumulator_t *)malloc(sizeof(accumulator_t));
                entry->key = key;
                entry->count = 1;
                entry->values = malloc(sizeof(accumulator_value_t) * args->config->input_fields_len);
                entry->values_len = args->config->input_fields_len;
                for (int i = 0; i < args->config->input_fields_len; ++i)
                {
                    json_object *target = jsonGetNestedValue(root_obj, args->config->input_fields[i]);
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
                HASH_ADD_STR(*args->state, key, entry);
            }
            else
            {
                // pthread_mutex_lock(&entry->lock);
                // this should be substituted with transformation logic
                for (int i = 0; i < args->config->input_fields_len; ++i)
                {
                    json_object *target = jsonGetNestedValue(root_obj, args->config->input_fields[i]);
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
                // pthread_mutex_unlock(&entry->lock);
            }
            pthread_rwlock_unlock(&state_lock);

            pthread_mutex_lock(&entry->lock);
            out_message = serialize(entry, args->config);
            pthread_mutex_unlock(&entry->lock);
            produceMessage(args->output, args->config, entry->key, out_message);
            free((void *)out_message);
        }
        json_object_put(root_obj);
        free(message);
    }
    freeKafkaInput(input);
    return NULL;
}
