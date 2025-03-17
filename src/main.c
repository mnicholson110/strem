#include <json-c/json_object.h>
#include <signal.h>
#include <stdlib.h>
#include <uthash.h>

#include "../include/aggregation.h"
#include "../include/consumer.h"
#include "../include/json_parser.h"
#include "../include/producer.h"

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
            // check for existence in hashmap
            const char *key = jsonGetCValue(const char *, root_obj, output->output_key);
            entry = NULL;
            HASH_FIND_STR(state, key, entry);
            if (!entry)
            {
                // allocate new accumulator
                entry = (accumulator_t *)malloc(sizeof(accumulator_t));
                // set the key
                entry->key = key;
                // set the count
                entry->count = 1;
                // set each value
                entry->values = malloc(sizeof(accumulator_value_t) * input->input_fields_len);
                for (int i = 0; i < input->input_fields_len; ++i)
                {
                    json_object *target = jsonGetNestedValue(root_obj, input->input_fields[i]);
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
                }
                entry->count++;
            }
            fprintf(stdout, "Store_id: %s, order count: %d, total amount: %.2f, lat: %s, long: %s\n\n", entry->key, entry->count, entry->values[0].dub, entry->values[1].str, entry->values[2].str);
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

    printf("Cleaning up!\n");
    freeKafkaInput(input);
    freeKafkaOutput(output);

    return 0;
}
