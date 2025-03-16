#include <json-c/json_object.h>
#include <signal.h>
#include <stdlib.h>
#include <uthash.h>

#include "../include/aggregation.h"
#include "../include/config.h"
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

    strem_config_t *config = loadConfig();

    kafka_input_t *input = initKafkaInput(config);
    kafka_output_t *output = initKafkaOutput(config);

    char *message = NULL;
    accumulator_t *state = NULL;
    accumulator_t *entry = NULL;
    // const char *out_message = NULL;

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
            const char *key = jsonGetCValue(const char *, root_obj, config->output_key);
            entry = NULL;
            HASH_FIND_STR(state, key, entry);
            if (!entry)
            {
                // deserialize the entire message into an accumulator_t
                // and add to hashmap
            }
            else
            {
                // update entry based on the transforms for each input_field
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

    rd_kafka_consumer_close(input->consumer);
    rd_kafka_destroy(input->consumer);
    rd_kafka_destroy(output->producer);
    freeConfig(config);
    free(input);
    free((void *)output->output_topic);
    free(output);

    return 0;
}
