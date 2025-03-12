#include <stdlib.h>

#include "../include/aggregation.h"
#include "../include/consumer.h"
#include "../include/json_parser.h"

int main()
{

    kafka_input_t input = initKafkaInput("localhost:9092", "new_test_group", "earliest");

    if (!kafkaInputSubscribe(&input, "order_db.order_schema.order"))
    {
        exit(EXIT_FAILURE);
    }

    char *message;

    accumulator_t *state = NULL;

    while (1)
    {
        message = pollMessage(&input);

        if (!message)
        {
            fprintf(stdout, "No Message\n");
            continue;
        }

        json_object *root_obj = json_tokener_parse(message);
        if (root_obj)
        {
            const char *status = getCValue(const char *, root_obj, "data/order/order_status");
            if (strcmp(status, "Delivered") == 0)
            {
                int store_id = getCValue(int, root_obj, "data/store/store_id/");
                double order_amount = getCValue(double, root_obj, "data/order/order_amount/");

                accumulator_t *entry;
                HASH_FIND_INT(state, &store_id, entry);
                if (!entry)
                {
                    entry = (accumulator_t *)malloc(sizeof(accumulator_t));
                    if (!entry)
                    {
                        fprintf(stderr, "Memory allocation failed\n");
                        exit(EXIT_FAILURE);
                    }
                    entry->count = 1;
                    entry->key = store_id;
                    entry->value = order_amount;
                    HASH_ADD_INT(state, key, entry);
                }
                else
                {
                    entry->count++;
                    entry->value += order_amount;
                }
                fprintf(stdout, "Store: %d, Count: %d, Amount: %.2f\n\n", entry->key, entry->count, entry->value);
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

    rd_kafka_consumer_close(input.consumer);
    rd_kafka_destroy(input.consumer);

    return 0;
}
