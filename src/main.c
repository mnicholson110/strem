#include <cjson/cJSON.h>
#include <stdlib.h>
#include <string.h>

#include "../include/consumer.h"

typedef struct
{
    int store_id;
    int order_count;
    double total_amount;
} store_aggregate_t;

static store_aggregate_t store_aggregates[1000];
static int num_stores = 0;

static void update_store_aggregate(int store_id, double order_amount);

int main()
{

    kafka_input_t input = initKafkaInput("localhost:9092", "test_group", "latest");

    if (!kafkaInputSubscribe(&input, "order_db.order_schema.order"))
    {
        exit(EXIT_FAILURE);
    }

    while (1)
    {
        rd_kafka_message_t *consumer_message;
        consumer_message = rd_kafka_consumer_poll(input.consumer, 500);
        if (!consumer_message)
        {
            fprintf(stdout, "Waiting...\n");
            continue;
        }

        if (consumer_message->err)
        {
            if (consumer_message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
            {
                // no messages right now
            }
            else
            {
                fprintf(stderr, "Consumer error: %s\n", rd_kafka_message_errstr(consumer_message));
            }
            rd_kafka_message_destroy(consumer_message);
            continue;
        }

        char *raw_json = (char *)consumer_message->payload;

        cJSON *root = cJSON_Parse(raw_json);
        if (root)
        {

            cJSON *data_obj = cJSON_Parse(cJSON_GetObjectItem(root, "data")->valuestring);
            if (data_obj)
            {

                cJSON *order_obj = cJSON_GetObjectItem(data_obj, "order");
                cJSON *store_obj = cJSON_GetObjectItem(data_obj, "store");
                if (order_obj && store_obj)
                {
                    fprintf(stdout, "Order and Store found\n");

                    cJSON *status_item = cJSON_GetObjectItem(order_obj, "order_status");
                    cJSON *amount_item = cJSON_GetObjectItem(order_obj, "order_amount");
                    cJSON *store_id_item = cJSON_GetObjectItem(store_obj, "store_id");
                    if (status_item && amount_item && store_id_item)
                    {
                        fprintf(stdout, "Record found\n");
                        const char *order_status = status_item->valuestring;
                        double order_amount = amount_item->valuedouble;
                        int store_id = store_id_item->valueint;

                        if (strcmp(order_status, "Delivered") == 0)
                        {
                            update_store_aggregate(store_id, order_amount);
                        }
                    }
                }
            }
            cJSON_Delete(root);
        }
        else
        {
            fprintf(stderr, "Failed to parse JSON.\n");
        }

        for (int i = 0; i < num_stores; i++)
        {
            if (store_aggregates[i].order_count > 0)
            {

                fprintf(stdout, "\nCurrent Aggregates:\n");
                fprintf(stdout, "Store %d -> Count: %d, Total: %.2f\n",
                        store_aggregates[i].store_id,
                        store_aggregates[i].order_count,
                        store_aggregates[i].total_amount);
            }
        }

        rd_kafka_message_destroy(consumer_message);
    }

    // Close the consumer gracefully.
    fprintf(stdout, "Closing consumer\n");
    rd_kafka_consumer_close(input.consumer);
    rd_kafka_destroy(input.consumer);

    return 0;
}

static void update_store_aggregate(int store_id, double order_amount)
{
    for (int i = 0; i < num_stores; i++)
    {
        if (store_aggregates[i].store_id == store_id)
        {
            store_aggregates[i].order_count += 1;
            store_aggregates[i].total_amount += order_amount;
            return;
        }
    }

    if (num_stores < 1000)
    {
        store_aggregates[num_stores].store_id = store_id;
        store_aggregates[num_stores].order_count = 1;
        store_aggregates[num_stores].total_amount = order_amount;
        num_stores++;
    }
    else
    {
        fprintf(stderr, "Maximum number of store aggregates reached.\n");
    }
}
