#include "../include/aggregation.h"

const char *serialize(accumulator_t *entry)
{
    json_object *obj = json_object_new_object();

    // json_object_object_add(obj, "store_id", json_object_new_string(entry->key));
    // json_object_object_add(obj, "count", json_object_new_int(entry->count));
    // json_object_object_add(obj, "total_order_amount", json_object_new_double(entry->value));

    const char *json_str = json_object_to_json_string(obj);
    const char *res = strdup(json_str);

    json_object_put(obj);

    return res;
}

accumulator_t *deserialize(json_object *obj)
{
    // TODO
    return NULL;
}

accumulator_t *sum(accumulator_t *entry, double value)
{
    // TODO
    return NULL;
}
