#include "../include/aggregation.h"
#include <stdio.h>

const char *serialize(accumulator_t *entry, const char **field_names)
{
    json_object *obj = json_object_new_object();

    for (int i = 0; i < entry->values_len; ++i)
    {
        json_object_object_add(obj, field_names[i], json_object_new_double(entry->values[i].dub));
    }

    json_object_object_add(obj, "count", json_object_new_int(entry->count));

    const char *json_str = json_object_to_json_string(obj);
    const char *res = strdup(json_str);

    json_object_put(obj);

    // printf("%s\n", res);
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
