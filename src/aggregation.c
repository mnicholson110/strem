#include "../include/aggregation.h"

const char *serialize(accumulator_t *entry, kafka_output_t *output)
{
    json_object *obj = json_object_new_object();

    for (int i = 0; i < entry->values_len; ++i)
    {
        switch (output->output_types[i])
        {
        case json_type_int:
            json_object_object_add(obj, output->output_fields[i], json_object_new_int(entry->values[i].num));
            break;
        case json_type_double:
            json_object_object_add(obj, output->output_fields[i], json_object_new_double(entry->values[i].dub));
            break;
        case json_type_string:
            json_object_object_add(obj, output->output_fields[i], json_object_new_string(entry->values[i].str));
            break;
        default:
            break;
        }
    }

    json_object_object_add(obj, "count", json_object_new_int(entry->count));

    const char *json_str = json_object_to_json_string(obj);
    const char *res = strdup(json_str);

    json_object_put(obj);

    // printf("%s\n", res);
    return res;
}
