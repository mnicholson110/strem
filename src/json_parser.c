#include "../include/json_parser.h"

json_object *json_get_nested_value(json_object *root, const char *path)
{
    if (!root || !path)
    {
        return NULL;
    }

    char *path_copy = strdup(path);
    char *token;
    char *saveptr;
    json_object *current = root;

    token = strtok_r(path_copy, "/", &saveptr);
    while (token != NULL)
    {
        if (json_object_get_type(current) == json_type_string)
        {
            const char *str = json_object_get_string(current);
            if (str && (str[0] == '{' || str[0] == '['))
            {
                json_object *next = json_tokener_parse(str);
                if (next)
                {
                    current = next;
                }
                else
                {
                    free(path_copy);
                    return NULL;
                }
            }
        }

        if (json_object_get_type(current) != json_type_object)
        {
            free(path_copy);
            return NULL;
        }

        json_object *child = NULL;

        if (!json_object_object_get_ex(current, token, &child))
        {
            free(path_copy);
            return NULL;
        }

        current = child;
        token = strtok_r(NULL, "/", &saveptr);
    }
    free(path_copy);
    return current;
}

int get_int_value(json_object *root, const char *path)
{
    json_object *value = json_get_nested_value(root, path);
    if (!value)
    {
        return 0;
    }
    if (json_object_get_type(value) == json_type_int)
    {
        return json_object_get_int(value);
    }
    return 0;
}

double get_double_value(json_object *root, const char *path)
{
    json_object *value = json_get_nested_value(root, path);
    if (!value)
    {
        return 0.0;
    }
    if (json_object_get_type(value) == json_type_double)
    {
        return json_object_get_double(value);
    }
    else if (json_object_get_type(value) == json_type_int)
    {
        return (double)json_object_get_int(value);
    }
    return 0.0;
}

const char *get_string_value(json_object *root, const char *path)
{
    json_object *value = json_get_nested_value(root, path);
    if (!value)
    {
        return NULL;
    }
    if (json_object_get_type(value) == json_type_string)
    {
        return json_object_get_string(value);
    }
    return json_object_to_json_string(value);
}
