#ifndef strem_json_parser_h
#define strem_json_parser_h

#include <json-c/json.h>
#include <string.h>

#define getCValue(type, obj, path) _Generic(((type *)0), \
    int *: get_int_value,                                \
    double *: get_double_value,                          \
    const char **: get_string_value)(obj, path)

struct json_object *json_get_nested_value(struct json_object *root, const char *path);
int get_int_value(struct json_object *root, const char *path);
double get_double_value(struct json_object *root, const char *path);
const char *get_string_value(struct json_object *root, const char *path);

#endif
