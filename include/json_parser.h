#ifndef strem_json_parser_h
#define strem_json_parser_h

#include <json-c/json.h>
#include <string.h>

#define jsonGetCValue(type, obj, path) _Generic(((type *)0), \
    int *: jsonGetIntValue,                                  \
    double *: jsonGetDoubleValue,                            \
    const char **: jsonGetStringValue)(obj, path)

struct json_object *jsonGetNestedValue(json_object *root, const char *path);
int jsonGetIntValue(json_object *root, const char *path);
double jsonGetDoubleValue(json_object *root, const char *path);
const char *jsonGetStringValue(json_object *root, const char *path);

#endif
