#ifndef strem_config_h
#define strem_config_h

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct strem_config
{
    const char *input_bootstrap_servers;
    const char *input_group_id;
    const char *input_auto_offset_reset;
    const char *input_topic;
    const char *output_bootstrap_servers;
    const char *output_topic;
    const char *output_key;
    const char **input_fields;
    int input_fields_len;
    const char **transforms;
    // output_fields
    // output_fields_len
    //  todo
    //  input_paralellism
} strem_config_t;

strem_config_t *loadConfig(void);
void freeConfig(strem_config_t *config);

#endif
