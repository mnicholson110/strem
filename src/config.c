#include "../include/config.h"

strem_config_t *loadConfig(void)
{
    strem_config_t *config = (strem_config_t *)malloc(sizeof(strem_config_t));

    const char *input_bootstrap_servers = getenv("INPUT_BOOTSTRAP_SERVERS");
    if (input_bootstrap_servers == NULL)
    {
        fprintf(stderr, "INPUT_BOOTSTRAP_SERVERS must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        config->input_bootstrap_servers = strdup(input_bootstrap_servers);
    };

    const char *input_group_id = getenv("INPUT_GROUP_ID");
    if (input_group_id == NULL)
    {
        fprintf(stderr, "INPUT_GROUP_ID must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        config->input_group_id = strdup(input_group_id);
    };

    const char *input_auto_offset_reset = getenv("INPUT_AUTO_OFFSET_RESET");
    if (input_auto_offset_reset == NULL)
    {
        fprintf(stderr, "INPUT_AUTO_OFFSET_RESET must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        config->input_auto_offset_reset = strdup(input_auto_offset_reset);
    };

    const char *input_topic = getenv("INPUT_TOPIC");
    if (input_topic == NULL)
    {
        fprintf(stderr, "INPUT_TOPIC must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        input_topic = strdup(input_topic);
    };

    const char *output_bootstrap_servers = getenv("OUTPUT_BOOTSTRAP_SERVERS");
    if (output_bootstrap_servers == NULL)
    {
        fprintf(stdout, "No OUTPUT_BOOTSTRAP_SERVERS specified. Using INPUT_BOOTSTRAP_SERVERS instead.\n");
        config->output_bootstrap_servers = config->input_bootstrap_servers;
    }
    else
    {
        config->output_bootstrap_servers = strdup(output_bootstrap_servers);
    }

    const char *output_topic = getenv("OUTPUT_TOPIC");
    if (output_topic == NULL)
    {
        fprintf(stderr, "OUTPUT_TOPIC must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        config->output_topic = strdup(output_topic);
    };

    const char *output_key = getenv("OUTPUT_KEY");
    if (output_key == NULL)
    {
        fprintf(stdout, "No OUTPUT_KEY specified. Did you mean to do this?\n");
    }
    else
    {
        config->output_key = strdup(output_key);
    }

    const char *input_fields = getenv("INPUT_FIELDS");
    if (input_fields == NULL)
    {
        fprintf(stderr, "INPUT_FIELDS must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        char *fields_copy = strdup(input_fields);
        char *token, *saveptr;
        token = strtok_r(fields_copy, ":", &saveptr);
        int idx = 0;
        while (token != NULL)
        {
            config->input_fields[idx] = strdup(token);
            token = strtok_r(NULL, ":", &saveptr);
            idx++;
        }
        config->input_fields_len = idx;
        free(fields_copy);
    }

    const char *transforms = getenv("TRANSFORMS");
    if (transforms == NULL)
    {
        fprintf(stderr, "TRANSFORMS must be set.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        char *transforms_copy = strdup(input_fields);
        char *token, *saveptr;
        token = strtok_r(transforms_copy, ":", &saveptr);
        int idx = 0;
        while (token != NULL)
        {
            config->transforms[idx] = strdup(token);
            token = strtok_r(NULL, ":", &saveptr);
            idx++;
        }
        free(transforms_copy);
    }

    return config;
}

void freeConfig(strem_config_t *config)
{
    free((void *)config->input_bootstrap_servers);
    free((void *)config->input_group_id);
    free((void *)config->input_auto_offset_reset);
    free((void *)config->input_topic);
    free((void *)config->output_bootstrap_servers);
    free((void *)config->output_topic);
    for (int i = 0; i < config->input_fields_len; i++)
    {
        free((void *)config->input_fields[i]);
        free((void *)config->transforms[i]);
    }
}
