#!/bin/bash

export INPUT_BOOTSTRAP_SERVERS="your_input_bootstrap_servers"
export INPUT_GROUP_ID="your_input_group_id"
export INPUT_AUTO_OFFSET_RESET="your_input_auto_offset_reset"
export INPUT_TOPIC="your_input_topic"
export OUTPUT_BOOTSTRAP_SERVERS="your_output_bootstrap_servers"
export OUTPUT_TOPIC="your_output_topic"
export OUTPUT_KEY="your_output_key"

cd .. && make run
