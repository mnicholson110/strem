#!/bin/bash

export INPUT_BOOTSTRAP_SERVERS=""
export INPUT_GROUP_ID=""
export INPUT_AUTO_OFFSET_RESET=""
export INPUT_TOPIC=""
export OUTPUT_BOOTSTRAP_SERVERS=""
export OUTPUT_TOPIC=""
export OUTPUT_KEY=""

cd .. && make run
