#!/bin/bash

export INPUT_BOOTSTRAP_SERVERS="localhost:9092"
export INPUT_GROUP_ID="new_group"
export INPUT_AUTO_OFFSET_RESET="latest"
export INPUT_TOPIC="order_db.order_schema.order"
export INPUT_FIELDS="/data/order/order_amount"
export FILTER_ON_FIELDS="/data/order/order_status"
export FILTER_ON_TYPES="eq"
export FILTER_ON_VALUES="Delivered"
export OUTPUT_TOPIC="new_topic"
export OUTPUT_KEY="/data/store/store_id"
export OUTPUT_FIELDS="total_order_amount"

cd .. && make && make run
