#!/bin/bash
# Create topics needed for the application

declare -A connectors

connectors[sncf-unique-disruptions]=sncf-unique-disruptions
connectors[sncf-disruptions-computed-delays]=sncf-delays
connectors[disruption-stop-train-cause-stats]=sncf-stop-train-cause-stats
connectors[disruption-cause-stats]=sncf-cause-stats
connectors[disruption-cause-stats-per-day]=sncf-cause-stats-per-day
connectors[disruption-stop-stats]=sncf-stop-stats
connectors[disruption-stop-stats-per-day]=sncf-stop-stats-per-day
connectors[disruption-train-stats]=sncf-train-stats
connectors[disruption-train-stats-per-day]=sncf-train-stats-per-day
connectors[disruption-stop-train-stats]=sncf-stop-train-stats
connectors[disruption-stop-cause-stats]=sncf-stop-cause-stats
connectors[disruption-train-cause-stats]=sncf-train-cause-stats


for c in "${!connectors[@]}"; do
    ~/Documents/Kafka/confluent-5.2.1/bin/confluent load "$c" -d ./kafka-connectors-properties/"${connectors[$c]}".properties
done