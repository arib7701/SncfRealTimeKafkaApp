#!/bin/bash
# Create topics needed for the application

topics_to_create=(
    sncf-disruptions
    sncf-unique-disruptions
    disruption-stop-stats
    disruption-train-stats
    disruption-cause-stats
    disruption-stop-stats-per-day
    disruption-train-stats-per-day
    disruption-cause-stats-per-day
    disruption-stop-train-stats
    disruption-stop-cause-stats
    disruption-stop-train-cause-stats
    disruption-train-cause-stats
    sncf-disruptions-computed-delays)

for i in "${topics_to_create[@]}"; do
    sh ~/Documents/Kafka/confluent-5.2.1/bin/kafka-topics --create --topic "$i" --zookeeper localhost:2181 --partitions 3 --replication-factor 1
done