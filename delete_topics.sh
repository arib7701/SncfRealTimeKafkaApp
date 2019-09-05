#!/bin/bash
# Delete all topics of the application

topics_to_delete=(
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

for i in "${topics_to_delete[@]}"; do
    sh ~/Documents/Kafka/confluent-5.2.1/bin/kafka-topics --delete --topic "$i" --zookeeper localhost:2181
done