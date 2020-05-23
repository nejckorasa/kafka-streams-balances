#!/bin/bash
set -e
PARTITIONS="3"
REPL_FACTOR="1"
ZOOKEEPER="zookeeper:2181"

echo "Waiting for Kafka to be ready..."
cub kafka-ready -b broker:29092 1 200
echo "Start creating topics"

input="/topics/topics.txt"
while IFS= read -r line; do
  echo "Creating if-not-exists topic: $line"
  kafka-topics --create --if-not-exists --zookeeper $ZOOKEEPER --partitions $PARTITIONS --replication-factor $REPL_FACTOR --topic $line
done <"$input"
echo "Topics created"