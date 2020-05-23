#!/bin/bash
echo "Prune the volumes referenced in the docker-compose.yml"
docker volume rm kafka-streams-kotlin-master_kafka-v-data
docker volume rm kafka-streams-kotlin-master_zookeeper-v-data
docker volume rm kafka-streams-kotlin-master_zookeeper-v-logs
