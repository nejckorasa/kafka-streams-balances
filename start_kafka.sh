#!/bin/bash
echo "Spin up Kafka - broker/zookeeper/proxy"
docker-compose -f docker-compose.yml up -d
cat .env