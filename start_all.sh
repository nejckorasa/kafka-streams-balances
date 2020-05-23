#!/bin/bash
echo "Spin up Kafka + Adjustments Streams app"

./gradlew build -x test

docker-compose -f docker-compose.yml up -d
docker wait kafka-setup
docker-compose -f docker-compose.yml -f adjustments-streams.yml up -d --build --no-recreate
cat .env