# Kafka Streams example app

Example app to demonstrate Kafka Streams with Kotlin.

## Usage

### Spin up Kafka

Only spin up Kafka components and run Adjustments Streams App outside docker-compose, i.e. you have to start it manually.

```bash
./start_kafka.sh
``` 

### Spin up everything

Spin up everything inside docker-compose:
 - Kafka configured in `docker-compose.yml` 
 - Adjustments Streams App configured in `adjustments-streams.yml`

```bash
./start_all.sh
``` 

### Stop everything

```bash
./stop_all.sh
``` 

### Follow output topic

Follow `balance-adjusted` topic. 

```bash
./follow_output_topic.sh
``` 

### Follow output topic

Prune the volumes referenced in the `docker-compose.yml`.

```bash
./follow_output_topic.sh
``` 
