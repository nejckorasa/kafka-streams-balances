# Kafka Streams example app

Example app to demonstrate Kafka Streams with Kotlin.


## Kafka Streams Topology

Kafka Streams Topology is configured in [AdjustmentsStreams.kt](src/main/kotlin/tech/nejckorasa/kafka/balances/AdjustmentsStreams.kt):

```
+------------------+      +---------------+      +--------------------+
|                  |      |               |      |                    |
|  adjust-balance  +----->+  transformer  +----->+  balance-adjusted  |
|                  |      |               |      |                    |
+------------------+      +-+-------------+      +--------------------+
                            |
                            |
                          +-+-+
                          |   | balance-store
                          +---+
```

- It consumes records from `adjust-balance` topic,
- records flow downstream through a stateful transformer that persists the data in a `balance-store` state store,
- stream is materialized to a `balance-adjusted` topic.

> Adjustments Streams App will send a new record to `adjust-balance` every second. 


## Usage

#### Spin up Kafka

Only spin up Kafka components and run Adjustments Streams App outside docker-compose, i.e. you have to start it manually.

Script will also create topics defined in [topics.txt](topics/topics.txt)

```bash
./start_kafka.sh
``` 

#### Spin up everything

Spin up everything inside docker-compose:
 - Kafka configured in [docker-compose.yml](docker-compose.yml)
 - Adjustments Streams App configured in [adjustments-streams.yml](adjustments-streams.yml)

```bash
./start_all.sh
``` 

#### Stop everything

```bash
./stop_all.sh
``` 

#### Follow output topic

Follow `balance-adjusted` topic. 

```bash
./follow_output_topic.sh
``` 
