#!/bin/bash
echo "Follow output topic - balance-adjusted"
kafkacat -C -b localhost:9092 -t balance-adjusted -o beginning