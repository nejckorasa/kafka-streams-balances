#!/bin/bash

[[ $1 == "rejected" ]] && topic="balance-adjustment-rejected" || topic="balance-adjustment"
echo "Follow topic ${topic}"

kafkacat -C -b localhost:9092 -t $topic -o beginning -q