#!/bin/bash

cd /pulsar/bin

./pulsar-admin namespaces create public/test10
./pulsar-admin namespaces set-is-allow-auto-update-schema --disable public/test10

## TOPICS
./pulsar-admin topics delete-partitioned-topic persistent://public/test10/topic-in
./pulsar-admin topics create-partitioned-topic --partitions 1 persistent://public/test10/topic-in
./pulsar-admin topics create-subscription persistent://public/test10/topic-in -s in-sub

./pulsar-admin topics delete-partitioned-topic persistent://public/test10/topic-out-1
./pulsar-admin topics create-partitioned-topic --partitions 1 persistent://public/test10/topic-out-1
./pulsar-admin topics create-subscription persistent://public/test10/topic-out-1 -s out-sub

./pulsar-admin topics delete-partitioned-topic persistent://public/test10/topic-out-2
./pulsar-admin topics create-partitioned-topic --partitions 1 persistent://public/test10/topic-out-2
./pulsar-admin topics create-subscription persistent://public/test10/topic-out-2 -s out-sub

## SCHEMAS
./pulsar-admin schemas delete persistent://public/test10/topic-in
./pulsar-admin schemas upload \
    --filename /pulsar/schemas/test10/TestMessage.json \
    persistent://public/test10/topic-in

./pulsar-admin schemas delete persistent://public/test10/topic-out-1
./pulsar-admin schemas upload \
    --filename /pulsar/schemas/test10/TestMessage.json \
    persistent://public/test10/topic-out-1

./pulsar-admin schemas delete persistent://public/test10/topic-out-2
./pulsar-admin schemas upload \
    --filename /pulsar/schemas/test10/TestMessage.json \
    persistent://public/test10/topic-out-2
