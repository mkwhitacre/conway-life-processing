#!/bin/sh

CONFLUENT_PATH=$1

#startup zk
echo "Starting Zookeeper"
nohup $CONFLUENT_PATH/bin/zookeeper-server-start $CONFLUENT_PATH/etc/kafka/zookeeper.properties > zk.out &

sleep 5

#startup kafka
echo "Starting Kafka"
nohup $CONFLUENT_PATH/bin/kafka-server-start $CONFLUENT_PATH/etc/kafka/server.properties > kafka.out &

sleep 5

# startup schema registry
echo "Starting Schema Registry"
nohup $CONFLUENT_PATH/bin/schema-registry-start $CONFLUENT_PATH/etc/schema-registry/schema-registry.properties > schema.out &


