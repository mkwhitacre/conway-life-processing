h1. Conway's Game of Life





h1. Example Processing Engines



# Starting Up Kafka

A few of the examples rely on Kafka Running locally to test out dealing with data flowing in an unbounded format.
To make it easy to set this up we will rely on [Confluent's Quickstart](http://docs.confluent.io/3.2.0/quickstart.html) platform.  Specifically download the quickstart zip and extract it to a known location.  From there you can startup Kafka and the Schema Registry with the following command:

```
./startup_kafka.sh ~/confluent/confluent-3.2.0

```

This will start up three separate processes that are necessary for running:
* Zookeeper
* Kafka
* Confluent Schema Registry

You can check the status of them once started by looking in the following log files respectively:
* zk.out
* kafka.out
* schema.out

To shutdown right now simply kill the Java processes in the order:

* Schema Registry
* Kafka
* Zookeeper

One thing to note is that data will persist in Kafka between runs.  Therefore if you want to 
start with a clean slate you will need to remove that data.  If you are using the default server.properties
file for starting Kafka the data will be stored at "/tmp/kafka-logs".  Therefore it can be cleaned up with:


```
rm -rf /tmp/kafka-logs

```


