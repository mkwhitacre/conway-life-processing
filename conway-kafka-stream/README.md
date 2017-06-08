# Purpose

The goal of this example is to show how to implement Conway's Game of Life on top of [Apache Kafka Streams](https://kafka.apache.org/documentation/streams)


# How To Run

This requires Kafka and Confluent's Schema Registry to run.  Follow the instructions on the project README on how to do this.

To run the example:

```
mvn clean install
```

The test supplies a few basic game setups to try out simply edit the test to try them out.

# It's Broken

So while this currently works for the "blinker" world it does not work for alternate boards due to a misinterpretation of windowing and aggregation along with follow up map functions.  Pull Requests are welcome for anyone with alternates of how to solve.