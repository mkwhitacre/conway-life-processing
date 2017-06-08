# Purpose

The goal of this example is to show how to implement Conway's Game of Life on top of [Apache Spark Structured Streaming](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

# How To Run

This requires Kafka to run.  Follow the instructions on the project README on how to do this.

To run the example:

```
mvn clean install
```

The test supplies a few basic game setups to try out simply edit the test to try them out.

# It's Broken

The algorithm is iterative and we expect the world to continuously keep evolving.  While that is implemented and data feeds back into Kafka, the structured streaming example reads that newly written data but then does not continue to materialize new generations.  Need to figure out why and Pull Requests are accepted.