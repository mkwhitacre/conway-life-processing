# Purpose

The goal of this example is to show how to implement Conway's Game of Life on top of Hadoop MapReduce but specifically using [Apache Crunch](http://crunch.apache.org/)


# How To Run

While the project is setup to build a distributable jar that could be ran on a cluster it is easy to simulate a run by simply doing a: 

```
mvn clean install
```

The test supplies a few basic game setups to try out simply edit the test to try them out.