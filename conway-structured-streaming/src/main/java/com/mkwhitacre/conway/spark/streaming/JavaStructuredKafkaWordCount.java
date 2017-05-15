/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mkwhitacre.conway.spark.streaming;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaStructuredKafkaWordCount <bootstrap-servers> <subscribe-type> <topics>
 *   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
 *   comma-separated list of host:port.
 *   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
 *   'subscribePattern'.
 *   |- <assign> Specific TopicPartitions to consume. Json string
 *   |  {"topicA":[0,1],"topicB":[2,4]}.
 *   |- <subscribe> The topic list to subscribe. A comma-separated list of
 *   |  topics.
 *   |- <subscribePattern> The pattern used to subscribe to topic(s).
 *   |  Java regex string.
 *   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
 *   |  specified for Kafka source.
 *   <topics> Different value format depends on the value of 'subscribe-type'.
 *
 * Example:
 *    `$ bin/run-example \
 *      sql.streaming.JavaStructuredKafkaWordCount host1:port1,host2:port2 \
 *      subscribe topic1,topic2`
 */
public final class JavaStructuredKafkaWordCount {

  public static void main(String[] args) throws Exception {
//    if (args.length < 3) {
//      System.err.println("Usage: JavaStructuredKafkaWordCount <bootstrap-servers> " +
//        "<subscribe-type> <topics>");
//      System.exit(1);
//    }

    String bootstrapServers = "localhost:9092";
    String subscribeType = "subscribe";
    String topics = "test";


    writeData(bootstrapServers, topics);

    SparkSession spark = SparkSession
      .builder()
      .master("local")
      .appName("JavaStructuredKafkaWordCount")
      .getOrCreate();

    // Create DataSet representing the stream of input lines from kafka
    Dataset<String> lines = spark
      .readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscribeType, topics)
      .option("startingOffsets", "{\""+topics+"\":{\"0\":-2}}")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as(Encoders.STRING());

    // Generate running word count
    Dataset<Row> wordCounts = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String x) {
        return Arrays.asList(x.split(" ")).iterator();
      }
    }, Encoders.STRING()).groupBy("value").count();

    // Start running the query that prints the running counts to the console
    StreamingQuery query = wordCounts.writeStream()
      .outputMode("complete")
      .format("console")
      .start();

    query.awaitTermination();
  }


  private static void writeData(String servers, String topic){

    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerDe.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerDe.class.getName());

    Producer<String, String> producer = new KafkaProducer<String, String>(props);


    List<Future<RecordMetadata>> futures = new LinkedList<>();
    for(int i = 0; i < 100; i++){
      futures.add(producer.send(new ProducerRecord<String, String>(topic, "key" + i, "value" + i)));
    }

    for(Future<RecordMetadata> f: futures){
      try {
        f.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("stuff didn't work", e);
      }
    }
  }

  public static class StringSerDe implements Serializer<String>, Deserializer<String>{

    @Override
    public String deserialize(String topic, byte[] value) {
      return new String(value);
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, String value) {
      return value.getBytes();
    }

    @Override
    public void close() {

    }
  }

}