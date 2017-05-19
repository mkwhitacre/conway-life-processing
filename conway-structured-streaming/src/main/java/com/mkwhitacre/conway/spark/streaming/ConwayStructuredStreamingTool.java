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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;


public final class ConwayStructuredStreamingTool {

    public static void main(String[] args) throws Exception {

        String bootstrapServers = "localhost:9092";
        String subscribeType = "subscribe";
        String topics = "test-cell-blinker";


        writeData(bootstrapServers, topics);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JavaStructuredKafkaWordCount")
                .getOrCreate();

        // Create DataSet representing the stream of input lines from kafka
        Dataset<Row> rows = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option(subscribeType, topics)
                .option("startingOffsets", "{\""+topics+"\":{\"0\":-2}}")
                .load();

        //TODO this is the output from just reading from Kafka...
//        +----------------+--------------------+---------+---------+------+--------------------+-------------+
//        |             key|               value|    topic|partition|offset|           timestamp|timestampType|
//        +----------------+--------------------+---------+---------+------+--------------------+-------------+
//        |   [6B 65 79 30]|[AC ED 00 05 73 7...|test-cell|        0|     0|2017-05-15 20:47:...|            0|
//        |   [6B 65 79 31]|[AC ED 00 05 73 7...|test-cell|        0|     1|2017-05-15 20:47:...|            0|
//        |   [6B 65 79 32]|[AC ED 00 05 73 7...|test-cell|        0|     2|2017-05-15 20:47:...|            0|
//        |   [6B 65 79 33]|[AC ED 00 05 73 7...|test-cell|        0|     3|2017-05-15 20:47:...|            0|
//        |   [6B 65 79 34]|[AC ED 00 05 73 7...|test-cell|        0|     4|2017-05-15 20:47:...|            0|
//        |   [6B 65 79 35]|[AC ED 00 05 73 7...|test-cell|        0|     5|2017-05-15 20:47:...|            0|
//        |   [6B 65 79 36]|[AC ED 00 05 73 7...|test-cell|        0|     6|2017-05-15 20:47:...|            0|
//        |   [6B 65 79 37]|[AC ED 00 05 73 7...|test-cell|        0|     7|2017-05-15 20:47:...|            0|
//        |   [6B 65 79 38]|[AC ED 00 05 73 7...|test-cell|        0|     8|2017-05-15 20:47:...|            0|
//        |   [6B 65 79 39]|[AC ED 00 05 73 7...|test-cell|        0|     9|2017-05-15 20:47:...|            0|
//        |[6B 65 79 31 30]|[AC ED 00 05 73 7...|test-cell|        0|    10|2017-05-15 20:47:...|            0|
//        |[6B 65 79 31 31]|[AC ED 00 05 73 7...|test-cell|        0|    11|2017-05-15 20:47:...|            0|
//        |[6B 65 79 31 32]|[AC ED 00 05 73 7...|test-cell|        0|    12|2017-05-15 20:47:...|            0|
//        |[6B 65 79 31 33]|[AC ED 00 05 73 7...|test-cell|        0|    13|2017-05-15 20:47:...|            0|
//        |[6B 65 79 31 34]|[AC ED 00 05 73 7...|test-cell|        0|    14|2017-05-15 20:47:...|            0|
//        |[6B 65 79 31 35]|[AC ED 00 05 73 7...|test-cell|        0|    15|2017-05-15 20:47:...|            0|
//        |[6B 65 79 31 36]|[AC ED 00 05 73 7...|test-cell|        0|    16|2017-05-15 20:47:...|            0|
//        |[6B 65 79 31 37]|[AC ED 00 05 73 7...|test-cell|        0|    17|2017-05-15 20:47:...|            0|
//        |[6B 65 79 31 38]|[AC ED 00 05 73 7...|test-cell|        0|    18|2017-05-15 20:47:...|            0|
//        |[6B 65 79 31 39]|[AC ED 00 05 73 7...|test-cell|        0|    19|2017-05-15 20:47:...|            0|
//        +----------------+--------------------+---------+---------+------+--------------------+-------------+

        //from https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html
        //key.deserializer: Keys are always deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame operations to explicitly deserialize the keys.
        //value.deserializer: Values are always deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame operations to explicitly deserialize the values.

        Dataset<SparkCell> cells = rows.select("value").as(Encoders.BINARY()).map(new SerializeFn(), Encoders.bean(SparkCell.class));

        //TODO this is the output when you print the cells dataset to the console
//        +-----+----------+---+---+
//        |alive|generation|  x|  y|
//        +-----+----------+---+---+
//        | true|         0|  0|  0|
//        | true|         0|  1|  1|
//        | true|         0|  2|  2|
//        | true|         0|  3|  3|
//        | true|         0|  4|  4|
//        | true|         0|  5|  5|
//        | true|         0|  6|  6|
//        | true|         0|  7|  7|
//        | true|         0|  8|  8|
//        | true|         0|  9|  9|
//        | true|         0| 10| 10|
//        | true|         0| 11| 11|
//        | true|         0| 12| 12|
//        | true|         0| 13| 13|
//        | true|         0| 14| 14|
//        | true|         0| 15| 15|
//        | true|         0| 16| 16|
//        | true|         0| 17| 17|
//        | true|         0| 18| 18|
//        | true|         0| 19| 19|
//        +-----+----------+---+---+






        Dataset<Tuple2<Tuple2<Long, Long>, Tuple2<SparkCell, Integer>>> keyedNeighbors = cells.flatMap(new FindNeighborsFn(),
                Encoders.tuple(Encoders.tuple(Encoders.LONG(), Encoders.LONG()),
                        Encoders.tuple(Encoders.bean(SparkCell.class), Encoders.INT())))



//        +-----+----------------+
//        |   _1|              _2|
//        +-----+----------------+
//        |[0,2]|        [null,1]|
//        |[0,3]|        [null,1]|
//        |[0,4]|        [null,1]|
//        |[1,2]|        [null,1]|
//        |[1,3]|[[true,0,1,3],0]|
//        |[1,4]|        [null,1]|
//        |[2,2]|        [null,1]|
//        |[2,3]|        [null,1]|
//        |[2,4]|        [null,1]|
//        |[0,1]|        [null,1]|
//        |[0,2]|        [null,1]|
//        |[0,3]|        [null,1]|
//        |[1,1]|        [null,1]|
//        |[1,2]|[[true,0,1,2],0]|
//        |[1,3]|        [null,1]|
//        |[2,1]|        [null,1]|
//        |[2,2]|        [null,1]|
//        |[2,3]|        [null,1]|
//        |[0,0]|        [null,1]|
//        |[0,1]|        [null,1]|
//        +-----+----------------+


//        .withColumnRenamed("_1", "coordinates").withColumnRenamed("cell_count", "_2");

        ;



        //find all neighbors

        //apply rules


        //filter out dead


        //write out the complete table back to same Kafka topic + console?


        // Start running the query that prints the running counts to the console
        StreamingQuery query = keyedNeighbors.writeStream()
                .outputMode("append")
                .format("console")
                .start();




        query.awaitTermination();
    }


    private static void writeData(String servers, String topic){

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JavaStructuredKafkaWordCount.StringSerDe.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CellSerDe.class.getName());

        Producer<String, SparkCell> producer = new KafkaProducer<>(props);

        List<Future<RecordMetadata>> futures = new LinkedList<>();
        for(SparkCell cell: createInitial(10)){

            futures.add(producer.send(new ProducerRecord<>(topic, "key" + cell.getX()+"-"+cell.getY(), cell)));
        }

        for(Future<RecordMetadata> f: futures){
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("stuff didn't work", e);
            }
        }
    }

    private static List<SparkCell> createInitial(long numCells){
        List<Tuple2<Long, Long>> coords = new LinkedList<>();

//        //toad (period 2)
//        coords.add(new Tuple2<>(2L, 3L));
//        coords.add(new Tuple2<>(3L, 3L));
//        coords.add(new Tuple2<>(4L, 3L));
//        coords.add(new Tuple2<>(1L, 2L));
//        coords.add(new Tuple2<>(2L, 2L));
//        coords.add(new Tuple2<>(3L, 2L));

        //blinker (period 2)
        coords.add(new Tuple2<>(1L, 3L));
        coords.add(new Tuple2<>(1L, 2L));
        coords.add(new Tuple2<>(1L, 1L));

//        //Glider
//        coords.add(new Tuple2<>(1L, 1L));
//        coords.add(new Tuple2<>(2L, 1L));
//        coords.add(new Tuple2<>(3L, 1L));
//        coords.add(new Tuple2<>(3L, 2L));
//        coords.add(new Tuple2<>(2L, 3L));


        return coords.stream().map(c -> {
            SparkCell cell = new SparkCell();
            cell.setAlive(true);
            cell.setGeneration(0);
            cell.setX(c._1());
            cell.setY(c._2());

            return cell;
        }).collect(Collectors.toList());
    }


    public static class CellSerDe implements Serializer<SparkCell>, Deserializer<SparkCell>{

        @Override
        public SparkCell deserialize(String topic, byte[] value) {
            try (ByteArrayInputStream in = new ByteArrayInputStream(value);
                 ObjectInputStream iStream = new ObjectInputStream(in)) {
                return (SparkCell) iStream.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public byte[] serialize(String topic, SparkCell value) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                 ObjectOutputStream outStream = new ObjectOutputStream(out)){
                outStream.writeObject(value);
                return out.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {

        }
    }

}