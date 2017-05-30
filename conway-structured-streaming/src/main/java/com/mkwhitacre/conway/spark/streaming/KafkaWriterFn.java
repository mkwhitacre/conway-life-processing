package com.mkwhitacre.conway.spark.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.sql.ForeachWriter;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaWriterFn extends ForeachWriter<SparkCell> {

    private final String topic;
    private final String servers;

    private Producer<String, SparkCell> producer;
    private List<Future<RecordMetadata>> futures;

    public KafkaWriterFn(String bootstrapServer, String topic){
        this.servers = bootstrapServer;
        this.topic = topic;
    }

    @Override
    public boolean open(long partitionId, long version) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JavaStructuredKafkaWordCount.StringSerDe.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConwayStructuredStreamingTool.CellSerDe.class.getName());

        producer = new KafkaProducer<>(props);

        futures = new LinkedList<>();

        return true;
    }

    @Override
    public void process(SparkCell cell) {
        System.out.println("Processing Cell:"+cell);
        futures.add(producer.send(new ProducerRecord<>(topic, "key" + cell.getX()+"-"+cell.getY(), cell)));
    }

    @Override
    public void close(Throwable errorOrNull) {

        for(Future<RecordMetadata> f: futures){
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("stuff didn't work", e);
            }
        }

        producer.close();
    }
}
