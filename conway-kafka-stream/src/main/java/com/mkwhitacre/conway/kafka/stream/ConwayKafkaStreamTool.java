package com.mkwhitacre.conway.kafka.stream;

import com.mkwhitacre.conway.kafka.stream.util.SpecificAvroSerde;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ConwayKafkaStreamTool {

    public static void main(String[] args){


        String bootstrapServers = "localhost:9092";
        String schemaURL = "http://localhost:8081";
        String startTopic = "start-cells-blinker-foo";

        writeData(bootstrapServers, schemaURL, startTopic);

        String endTopic = "life-cells-blinker";


        readCells(bootstrapServers, schemaURL, startTopic);


    }

    private static void readCells(final String bootstrapServers,
                                              final String schemaRegistryUrl,
                                              String topic) {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        SpecificAvroSerde<Coord> keySerde = createSerde(schemaRegistryUrl);
        SpecificAvroSerde<Cell> valueSerde = createSerde(schemaRegistryUrl);

        final KafkaConsumer<Coord, Cell> consumer = new KafkaConsumer<>(consumerProps,
                keySerde.deserializer(), valueSerde.deserializer());
        consumer.subscribe(Collections.singleton(topic));
        while(true) {
            final ConsumerRecords<Coord, Cell> records = consumer.poll(Long.MAX_VALUE);
            records.forEach(record -> System.out.println(record.value()));
        }
//        consumer.close();
    }


    private static void writeData(String servers, String schemaURL, String topic){

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);

        SpecificAvroSerde<Coord> keySerde = createSerde(schemaURL);
        SpecificAvroSerde<Cell> valueSerde = createSerde(schemaURL);

        Producer<Coord, Cell> producer = new KafkaProducer<>(props, keySerde.serializer(), valueSerde.serializer());

        List<Future<RecordMetadata>> futures = new LinkedList<>();
        for(Cell cell: createInitial(10)){

//            Coord coord = Coord.newBuilder().setX(cell.getX()).setY(cell.getY()).build();
                        Coord coord = Coord.newBuilder(cell.getCoordinates()).build();

            futures.add(producer.send(new ProducerRecord<>(topic, coord , cell)));
        }

        for(Future<RecordMetadata> f: futures){
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("stuff didn't work", e);
            }
        }
    }


    private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl) {
        final CachedSchemaRegistryClient
                schemaRegistry =
                new CachedSchemaRegistryClient(schemaRegistryUrl, 10);

        final Map<String, String>
                serdeProps =
                Collections.singletonMap("schema.registry.url", schemaRegistryUrl);

        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>(schemaRegistry, serdeProps);
        serde.configure(serdeProps, true);
        return serde;
    }


    private static List<Cell> createInitial(long numCells){
        List<Coord> coords = new LinkedList<>();

        //toad (period 2)
//        coords.add(Coord.newBuilder().setX(2L).setY(3L).build());
//        coords.add(Coord.newBuilder().setX(3L).setY(3L).build());
//        coords.add(Coord.newBuilder().setX(4L).setY(3L).build());
//        coords.add(Coord.newBuilder().setX(1L).setY(2L).build());
//        coords.add(Coord.newBuilder().setX(2L).setY(2L).build());
//        coords.add(Coord.newBuilder().setX(3L).setY(2L).build());

        //blinker (period 2)
        coords.add(Coord.newBuilder().setX(1L).setY(3L).build());
        coords.add(Coord.newBuilder().setX(1L).setY(2L).build());
        coords.add(Coord.newBuilder().setX(1L).setY(1L).build());

        //Glider
//        coords.add(Coord.newBuilder().setX(1L).setY(1L).build());
//        coords.add(Coord.newBuilder().setX(2L).setY(1L).build());
//        coords.add(Coord.newBuilder().setX(3L).setY(1L).build());
//        coords.add(Coord.newBuilder().setX(3L).setY(2L).build());
//        coords.add(Coord.newBuilder().setX(2L).setY(3L).build());


        return coords.stream().map(c ->
            Cell.newBuilder().setAlive(true).setGeneration(0).setCoordinates(c).build())

//        Cell.newBuilder().setAlive(true).setGeneration(0).setX(c.getX()).setY(c.getY()).build())
                .collect(Collectors.toList());

    }
}
