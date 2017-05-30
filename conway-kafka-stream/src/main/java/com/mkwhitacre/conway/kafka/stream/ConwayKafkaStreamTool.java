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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class ConwayKafkaStreamTool {

    public static void main(String[] args) throws InterruptedException {


        String bootstrapServers = "localhost:9092";
        String schemaURL = "http://localhost:8081";
        //Use this to ensure uniqueness of topic + state
        long unique = System.currentTimeMillis();
        String startTopic = "start-cells-blinker-foobar"+unique;


        writeData(bootstrapServers, schemaURL, startTopic);

        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "conway-kafka-streams-example");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);



        final KStreamBuilder builder = new KStreamBuilder();

        SpecificAvroSerde<Coord> keySerde = createSerde(schemaURL);
        SpecificAvroSerde<Cell> valueSerde = createSerde(schemaURL);



        KStream<Coord, Cell> cells = builder.stream(keySerde, valueSerde, startTopic);

        KStream<Coord, Cell> cellsWithNeighbors = cells.flatMap((key, value) -> {

            long cellX = key.getX();
            long cellY = key.getY();

            System.out.println("Key:"+key+" Value:"+value);

            //emit this value for coordinates that are for neighbors
            Cell neighborValue = Cell.newBuilder().setAlive(false).setX(cellX).setY(cellY)
                    .setNeighborCount(1).setGeneration(value.getGeneration()+1).build();

            return LongStream.range(-1, 2).mapToObj(x -> LongStream.range(-1, 2)
                    //collect neighbor coordinates
                    .mapToObj(y ->
                        Coord.newBuilder().setX(cellX + x).setY(cellY + y).build())
                    .collect(Collectors.toList()))
                    //flatmap into a single list of <Coord, Cell>
                    .flatMap(List::stream).map(c -> {
                //if this refers to the cell then place SparkCell in the value
                if (c.getX().equals(cellX) && c.getY().equals(cellY)) {
                    return new KeyValue<>(c, value);
                }

                return new KeyValue<>(c, Cell.newBuilder(neighborValue).setX(c.getX()).setY(c.getY()).build());
            }).collect(Collectors.toList());
        });


        KTable<Coord, Cell> reduce = cellsWithNeighbors.groupByKey(keySerde, valueSerde)



 .reduce((aggValue, newValue) -> {
      return Cell.newBuilder(aggValue).setAlive(aggValue.getAlive()|| newValue.getAlive())
                    .setNeighborCount(aggValue.getNeighborCount()+newValue.getNeighborCount()).build();
        }, "reduce-cells"+unique).mapValues(c -> {

            boolean isAlive = c.getAlive();
            int neighborCount = c.getNeighborCount();

            Cell.Builder cellBuilder = Cell.newBuilder(c);

            if(isAlive){
                if(neighborCount == 2 || neighborCount == 3){
                    cellBuilder.setAlive(true);
                }
            }else{
                if(neighborCount == 3){
                    cellBuilder.setAlive(true);
                }
            }

            return cellBuilder.build();
        }).filter((k, v) -> v != null || v.getAlive());

        reduce.print();

        reduce = reduce.mapValues(c -> Cell.newBuilder(c).clearNeighborCount().build());

        reduce.to(keySerde, valueSerde, startTopic);

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

        streams.cleanUp();
        streams.start();


        Thread.sleep(120000L);


//        readCells(bootstrapServers, schemaURL, startTopic);


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

            Coord coord = Coord.newBuilder().setX(cell.getX()).setY(cell.getY()).build();
//                        Coord coord = Coord.newBuilder(cell.getCoordinates()).build();

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
//            Cell.newBuilder().setAlive(true).setGeneration(0).setCoordinates(c).build())

        Cell.newBuilder().setAlive(true).setGeneration(0).setX(c.getX()).setY(c.getY()).build())
                .collect(Collectors.toList());

    }
}
