package com.mkwhitacre.conway.kafka.stream;

import com.mkwhitacre.conway.kafka.stream.util.SpecificAvroSerde;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;
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
        String topic = "game-life-cells" + unique;


        writeData(bootstrapServers, schemaURL, topic);

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

        //read the values out of Kafka to populate the table.
        KTable<Coord, Cell> world = builder.table(keySerde, valueSerde, topic, "world-store" + unique);

        //filter out any null values because those represent "tombstones" and values that should be removed
        KStream<Coord, Cell> cells = world.toStream().filter((k, v) -> v != null);


        //create all of the neighbors so that we can perform the proper counts.
        KStream<Coord, Cell> cellsWithNeighbors = cells.flatMap((key, value) -> {

            System.out.println("Key:" + key+ " Value: "+value);

            long cellX = key.getX();
            long cellY = key.getY();
            //increment the generation value as we are calculating a new generation
            long valueNextGeneration = value.getGeneration() + 1;
            //emit this value for coordinates that are for neighbors
            Cell neighborValue = Cell.newBuilder().setAlive(false).setX(cellX).setY(cellY)
                    .setNeighborCount(1).setGeneration(valueNextGeneration).build();

            //create the new "value" for the cell representing the current cell.
            Cell newValue = Cell.newBuilder(value).setGeneration(valueNextGeneration).build();

            return LongStream.range(-1, 2).mapToObj(x -> LongStream.range(-1, 2)
                    //collect neighbor coordinates
                    .mapToObj(y ->
                            Coord.newBuilder().setX(cellX + x).setY(cellY + y).build())
                    .collect(Collectors.toList()))
                    //flatmap into a single list of <Coord, Cell>
                    .flatMap(List::stream).map(c -> {
                        //if this refers to the cell then place SparkCell in the value
                        if (c.getX().equals(cellX) && c.getY().equals(cellY)) {
                            return new KeyValue<>(c, newValue);
                        }
                        //update the X and Y values appropriately
                        return new KeyValue<>(c, Cell.newBuilder(neighborValue).setX(c.getX()).setY(c.getY()).build());
                    }).collect(Collectors.toList());
        });


        KTable<Coord, Cell> aggTable = cellsWithNeighbors.groupByKey(keySerde, valueSerde)
                .reduce((aggValue, newValue) -> {

                    System.out.println("AggValue:" + aggValue+ " NewValue: "+newValue);
                    //The KTable being created maintains the values from the last iteration
                    //and we do not want those values to affect our current count.
                    //therefore to avoid using those values in the count we only aggregate
                    //when the generation is equal and we also only use the higher value.
                    long aggGeneration = aggValue.getGeneration();
                    long newGeneration = newValue.getGeneration();

                    if (aggGeneration != newGeneration) {
                        if (aggGeneration > newGeneration) {
                            return aggValue;
                        } else {
                            return newValue;
                        }
                    }

                    //generation values are the same so we should aggregate the values
                    return Cell.newBuilder(aggValue).setAlive(aggValue.getAlive() || newValue.getAlive())
                            .setNeighborCount(aggValue.getNeighborCount() + newValue.getNeighborCount()).build();
                }, "reduce-cells" + unique)


                .through(keySerde, valueSerde, "agg-world"+unique, "agg"+unique);

        aggTable.print();

                //now that we've aggregated counts we should apply the rules
        KTable<Coord, Cell> rulesCells = aggTable.mapValues(c -> {

            System.out.println("Map Value:" + c);

            boolean isAlive = c.getAlive();
            int neighborCount = c.getNeighborCount();
            //reset the count
            Cell.Builder cellBuilder = Cell.newBuilder(c).clearNeighborCount();

            if (isAlive) {
                //cell is currently alive and should stay alive based on the number of neighbors
                if (neighborCount == 2 || neighborCount == 3) {
                    return cellBuilder.setAlive(true).build();
                }
            } else {
                //cell is not alive but should be if count is correct value.
                if (neighborCount == 3) {
                    return cellBuilder.setAlive(true).build();
                }
            }

            //For cells that should not be alive or die, need to emit null to represent a delete
            return (Cell) null;
        });


        //print out the state of the table for debug purposes.
        rulesCells.print();

        //write the "changes" of updates, inserts, and deletes into the original topic
        rulesCells.toStream().to(keySerde, valueSerde, topic);

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

        streams.cleanUp();
        streams.start();

        //the streams process it non-blocking.  Just run for a time
        Thread.sleep(60000L);
    }

    private static void writeData(String servers, String schemaURL, String topic) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);

        SpecificAvroSerde<Coord> keySerde = createSerde(schemaURL);
        SpecificAvroSerde<Cell> valueSerde = createSerde(schemaURL);

        Producer<Coord, Cell> producer = new KafkaProducer<>(props, keySerde.serializer(), valueSerde.serializer());

        List<Future<RecordMetadata>> futures = new LinkedList<>();
        for (Cell cell : createInitial(10)) {

            Coord coord = Coord.newBuilder().setX(cell.getX()).setY(cell.getY()).build();

            futures.add(producer.send(new ProducerRecord<>(topic, coord, cell)));
        }

        for (Future<RecordMetadata> f : futures) {
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


    private static List<Cell> createInitial(long numCells) {
        List<Coord> coords = new LinkedList<>();

        //toad (period 2)
        coords.add(Coord.newBuilder().setX(2L).setY(3L).build());
        coords.add(Coord.newBuilder().setX(3L).setY(3L).build());
        coords.add(Coord.newBuilder().setX(4L).setY(3L).build());
        coords.add(Coord.newBuilder().setX(1L).setY(2L).build());
        coords.add(Coord.newBuilder().setX(2L).setY(2L).build());
        coords.add(Coord.newBuilder().setX(3L).setY(2L).build());

        //blinker (period 2)
//        coords.add(Coord.newBuilder().setX(1L).setY(3L).build());
//        coords.add(Coord.newBuilder().setX(1L).setY(2L).build());
//        coords.add(Coord.newBuilder().setX(1L).setY(1L).build());

        //Glider
//        coords.add(Coord.newBuilder().setX(1L).setY(1L).build());
//        coords.add(Coord.newBuilder().setX(2L).setY(1L).build());
//        coords.add(Coord.newBuilder().setX(3L).setY(1L).build());
//        coords.add(Coord.newBuilder().setX(3L).setY(2L).build());
//        coords.add(Coord.newBuilder().setX(2L).setY(3L).build());


        return coords.stream().map(c ->
                Cell.newBuilder().setAlive(true).setGeneration(0).setX(c.getX()).setY(c.getY()).build())
                .collect(Collectors.toList());

    }
}
