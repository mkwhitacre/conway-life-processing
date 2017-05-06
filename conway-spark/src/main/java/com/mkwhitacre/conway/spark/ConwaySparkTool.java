package com.mkwhitacre.conway.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Computes an approximation to pi
 * Usage: JavaSparkPi [partitions]
 */
public final class ConwaySparkTool {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Conway Spark Example")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        List<SparkCell> initial = createInitial(1000);

        JavaRDD<SparkCell> world = jsc.parallelize(initial, 3);
        world.collect().forEach(System.out::println);

        for(int i = 1; i < 10; i++){
            world = processGeneration(world, i);
            world.collect().forEach(System.out::println);
        }

        spark.stop();
    }

    private static JavaRDD<SparkCell> processGeneration(JavaRDD<SparkCell> world, final int generation) {

        //find all neighbors
        JavaPairRDD<Tuple2<Long, Long>, Tuple2<SparkCell, Integer>> neighborsAndCount = world.flatMapToPair(new FindNeighborsFn());

        //group keys to same process
        // https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
        JavaPairRDD<Tuple2<Long, Long>, Tuple2<SparkCell, Integer>> cellAndTotalNeighborCount = neighborsAndCount.reduceByKey(new CountNeighborsFn());

        //apply rules
        return cellAndTotalNeighborCount.map(new ApplyRulesFn(generation))
        //filter out any that should be dead
        .filter(new EnsureAliveFn());
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
//        coords.add(new Tuple2<>(1L, 3L));
//        coords.add(new Tuple2<>(1L, 2L));
//        coords.add(new Tuple2<>(1L, 1L));

//        //Glider
        coords.add(new Tuple2<>(1L, 1L));
        coords.add(new Tuple2<>(2L, 1L));
        coords.add(new Tuple2<>(3L, 1L));
        coords.add(new Tuple2<>(3L, 2L));
        coords.add(new Tuple2<>(2L, 3L));


        return coords.stream().map(c -> {
            SparkCell cell = new SparkCell();
            cell.setAlive(true);
            cell.setGeneration(0);
            cell.setX(c._1());
            cell.setY(c._2());

            return cell;
        }).collect(Collectors.toList());
    }
}