package com.mkwhitacre.conway.spark.streaming;

import org.apache.spark.api.java.function.MapFunction;
import scala.Tuple2;



public class KeyItFn implements MapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<SparkCell, Integer>>, Tuple2<Long, Long>> {

    @Override
    public Tuple2<Long, Long> call(Tuple2<Tuple2<Long, Long>, Tuple2<SparkCell, Integer>> value) throws Exception {
        return value._1();
    }
}
