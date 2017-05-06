package com.mkwhitacre.conway.spark;

import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;


public class CountNeighborsFn implements Function2<Tuple2<SparkCell, Integer>, Tuple2<SparkCell, Integer>, Tuple2<SparkCell, Integer>> {
    @Override
    public Tuple2<SparkCell, Integer> call(Tuple2<SparkCell, Integer> value1, Tuple2<SparkCell, Integer> value2) throws Exception {

        //Should only get one cell populated the rest should be null.
        SparkCell cell = value1._1();
        if(cell == null){
            cell = value2._1();
        }

        //summarize the neighbor count.
        return new Tuple2<>(cell, value1._2()+value2._2());
    }
}
