package com.mkwhitacre.conway.spark.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple6;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class GenerateNeighborsFn implements FlatMapFunction<Row, Tuple3<String, SparkCell, Integer>> {
    @Override
    public Iterator<Tuple3<String, SparkCell, Integer>> call(Row row) throws Exception {

        long cellX = row.getAs("x");
        long cellY = row.getAs("y");
        long generation = row.getAs("generation");

        return LongStream.range(-1, 2).mapToObj(x -> LongStream.range(-1, 2)
                //collect neighbor coordinates
                .mapToObj(y -> new Tuple2<>(cellX + x, cellY + y)).collect(Collectors.toList()))
                //flatmap into a single list of <Coord, Cell>
                .flatMap(List::stream).map(c -> {
                    String coord = c._1()+","+c._2();
                    SparkCell cellValue = new SparkCell();
                    cellValue.setGeneration(generation);
                    cellValue.setX(c._1());
                    cellValue.setY(c._2());

                    //if this refers to the cell then place SparkCell in the value
                    if (c._1().equals(cellX) && c._2().equals(cellY)) {
                        cellValue.setAlive(true);
                        return new Tuple3<>(coord, cellValue, 0);
                    }

                    cellValue.setAlive(false);
                    //refers to neighbor, leave it empty
                    return new Tuple3<>(coord, cellValue, 1);
                })
                .collect(Collectors.toList()).iterator();
    }
}
