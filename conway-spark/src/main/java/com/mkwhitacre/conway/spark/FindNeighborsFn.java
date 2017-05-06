package com.mkwhitacre.conway.spark;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class FindNeighborsFn implements PairFlatMapFunction<SparkCell,
        Tuple2<Long, Long>, Tuple2<SparkCell, Integer>> {

    @Override
    public Iterator<Tuple2<Tuple2<Long, Long>, Tuple2<SparkCell, Integer>>> call(final SparkCell sparkCell) throws Exception {

        long cellX = sparkCell.getX();
        long cellY = sparkCell.getY();
        //If the coordinates represent the cell the emit the cell but don't increment the second count
        //because a cell can't be a neighbor to itself
        Tuple2<SparkCell, Integer> cellValue = new Tuple2<>(sparkCell, 0);

        //emit this value for coordinates that are for neighbors
        Tuple2<SparkCell, Integer> neighborValue = new Tuple2<>(null, 1);

        return LongStream.range(-1, 2).mapToObj(x -> LongStream.range(-1, 2)
                //collect neighbor coordinates
                .mapToObj(y -> new Tuple2<>(cellX + x, cellY + y)).collect(Collectors.toList()))
                //flatmap into a single list of <Coord, Cell>
                .flatMap(List::stream).map(c -> {
                    //if this refers to the cell then place SparkCell in the value
                    if (c._1().equals(cellX) && c._2().equals(cellY)) {
                        return new Tuple2<>(c, cellValue);
                    }

                    //refers to neighbor, leave it empty
                    return new Tuple2<>(c, neighborValue);
                })
                .collect(Collectors.toList()).iterator();

    }
}
