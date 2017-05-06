package com.mkwhitacre.conway.spark;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;


public class ApplyRulesFn implements Function<Tuple2<Tuple2<Long, Long>, Tuple2<SparkCell, Integer>>, SparkCell> {

    private final long generation;

    public ApplyRulesFn(long generation){
        this.generation = generation;
    }

    @Override
    public SparkCell call(Tuple2<Tuple2<Long, Long>, Tuple2<SparkCell, Integer>> value) throws Exception {

        Tuple2<Long, Long> coord = value._1();

        SparkCell self = value._2()._1();
        int neighborCount = value._2()._2();

        SparkCell cell = new SparkCell();
        cell.setX(coord._1());
        cell.setY(coord._2());
        cell.setGeneration(generation);

        //Any dead cell with exactly three live neighbours becomes a live cell, as if by reproduction.
        if(self == null) {
            //cell is dead so check to see if icomes alive
            if(neighborCount == 3) {
                cell.setAlive(true);
                return cell;
            }
        } else{
            //cell is alive so check to see if it stays alive
            if(neighborCount ==2 || neighborCount== 3) {
                cell.setAlive(true);
                return cell;
            }
        }

        //Any live cell with fewer than two live neighbours dies, as if caused by underpopulation.
        //Any live cell with more than three live neighbours dies, as if by overpopulation.
        cell.setAlive(false);
        return cell;
    }
}
