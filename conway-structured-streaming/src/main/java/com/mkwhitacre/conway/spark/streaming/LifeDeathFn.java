package com.mkwhitacre.conway.spark.streaming;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;


public class LifeDeathFn implements MapFunction<Row, SparkCell> {
    @Override
    public SparkCell call(Row row) throws Exception {

        System.out.println(row.schema().fieldNames());

        long neighborCount = row.getLong(0);
        Row merged = row.getStruct(1);

        boolean isAlive = merged.getBoolean(0);
        long x = merged.getLong(1);
        long y = merged.getLong(2);

        SparkCell cell = new SparkCell();
        cell.setX(x);
        cell.setY(y);
        cell.setAlive(false);

        if(neighborCount == 3){
            cell.setAlive(true);
        }else if(neighborCount == 2 && isAlive){
            cell.setAlive(true);
        }

        return cell;
    }
}
