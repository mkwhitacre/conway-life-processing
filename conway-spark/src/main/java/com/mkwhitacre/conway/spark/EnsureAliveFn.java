package com.mkwhitacre.conway.spark;

import org.apache.spark.api.java.function.Function;

public class EnsureAliveFn implements Function<SparkCell, Boolean> {
    @Override
    public Boolean call(SparkCell sparkCell) throws Exception {
        return sparkCell.isAlive();
    }
}
