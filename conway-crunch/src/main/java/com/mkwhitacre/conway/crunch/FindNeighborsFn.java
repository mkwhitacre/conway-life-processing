package com.mkwhitacre.conway.crunch;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import java.util.stream.LongStream;

public class FindNeighborsFn extends org.apache.crunch.DoFn<Cell, Pair<Pair<Long, Long>, Cell>> {

    @Override
    public void process(Cell cell, Emitter<Pair<Pair<Long, Long>, Cell>> emitter) {
        LongStream.range(-1, 2).forEach(x -> {
            LongStream.range(-1, 2).forEach(y -> {
                emitter.emit(Pair.of(Pair.of(cell.getX()+x, cell.getY()+y), cell));
            });
        });
    }
}
