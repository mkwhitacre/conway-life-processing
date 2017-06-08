package com.mkwhitacre.conway.crunch;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

import java.util.Objects;


public class ApplyRulesFn extends MapFn<Pair<Pair<Long, Long>, Iterable<Cell>>, Pair<Pair<Long, Long>, Cell>> {
    private final long generation;

    public ApplyRulesFn(long generation) {
        this.generation = generation;
    }

    @Override
    public Pair<Pair<Long, Long>, Cell> map(Pair<Pair<Long, Long>, Iterable<Cell>> cells) {
        Pair<Long, Long> coord = cells.first();

        int neighborCount = 0;
        Cell self = null;

        for(Cell c: cells.second()){
            //check if the cell represents itself and if so then track it to see if it stays alive or dies
            if(Objects.equals(c.getX(), coord.first()) && Objects.equals(c.getY(), coord.second())){
                //make a defensive copy
                self = Cell.newBuilder(c).build();
            }else{
                //count the neighbors to apply rules
                neighborCount++;
            }
        }

        //Any dead cell with exactly three live neighbours becomes a live cell, as if by reproduction.
        if(self == null) {
            //cell is dead so check to see if icomes alive
            if(neighborCount == 3) {
                return Pair.of(coord, Cell.newBuilder().setX(coord.first())
                        .setY(coord.second()).setAlive(true).setGeneration(generation).build());
            }
        } else{
            //cell is alive so check to see if it stays alive
            if(neighborCount ==2 || neighborCount== 3) {
                //Any live cell with two or three live neighbours lives on to the next generation.
                return Pair.of(coord, Cell.newBuilder(self).setAlive(true)
                        .setGeneration(generation).build());
            }
        }

        //Any live cell with fewer than two live neighbours dies, as if caused by underpopulation.
        //Any live cell with more than three live neighbours dies, as if by overpopulation.
        return Pair.of(coord, Cell.newBuilder().setX(coord.first()).setY(coord.second())
                .setAlive(false).setGeneration(generation).build());

    }
}
