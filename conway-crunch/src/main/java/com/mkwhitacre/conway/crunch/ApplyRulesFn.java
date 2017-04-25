package com.mkwhitacre.conway.crunch;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;


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
            if(c.getX() == coord.first() && c.getY() == coord.second()){
                self = Cell.newBuilder(c).build();
            }else{
                neighborCount++;
            }
        }

        //Any dead cell with exactly three live neighbours becomes a live cell, as if by reproduction.
        if(self == null) {
            if(neighborCount == 3) {
                return Pair.of(coord, Cell.newBuilder(self).setAlive(true).setGeneration(generation).build());
            }
        } else{

            if(neighborCount ==2 || neighborCount== 3) {
                //Any live cell with two or three live neighbours lives on to the next generation.
                return Pair.of(coord, Cell.newBuilder().setAlive(true).setSelf(true)
                        .setX(coord.first()).setY(coord.second()).setGeneration(generation).build());
            }
        }

        //Any live cell with fewer than two live neighbours dies, as if caused by underpopulation.
        //Any live cell with more than three live neighbours dies, as if by overpopulation.
        return Pair.of(coord, Cell.newBuilder().setAlive(false)
                .setX(coord.first()).setY(coord.second()).setGeneration(generation).build());

    }
}
