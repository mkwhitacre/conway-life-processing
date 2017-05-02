package com.mkwhitacre.conway.crunch;

import org.apache.crunch.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ApplyRulesFnTest {

    private Cell cell;

    long x;
    long y;


    public final static Random random = new Random();
    private ApplyRulesFn fn;
    private long generation;

    @Before
    public void setup(){
        x = random.nextLong();
        y = random.nextLong();
        generation = random.nextLong();
        cell = Cell.newBuilder().setAlive(true).setGeneration(generation-1).setX(x).setY(y).build();
        fn = new ApplyRulesFn(generation);
    }

    //is alive has 3 neighbors
    @Test
    public void alive_has3Neighbors(){
        List<Cell> neighborCells = IntStream.range(-1, 2)
                .mapToObj(i -> Cell.newBuilder(cell).setX(x + i).setY(y - 1).build()).collect(Collectors.toList());


        List<Cell> iter = new LinkedList<>();
        iter.addAll(neighborCells);
        iter.add(cell);

        Pair<Pair<Long, Long>, Cell> out = fn.map(Pair.of(Pair.of(x, y), iter));

        Pair<Long, Long> coord = out.first();
        Cell outCell = out.second();

        assertThat(coord.first(), is(x));
        assertThat(coord.second(), is(y));

        assertThat(outCell.getX(), is(x));
        assertThat(outCell.getY(), is(y));
        assertThat(outCell.getAlive(), is(true));
        assertThat(outCell.getSelf(), is(true));
        assertThat(outCell.getGeneration(), is(generation));
    }

    //is alive has 4 neighbors
    @Test
    public void alive_has4Neighbors(){
        List<Cell> neighborCells = IntStream.range(-1, 3)
                .mapToObj(i -> Cell.newBuilder(cell).setX(x + i).setY(y - 1).build()).collect(Collectors.toList());


        List<Cell> iter = new LinkedList<>();
        iter.addAll(neighborCells);
        iter.add(cell);

        Pair<Pair<Long, Long>, Cell> out = fn.map(Pair.of(Pair.of(x, y), iter));

        Pair<Long, Long> coord = out.first();
        Cell outCell = out.second();

        assertThat(coord.first(), is(x));
        assertThat(coord.second(), is(y));

        assertThat(outCell.getX(), is(x));
        assertThat(outCell.getY(), is(y));
        assertThat(outCell.getAlive(), is(false));
        assertThat(outCell.getSelf(), is(true));
        assertThat(outCell.getGeneration(), is(generation));
    }


    //is alive has 2 neighbors

    @Test
    public void alive_has2Neighbors(){
        List<Cell> neighborCells = IntStream.range(-1, 1)
                .mapToObj(i -> Cell.newBuilder(cell).setX(x + i).setY(y - 1).build()).collect(Collectors.toList());


        List<Cell> iter = new LinkedList<>();
        iter.addAll(neighborCells);
        iter.add(cell);

        Pair<Pair<Long, Long>, Cell> out = fn.map(Pair.of(Pair.of(x, y), iter));

        Pair<Long, Long> coord = out.first();
        Cell outCell = out.second();

        assertThat(coord.first(), is(x));
        assertThat(coord.second(), is(y));

        assertThat(outCell.getX(), is(x));
        assertThat(outCell.getY(), is(y));
        assertThat(outCell.getAlive(), is(true));
        assertThat(outCell.getSelf(), is(true));
        assertThat(outCell.getGeneration(), is(generation));
    }

    //is alive has 0 neighbors
    @Test
    public void alive_has0Neighbors(){
        List<Cell> iter = new LinkedList<>();
        iter.add(cell);

        Pair<Pair<Long, Long>, Cell> out = fn.map(Pair.of(Pair.of(x, y), iter));

        Pair<Long, Long> coord = out.first();
        Cell outCell = out.second();

        assertThat(coord.first(), is(x));
        assertThat(coord.second(), is(y));

        assertThat(outCell.getX(), is(x));
        assertThat(outCell.getY(), is(y));
        assertThat(outCell.getAlive(), is(false));
        assertThat(outCell.getSelf(), is(true));
        assertThat(outCell.getGeneration(), is(generation));
    }


    //is dead has 3 neighbors
    @Test
    public void dead_has3Neighbors(){
        List<Cell> neighborCells = IntStream.range(-1, 2)
                .mapToObj(i -> Cell.newBuilder(cell).setX(x + i).setY(y - 1).build()).collect(Collectors.toList());

        List<Cell> iter = new LinkedList<>();
        iter.addAll(neighborCells);

        Pair<Pair<Long, Long>, Cell> out = fn.map(Pair.of(Pair.of(x, y), iter));

        Pair<Long, Long> coord = out.first();
        Cell outCell = out.second();

        assertThat(coord.first(), is(x));
        assertThat(coord.second(), is(y));

        assertThat(outCell.getX(), is(x));
        assertThat(outCell.getY(), is(y));
        assertThat(outCell.getAlive(), is(true));
        assertThat(outCell.getSelf(), is(true));
        assertThat(outCell.getGeneration(), is(generation));
    }

    //is dead has 4 neighbors
    @Test
    public void dead_has4Neighbors(){
        List<Cell> neighborCells = IntStream.range(-1, 4)
                .mapToObj(i -> Cell.newBuilder(cell).setX(x + i).setY(y - 1).build()).collect(Collectors.toList());

        List<Cell> iter = new LinkedList<>();
        iter.addAll(neighborCells);

        Pair<Pair<Long, Long>, Cell> out = fn.map(Pair.of(Pair.of(x, y), iter));

        Pair<Long, Long> coord = out.first();
        Cell outCell = out.second();

        assertThat(coord.first(), is(x));
        assertThat(coord.second(), is(y));

        assertThat(outCell.getX(), is(x));
        assertThat(outCell.getY(), is(y));
        assertThat(outCell.getAlive(), is(false));
        assertThat(outCell.getSelf(), is(true));
        assertThat(outCell.getGeneration(), is(generation));
    }


    //is dead has 2 neighbors
    @Test
    public void dead_has2Neighbors(){
        List<Cell> neighborCells = IntStream.range(-1, 1)
                .mapToObj(i -> Cell.newBuilder(cell).setX(x + i).setY(y - 1).build()).collect(Collectors.toList());

        List<Cell> iter = new LinkedList<>();
        iter.addAll(neighborCells);

        Pair<Pair<Long, Long>, Cell> out = fn.map(Pair.of(Pair.of(x, y), iter));

        Pair<Long, Long> coord = out.first();
        Cell outCell = out.second();

        assertThat(coord.first(), is(x));
        assertThat(coord.second(), is(y));

        assertThat(outCell.getX(), is(x));
        assertThat(outCell.getY(), is(y));
        assertThat(outCell.getAlive(), is(false));
        assertThat(outCell.getSelf(), is(true));
        assertThat(outCell.getGeneration(), is(generation));
    }


    //is dead has 1 neighbors
    //technically shouldn't hit this
    @Test
    public void dead_has0Neighbors(){
        List<Cell> neighborCells = IntStream.range(-1, 0)
                .mapToObj(i -> Cell.newBuilder(cell).setX(x + i).setY(y - 1).build()).collect(Collectors.toList());

        List<Cell> iter = new LinkedList<>();
        iter.addAll(neighborCells);

        Pair<Pair<Long, Long>, Cell> out = fn.map(Pair.of(Pair.of(x, y), iter));

        Pair<Long, Long> coord = out.first();
        Cell outCell = out.second();

        assertThat(coord.first(), is(x));
        assertThat(coord.second(), is(y));

        assertThat(outCell.getX(), is(x));
        assertThat(outCell.getY(), is(y));
        assertThat(outCell.getAlive(), is(false));
        assertThat(outCell.getSelf(), is(true));
        assertThat(outCell.getGeneration(), is(generation));
    }
}
