package com.mkwhitacre.conway.spark;


import org.junit.Test;
import scala.Tuple2;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertThat;

public class FindNeighborsFnTest {

    @Test
    public void generateNeighbors() throws Exception {

        long x = 2;
        long y = 2;
        Tuple2<Long, Long> self = new Tuple2<>(x, y);

        SparkCell cell = new SparkCell();
        cell.setAlive(true);
        cell.setGeneration(0);
        cell.setX(x);
        cell.setY(y);


        FindNeighborsFn fn = new FindNeighborsFn();
        Iterator<Tuple2<Tuple2<Long, Long>, Tuple2<SparkCell, Integer>>> results = fn.call(cell);

        int total = 0;

        List<Tuple2<Long, Long>> expectedCoords = new LinkedList<>();
        expectedCoords.add(new Tuple2<>(1L, 1L));
        expectedCoords.add(new Tuple2<>(1L, 2L));
        expectedCoords.add(new Tuple2<>(1L, 3L));

        expectedCoords.add(new Tuple2<>(2L, 1L));
        expectedCoords.add(new Tuple2<>(2L, 2L));
        expectedCoords.add(new Tuple2<>(2L, 3L));

        expectedCoords.add(new Tuple2<>(3L, 1L));
        expectedCoords.add(new Tuple2<>(3L, 2L));
        expectedCoords.add(new Tuple2<>(3L, 3L));

        while(results.hasNext()){
            Tuple2<Tuple2<Long, Long>, Tuple2<SparkCell, Integer>> next = results.next();
            System.out.println(next);

            assertThat(expectedCoords, hasItem(next._1()));
            //remove as we should only get this value once.
            expectedCoords.remove(next._1());
            if(next._1().equals(self)){
                assertThat(next._2(), is(new Tuple2<>(cell, 0)));

            }else{
                assertThat(next._2(), is(new Tuple2<>(null, 1)));
            }

            total++;
        }

        assertThat(total, is(9));
    }

}
