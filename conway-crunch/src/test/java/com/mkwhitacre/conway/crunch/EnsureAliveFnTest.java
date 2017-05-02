package com.mkwhitacre.conway.crunch;

import org.apache.crunch.Pair;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EnsureAliveFnTest {

    @Test
    public void isNotAlive(){
        Pair<Pair<Long, Long>, Cell> value = Pair.of(Pair.of(1L, 1L), Cell.newBuilder().setAlive(false).setGeneration(1).setX(1).setY(1).build());
        assertFalse(new EnsureAllAliveFn().accept(value));
    }

    @Test
    public void isAlive(){
        Pair<Pair<Long, Long>, Cell> value = Pair.of(Pair.of(1L, 1L), Cell.newBuilder().setAlive(true).setGeneration(1).setX(1).setY(1).build());
        assertTrue(new EnsureAllAliveFn().accept(value));
    }
}
