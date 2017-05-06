package com.mkwhitacre.conway.spark;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EnsureAliveFnTest {

    @Test
    public void isAlive() throws Exception {
        SparkCell cell = new SparkCell();
        cell.setAlive(true);
        cell.setGeneration(0);
        cell.setX(1);
        cell.setY(1);

        assertTrue(new EnsureAliveFn().call(cell));
    }

    @Test
    public void isDead() throws Exception {
        SparkCell cell = new SparkCell();
        cell.setAlive(false);
        cell.setGeneration(0);
        cell.setX(1);
        cell.setY(1);

        assertFalse(new EnsureAliveFn().call(cell));
    }

}
