package com.mkwhitacre.conway.crunch;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class FindNeighborsFnTest {

    @Mock
    private Emitter<Pair<Pair<Long, Long>, Cell>> emitter;

    @Captor
    private ArgumentCaptor<Pair<Pair<Long, Long>, Cell>> emitterCaptor;

    @Test
    public void validateNeighbors(){
        FindNeighborsFn findNeighborsFn = new FindNeighborsFn();

        Cell cell = Cell.newBuilder().setAlive(true).setGeneration(1).setX(1).setY(1).build();

        findNeighborsFn.process(cell, emitter);

        verify(emitter, times(9)).emit(emitterCaptor.capture());

        List<Pair<Pair<Long, Long>, Cell>> allValues = emitterCaptor.getAllValues();
        List<Pair<Pair<Long, Long>, Cell>> expected = new LinkedList<>();

        expected.add(Pair.of(Pair.of(0L,0L), cell));
        expected.add(Pair.of(Pair.of(0L,1L), cell));
        expected.add(Pair.of(Pair.of(0L,2L), cell));
        expected.add(Pair.of(Pair.of(1L,0L), cell));
        expected.add(Pair.of(Pair.of(1L,1L), cell));
        expected.add(Pair.of(Pair.of(1L,2L), cell));
        expected.add(Pair.of(Pair.of(2L,0L), cell));
        expected.add(Pair.of(Pair.of(2L,1L), cell));
        expected.add(Pair.of(Pair.of(2L,2L), cell));

        expected.forEach(p -> assertThat(allValues, hasItem(p)));
    }
}
