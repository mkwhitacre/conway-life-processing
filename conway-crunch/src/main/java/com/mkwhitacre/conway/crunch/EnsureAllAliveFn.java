package com.mkwhitacre.conway.crunch;

import org.apache.crunch.Pair;

public class EnsureAllAliveFn extends org.apache.crunch.FilterFn<org.apache.crunch.Pair<org.apache.crunch.Pair<Long, Long>, Cell>> {
    @Override
    public boolean accept(Pair<Pair<Long, Long>, Cell> pairCellPair) {
        return pairCellPair.second().getAlive();
    }
}
