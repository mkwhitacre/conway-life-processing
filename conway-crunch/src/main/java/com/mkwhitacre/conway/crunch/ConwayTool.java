package com.mkwhitacre.conway.crunch;


import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.LinkedList;
import java.util.List;

public class ConwayTool extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ConwayTool(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        //write out the starting
        Pipeline pipeline = new MRPipeline(ConwayTool.class, getConf());

        PCollection<Cell> initial = createInitial(pipeline, 1000);
        printCells(initial);

        PCollection<Cell> world = initial;
        for(int i = 1; i < 10; i++){
            world = processGeneration(world, i);
            printCells(world);
        }

        PipelineResult result = pipeline.done();

        if(!result.succeeded()){

            return 1;
        }

        return 0;
    }


    private PCollection<Cell> processGeneration(PCollection<Cell> world, long generation){

        //find all neighbors
        PTable<Pair<Long, Long>, Cell> selfAndNeighbors = world.parallelDo(new FindNeighborsFn(),
                Avros.tableOf(Avros.pairs(Avros.longs(), Avros.longs()), Avros.records(Cell.class)));

        PGroupedTable<Pair<Long, Long>, Cell> groupedCells = selfAndNeighbors.groupByKey();
        //apply rules
        PTable<Pair<Long, Long>, Cell> nextGeneration = groupedCells.parallelDo(new ApplyRulesFn(generation),
                Avros.tableOf(Avros.pairs(Avros.longs(), Avros.longs()), Avros.records(Cell.class)));

        nextGeneration = nextGeneration.filter(new EnsureAllAliveFn());

        //sort for printing
        //TODO easier for viewing but more complicated to do.

        return nextGeneration.values();
    }

    private PCollection<Cell> createInitial(Pipeline pipeline, long numCells){
        List<Cell> cells = new LinkedList<>();
        cells.add(Cell.newBuilder().setSelf(true).setX(1).setY(3).setGeneration(0).setAlive(true).build());
        cells.add(Cell.newBuilder().setSelf(true).setX(1).setY(2).setGeneration(0).setAlive(true).build());
        cells.add(Cell.newBuilder().setSelf(true).setX(1).setY(1).setGeneration(0).setAlive(true).build());

        return pipeline.create(cells, Avros.records(Cell.class));
    }

    private void printCells(PCollection<Cell> world){



    }
}
