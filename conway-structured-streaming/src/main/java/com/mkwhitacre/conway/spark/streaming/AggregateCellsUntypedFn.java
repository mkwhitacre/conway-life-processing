package com.mkwhitacre.conway.spark.streaming;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class AggregateCellsUntypedFn extends UserDefinedAggregateFunction{

    private StructType inputSchema;

    @Override
    public StructType inputSchema() {

        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("alive", DataTypes.BooleanType, false));
        inputFields.add(DataTypes.createStructField("x", DataTypes.LongType, false));
        inputFields.add(DataTypes.createStructField("y", DataTypes.LongType, false));
        inputSchema = DataTypes.createStructType(inputFields);

        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return inputSchema;
    }

    @Override
    public DataType dataType() {
        return inputSchema;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, false);
        buffer.update(1, Long.MIN_VALUE);
        buffer.update(2, Long.MIN_VALUE);
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        boolean cellAlive = input.getBoolean(0);
        long cellX = input.getLong(1);
        long cellY = input.getLong(2);

        boolean isAlive = buffer.getBoolean(0) || cellAlive;
        long x = Math.max(buffer.getLong(1), cellX);
        long y = Math.max(buffer.getLong(2), cellY);

        buffer.update(0, isAlive);
        buffer.update(1, x);
        buffer.update(2, y);
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        update(buffer1, buffer2);
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer;
    }
}
