package com.streaming.operators;

import com.streaming.functions.ReduceFunction;
import com.streaming.DataStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;

public class ReduceOperator<T> implements Operator<T> {
    private ReduceFunction<T> reduceFunction;

    public ReduceOperator(ReduceFunction<T> reduceFunction) {
        this.reduceFunction = reduceFunction;
    }

    @Override
    public void execute() {
        // Apply the reduce function to each partition
        Map<Object, T> reducedValues = new HashMap<>();
        while (true) {
            List<T> partition = (List<T>) DataStream.getLatestData();
            if (partition == null) break;
            T result = partition.stream().reduce((BinaryOperator<T>) reduceFunction).orElse(null);
            DataStream.addDataToBuffer(result);
        }
    }

    @Override
    public Operator<T> getOperator() {
        return this;
    }
}