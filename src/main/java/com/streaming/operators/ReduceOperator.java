package com.streaming.operators;

import com.streaming.DataStream;
import com.streaming.functions.ReduceFunction;

import java.util.List;

public class ReduceOperator<T> implements Operator<T> {
    private ReduceFunction<T> reduceFunction;

    public ReduceOperator(ReduceFunction<T> reduceFunction) {
        this.reduceFunction = reduceFunction;
    }

    @Override
    public void execute() {
        while (true) {
            Object data = DataStream.getLatestData();
            if (data == null) {
                System.out.println("No more data to process in ReduceOperator.");
                break;
            }

            if (!(data instanceof List)) {
                System.err.println("Expected List<T> but got: " + data.getClass());
                continue;
            }

            List<T> partition = (List<T>) data;
            System.out.println("ReduceOperator processing partition: " + partition);

            try {
                T result = partition.stream().reduce(reduceFunction).orElse(null);
                if (result == null) {
                    System.err.println("ReduceFunction returned null for partition: " + partition);
                    continue; // Skip this partition if result is null
                }

                System.out.println("ReduceOperator output: " + result);
                DataStream.addDataToBuffer(result);
            } catch (Exception e) {
                System.err.println("Error processing partition in ReduceOperator: " + partition);
                e.printStackTrace();
                continue; // Skip this partition if an error occurs
            }
        }
    }

    @Override
    public Operator<T> getOperator() {
        return this;
    }
}