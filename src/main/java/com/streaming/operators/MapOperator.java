package com.streaming.operators;

import com.streaming.DataStream;
import com.streaming.functions.MapFunction;

public class MapOperator<T, R> implements Operator<T> {
    private MapFunction<T, R> mapFunction;

    public MapOperator(MapFunction<T, R> mapFunction) {
        this.mapFunction = mapFunction;
    }

    @Override
    public void execute() {
        System.out.println("MapOperator starting execution...");
        while (true) {
            Object data = DataStream.getLatestData();
            if (data == null) {
                System.out.println("MapOperator: No more data to process.");
                break;
            }

            try {
                R result = mapFunction.map((T) data);
                if (result == null) {
                    System.err.println("MapFunction returned null for data: " + data);
                    continue;
                }

                System.out.println("MapOperator output: " + result);
                DataStream.addDataToBuffer(result);
            } catch (Exception e) {
                System.err.println("Error processing data in MapOperator: " + data);
                e.printStackTrace();
                continue;
            }
        }
        System.out.println("MapOperator finished execution.");
    }

    @Override
    public Operator<T> getOperator() {
        return this;
    }
}
