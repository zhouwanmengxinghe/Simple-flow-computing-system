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
        while (true) {
            T data = DataStream.getLatestData();
            if (data == null) {
                System.out.println("No more data to process in MapOperator.");
                break;
            }

            try {
                R result = mapFunction.map(data);
                if (result == null) {
                    System.err.println("MapFunction returned null for data: " + data);
                    continue; // Skip this data if result is null
                }

                System.out.println("MapOperator output: " + result);
                DataStream.addDataToBuffer(result);
            } catch (Exception e) {
                System.err.println("Error processing data in MapOperator: " + data);
                e.printStackTrace();
                continue; // Skip this data if an error occurs
            }
        }
    }

    @Override
    public Operator<T> getOperator() {
        return this;
    }
}