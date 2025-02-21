package com.streaming.operators;

import com.streaming.functions.MapFunction;
import com.streaming.DataStream;
public class MapOperator<T, R> implements Operator<T> {
    private MapFunction<T, R> mapFunction;

    public MapOperator(MapFunction<T, R> mapFunction) {
        this.mapFunction = mapFunction;
    }

    @Override
    public void execute() {
        // Apply the map function to each element
        // Here you can process the data and pass it to the next operator
        while (true) {
            T data = DataStream.getLatestData();
            if (data == null) break;
            R result = mapFunction.map(data);
            DataStream.addDataToBuffer(result);
        }
    }

    @Override
    public Operator<T> getOperator() {
        return this;
    }
}