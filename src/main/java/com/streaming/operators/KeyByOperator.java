package com.streaming.operators;

import com.streaming.DataStream;
import com.streaming.functions.KeySelector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeyByOperator<T> implements Operator<T> {
    private KeySelector<T, ?> keySelector;

    public KeyByOperator(KeySelector<T, ?> keySelector) {
        this.keySelector = keySelector;
    }

    @Override
    public void execute() {
        System.out.println("KeyByOperator starting execution...");
        Map<Object, List<T>> partitions = new HashMap<>();

        while (true) {
            Object data = DataStream.getLatestData();
            if (data == null) {
                System.out.println("KeyByOperator: No more data to process.");
                break;
            }

            Object key = keySelector.getKey((T) data);
            if (key == null) {
                System.err.println("KeySelector returned null for data: " + data);
                continue;
            }

            System.out.println("KeyByOperator processing data: " + data + " with key: " + key);
            partitions.computeIfAbsent(key, k -> new ArrayList<>()).add((T) data);
        }

        for (Map.Entry<Object, List<T>> entry : partitions.entrySet()) {
            System.out.println("KeyByOperator output: " + entry.getKey() + " -> " + entry.getValue());
            DataStream.addDataToBuffer(entry.getValue());
        }
        System.out.println("KeyByOperator finished execution.");
    }

    @Override
    public Operator<T> getOperator() {
        return this;
    }
}
