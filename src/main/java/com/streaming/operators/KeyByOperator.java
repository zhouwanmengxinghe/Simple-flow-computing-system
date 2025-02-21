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
        Map<Object, List<T>> partitions = new HashMap<>();

        // Partition data by key
        while (true) {
            T data = DataStream.getLatestData();
            if (data == null) {
                System.out.println("No more data to process in KeyByOperator.");
                break;
            }

            Object key = keySelector.getKey(data);
            if (key == null) {
                System.err.println("KeySelector returned null for data: " + data);
                continue; // Skip this data if key is null
            }

            System.out.println("KeyByOperator processing data: " + data + " with key: " + key);
            partitions.computeIfAbsent(key, k -> new ArrayList<>()).add(data);
        }

        // Pass partitions to the next operator
        for (Map.Entry<Object, List<T>> entry : partitions.entrySet()) {
            System.out.println("KeyByOperator output: " + entry.getKey() + " -> " + entry.getValue());
            // Ensure downstream operators can handle List<T> as input
            DataStream.addDataToBuffer(entry.getValue());
        }
    }

    @Override
    public Operator<T> getOperator() {
        return this;
    }
}