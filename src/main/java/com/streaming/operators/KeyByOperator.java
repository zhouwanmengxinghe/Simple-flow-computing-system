package com.streaming.operators;

import com.streaming.functions.KeySelector;
import com.streaming.DataStream;

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
        // Partition data by key
        Map<Object, List<T>> partitions = new HashMap<>();
        while (true) {
            T data = DataStream.getLatestData();
            if (data == null) break;
            Object key = keySelector.getKey(data);
            partitions.computeIfAbsent(key, k -> new ArrayList<>()).add(data);
        }
        // Pass partitions to the next operator
        for (List<T> partition : partitions.values()) {
            DataStream.addDataToBuffer(partition);
        }
    }

    @Override
    public Operator<T> getOperator() {
        return this;
    }
}