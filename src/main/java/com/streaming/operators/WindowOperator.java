package com.streaming.operators;

import com.streaming.DataStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WindowOperator<T> implements Operator<T> {
    private long windowSize;

    public WindowOperator(long windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public void execute() {
        Map<Object, List<T>> windowedData = new HashMap<>();
        long lastTriggerTime = System.currentTimeMillis();

        while (true) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastTriggerTime >= windowSize) {
                if (!windowedData.isEmpty()) {
                    for (List<T> data : windowedData.values()) {
                        System.out.println("WindowOperator output: " + data);
                        DataStream.addDataToBuffer(data);
                    }
                    windowedData.clear();
                }
                lastTriggerTime = currentTime;
            }

            T data = (T) DataStream.getLatestData();
            if (data == null) {
                System.out.println("No more data to process in WindowOperator.");
                break;
            }

            Object key = getKey(data);
            if (key == null) {
                System.err.println("WindowOperator: getKey returned null for data: " + data);
                continue; // Skip this data if key is null
            }

            windowedData.computeIfAbsent(key, k -> new ArrayList<>()).add(data);
        }
    }

    private Object getKey(T data) {
        // 假设数据是一个字符串数组，key 是第一个元素
        if (data instanceof String[]) {
            return ((String[]) data)[0];
        }
        return null;
    }

    @Override
    public Operator<T> getOperator() {
        return this;
    }
}