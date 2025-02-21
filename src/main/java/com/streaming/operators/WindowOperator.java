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
                    for (Map.Entry<Object, List<T>> entry : windowedData.entrySet()) {
                        System.out.println("WindowOperator output: " + entry.getKey() + " -> " + entry.getValue());
                        DataStream.addDataToBuffer(entry.getValue());
                    }
                    windowedData.clear();
                }
                lastTriggerTime = currentTime;
            }

            T data = (T) DataStream.getLatestData();
            if (data == null) {
                System.out.println("No more data to process in WindowOperator.");
                // Trigger a final window if there is any remaining data
                if (!windowedData.isEmpty()) {
                    for (Map.Entry<Object, List<T>> entry : windowedData.entrySet()) {
                        System.out.println("WindowOperator output: " + entry.getKey() + " -> " + entry.getValue());
                        DataStream.addDataToBuffer(entry.getValue());
                    }
                    windowedData.clear();
                }
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
