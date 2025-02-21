package com.streaming.operators;

import com.streaming.DataStream;
import java.util.*;

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
                for (List<T> data : windowedData.values()) {
                    // 将窗口数据传递给下一个算子
                    DataStream.addDataToBuffer(data);
                }
                windowedData.clear();
                lastTriggerTime = currentTime;
            }
            T data = (T) DataStream.getLatestData();
            if (data != null) {
                Object key = getKey(data);
                windowedData.computeIfAbsent(key, k -> new ArrayList<>()).add(data);
            }
        }
    }

    private String getKey(T data) {
        // 假设数据是一个字符串数组，key 是第一个元素
        if (data instanceof String[]) {
            return ((String[]) data)[0];
        }
        return null;
    }
}