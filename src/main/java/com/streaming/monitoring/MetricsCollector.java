package com.streaming.monitoring;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsCollector {
    private static final MetricsCollector instance = new MetricsCollector();
    private final Map<String, AtomicLong> operatorProcessingTime;
    private final Map<String, AtomicLong> operatorRecordsProcessed;
    private final Map<String, AtomicLong> operatorErrors;

    private MetricsCollector() {
        operatorProcessingTime = new ConcurrentHashMap<>();
        operatorRecordsProcessed = new ConcurrentHashMap<>();
        operatorErrors = new ConcurrentHashMap<>();
    }

    public static MetricsCollector getInstance() {
        return instance;
    }

    public void recordProcessingTime(String operatorName, long processingTime) {
        operatorProcessingTime.computeIfAbsent(operatorName, k -> new AtomicLong())
                             .addAndGet(processingTime);
    }

    public void incrementRecordsProcessed(String operatorName) {
        operatorRecordsProcessed.computeIfAbsent(operatorName, k -> new AtomicLong())
                               .incrementAndGet();
    }

    public void incrementErrors(String operatorName) {
        operatorErrors.computeIfAbsent(operatorName, k -> new AtomicLong())
                      .incrementAndGet();
    }

    public Map<String, Long> getOperatorProcessingTimes() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        operatorProcessingTime.forEach((key, value) -> result.put(key, value.get()));
        return result;
    }

    public Map<String, Long> getOperatorRecordsProcessed() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        operatorRecordsProcessed.forEach((key, value) -> result.put(key, value.get()));
        return result;
    }

    public Map<String, Long> getOperatorErrors() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        operatorErrors.forEach((key, value) -> result.put(key, value.get()));
        return result;
    }

    public void reset() {
        operatorProcessingTime.clear();
        operatorRecordsProcessed.clear();
        operatorErrors.clear();
    }
}