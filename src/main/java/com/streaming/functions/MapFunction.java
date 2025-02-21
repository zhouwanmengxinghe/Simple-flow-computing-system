package com.streaming.functions;

public interface MapFunction<T, R> {
    R map(T value);
}
