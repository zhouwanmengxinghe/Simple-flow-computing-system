package com.streaming.functions;

public interface ReduceFunction<T> {
    T reduce(T value1, T value2);
}
