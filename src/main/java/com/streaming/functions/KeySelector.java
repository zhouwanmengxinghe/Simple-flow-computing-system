package com.streaming.functions;

public interface KeySelector<T, K> {
    K getKey(T value);


}

