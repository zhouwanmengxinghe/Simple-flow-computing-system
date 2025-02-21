package com.streaming.operators;

public interface Operator<T> {
    void execute();
    default Operator<T> getOperator() {
        return this;
    }
}
