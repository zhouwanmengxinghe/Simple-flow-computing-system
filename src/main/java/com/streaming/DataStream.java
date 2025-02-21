package com.streaming;

import com.streaming.operators.Operator;
import com.streaming.operators.SinkOperator;
import com.streaming.operators.SourceOperator;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DataStream<T> {
    private DAG<T> dag;
    private Operator<T> lastOperator;
    private static final ConcurrentLinkedQueue<Object> dataBuffer = new ConcurrentLinkedQueue<>();
    public DataStream() {
        dag = new DAG<>();
        lastOperator = null;
    }

    public DataStream<T> addOperator(Operator<T> operator) {
        if (lastOperator != null) {
            dag.addEdge(lastOperator, operator);
        }
        dag.addNode(operator);
        lastOperator = operator;
        return this;
    }

    public void execute() {
        List<Operator<T>> sortedOperators = topologicalSort(dag);
        for (Operator<T> operator : sortedOperators) {
            operator.execute();
        }
    }

    private List<Operator<T>> topologicalSort(DAG<T> dag) {
        List<Operator<T>> sorted = new ArrayList<>();
        Set<Operator<T>> visited = new HashSet<>();
        for (Operator<T> node : dag.getAdjacencyList().keySet()) {
            if (!visited.contains(node)) {
                visit(node, visited, sorted, dag);
            }
        }
        return sorted;
    }

    private void visit(Operator<T> node, Set<Operator<T>> visited, List<Operator<T>> sorted, DAG<T> dag) {
        visited.add(node);
        for (Operator<T> neighbor : dag.getAdjacencyList().get(node)) {
            if (!visited.contains(neighbor)) {
                visit(neighbor, visited, sorted, dag);
            }
        }
        sorted.add(node);
    }

    public static <T> DataStream<T> fromKafka(String topic) {
        DataStream<T> stream = new DataStream<>();
        SourceOperator<T> sourceOperator = new SourceOperator<>(topic);
        stream.addOperator(sourceOperator);
        return stream;
    }

    public void writeAsText(String path) {
        SinkOperator<T> sinkOperator = new SinkOperator<>(path);
        addOperator(sinkOperator);
    }

    // 添加数据到缓冲区
    public static void addDataToBuffer(Object data) {
        dataBuffer.add(data);
    }

    // 从缓冲区获取最新数据
    @SuppressWarnings("unchecked")
    public static <R> R getLatestData() {
        return (R) dataBuffer.poll(); // 从缓冲区中取出并移除最新的数据
    }
}