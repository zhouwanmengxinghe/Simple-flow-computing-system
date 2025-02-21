package com.streaming;

import com.streaming.operators.Operator;
import com.streaming.operators.SinkOperator;
import com.streaming.operators.SourceOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DataStream<T> {
    private DAG<T> dag;
    private Operator<T> lastOperator;
    private static final ConcurrentLinkedQueue<Object> dataBuffer = new ConcurrentLinkedQueue<>();

    public DataStream() {
        dag = new DAG<>();
        lastOperator = null;
    }

    public <R> DataStream<R> addOperator(Operator<T> operator) {
        if (lastOperator != null) {
            dag.addEdge(lastOperator, operator);
        }
        dag.addNode(operator);
        lastOperator = operator;
        return new DataStream<>();
    }

    public void execute() {
        List<Operator<T>> sortedOperators = topologicalSort(dag);
        for (Operator<T> operator : sortedOperators) {
            System.out.println("Executing Operator: " + operator.getClass().getSimpleName());
            try {
                operator.execute();
            } catch (Exception e) {
                System.err.println("Error executing operator: " + operator.getClass().getName());
                e.printStackTrace();
            }
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
        System.out.println("Topological Order: " + sorted);
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

    public static <T> DataStream<T> fromKafka(String topic, List<String> mockData) {
        DataStream<T> stream = new DataStream<>();
        SourceOperator<T> sourceOperator = new SourceOperator<>((List<String>) mockData);
        stream.addOperator(sourceOperator);
        return stream;
    }

    public void writeAsText(String path) {
        SinkOperator<T> sinkOperator = new SinkOperator<>(path);
        addOperator(sinkOperator);
    }

    public static void addDataToBuffer(Object data) {
        System.out.println("Adding data to buffer: " + data);
        dataBuffer.add(data);
    }

    @SuppressWarnings("unchecked")
    public static Object getLatestData() {
        Object data = dataBuffer.poll();
        return data;
    }
}
