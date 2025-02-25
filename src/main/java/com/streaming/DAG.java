package com.streaming;

import com.streaming.operators.Operator;

import java.util.*;

public class DAG<T> {
    private Map<Operator<T>, List<Operator<T>>> adjacencyList;

    public DAG() {
        adjacencyList = new HashMap<>();
    }

    public void addNode(Operator<T> node) {
        adjacencyList.putIfAbsent(node, new ArrayList<>());
    }

    public void addEdge(Operator<T> from, Operator<T> to) {
        adjacencyList.get(from).add(to);
    }

    public Set<Operator<T>> getNodes() {
        return adjacencyList.keySet();
    }

    public Map<Operator<T>, List<Operator<T>>> getAdjacencyList() {
        return adjacencyList;
    }
}