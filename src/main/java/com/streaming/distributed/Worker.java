package com.streaming.distributed;

import com.streaming.operators.Operator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Worker节点负责执行具体的算子任务和数据传输
 */
public class Worker {
    private final String workerId;
    private final String host;
    private final int port;
    private final ExecutorService executorService;
    private final Map<String, OperatorTask<?>> runningTasks;
    
    public Worker(String workerId, String host, int port) {
        this.workerId = workerId;
        this.host = host;
        this.port = port;
        this.executorService = Executors.newCachedThreadPool();
        this.runningTasks = new ConcurrentHashMap<>();
    }
    
    /**
     * 执行算子任务
     */
    public void executeOperator(String taskId, Operator<?> operator) {
        OperatorTask<?> task = new OperatorTask<>(taskId, operator);
        runningTasks.put(taskId, task);
        executorService.submit(task);
    }
    
    /**
     * 发送数据到下游Worker
     */
    public void sendData(String taskId, String targetWorkerId, Object data) {
        // TODO: 实现数据传输逻辑
        // 可以使用Socket或其他网络通信方式
    }
    
    /**
     * 接收上游Worker发送的数据
     */
    public void receiveData(String taskId, Object data) {
        OperatorTask<?> task = runningTasks.get(taskId);
        if (task != null) {
            task.processData(data);
        }
    }
    
    /**
     * 算子任务类
     */
    private static class OperatorTask<T> implements Runnable {
        private final String taskId;
        private final Operator<T> operator;
        private final Queue<Object> dataQueue;
        
        public OperatorTask(String taskId, Operator<T> operator) {
            this.taskId = taskId;
            this.operator = operator;
            this.dataQueue = new LinkedList<>();
        }
        
        public void processData(Object data) {
            synchronized (dataQueue) {
                dataQueue.offer(data);
                dataQueue.notify();
            }
        }
        
        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    Object data;
                    synchronized (dataQueue) {
                        while (dataQueue.isEmpty()) {
                            dataQueue.wait();
                        }
                        data = dataQueue.poll();
                    }
                    
                    // 执行算子处理逻辑
                    // TODO: 实现具体的数据处理和shuffle逻辑
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    public void shutdown() {
        executorService.shutdown();
    }
}