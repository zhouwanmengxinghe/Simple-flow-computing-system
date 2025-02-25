package com.streaming.distributed;

import com.streaming.DAG;
import com.streaming.operators.Operator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Master节点负责任务调度和资源管理
 */
public class Master {
    private final Map<String, WorkerInfo> workers;
    private final Map<String, TaskInfo> tasks;
    
    public Master() {
        this.workers = new ConcurrentHashMap<>();
        this.tasks = new ConcurrentHashMap<>();
    }
    
    /**
     * 注册Worker节点
     */
    public void registerWorker(String workerId, String host, int port) {
        workers.put(workerId, new WorkerInfo(workerId, host, port));
    }
    
    /**
     * 提交任务
     */
    public String submitTask(DAG dag) {
        String taskId = UUID.randomUUID().toString();
        TaskInfo taskInfo = new TaskInfo(taskId, dag);
        tasks.put(taskId, taskInfo);
        scheduleTask(taskInfo);
        return taskId;
    }
    
    /**
     * 调度任务到Worker节点
     */
    private void scheduleTask(TaskInfo taskInfo) {
        DAG dag = taskInfo.getDag();
        // 获取DAG中的所有算子节点
        List<Operator<?>> operators = new ArrayList<>(dag.getNodes());
        
        // 简单的轮询调度策略
        List<WorkerInfo> availableWorkers = new ArrayList<>(workers.values());
        if (availableWorkers.isEmpty()) {
            throw new RuntimeException("No available workers");
        }
        
        int workerIndex = 0;
        for (Operator<?> operator : operators) {
            WorkerInfo worker = availableWorkers.get(workerIndex % availableWorkers.size());
            taskInfo.assignOperator(operator, worker);
            workerIndex++;
        }
    }
    
    /**
     * Worker信息类
     */
    private static class WorkerInfo {
        private final String workerId;
        private final String host;
        private final int port;
        
        public WorkerInfo(String workerId, String host, int port) {
            this.workerId = workerId;
            this.host = host;
            this.port = port;
        }
    }
    
    /**
     * 任务信息类
     */
    private static class TaskInfo {
        private final String taskId;
        private final DAG dag;
        private final Map<Operator<?>, WorkerInfo> operatorAssignments;
        
        public TaskInfo(String taskId, DAG dag) {
            this.taskId = taskId;
            this.dag = dag;
            this.operatorAssignments = new HashMap<>();
        }
        
        public void assignOperator(Operator<?> operator, WorkerInfo worker) {
            operatorAssignments.put(operator, worker);
        }
        
        public DAG getDag() {
            return dag;
        }
    }
}