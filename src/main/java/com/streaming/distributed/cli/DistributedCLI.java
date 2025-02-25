package com.streaming.distributed.cli;

import com.streaming.DAG;
import com.streaming.distributed.Master;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * 分布式流计算系统的命令行接口
 */
public class DistributedCLI {
    private final Master master;
    private final Properties config;
    
    public DistributedCLI(String configPath) throws IOException {
        this.master = new Master();
        this.config = loadConfig(configPath);
        initializeCluster();
    }
    
    /**
     * 加载集群配置
     */
    private Properties loadConfig(String configPath) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(configPath)) {
            props.load(fis);
        }
        return props;
    }
    
    /**
     * 初始化集群（注册Worker节点）
     */
    private void initializeCluster() {
        String[] workers = config.getProperty("workers").split(",");
        for (String worker : workers) {
            String[] parts = worker.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            master.registerWorker(host + ":" + port, host, port);
        }
    }
    
    /**
     * 提交流计算应用
     */
    public String submitApplication(DAG dag) {
        return master.submitTask(dag);
    }
    
    /**
     * 主函数，处理命令行参数
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java DistributedCLI <config_path> <application_class>");
            System.exit(1);
        }
        
        try {
            String configPath = args[0];
            String applicationClass = args[1];
            
            // 初始化CLI
            DistributedCLI cli = new DistributedCLI(configPath);
            
            // 加载并实例化应用类
            Class<?> clazz = Class.forName(applicationClass);
            Object app = clazz.getDeclaredConstructor().newInstance();
            
            // 获取DAG并提交任务
            if (app instanceof FlowApplication) {
                FlowApplication flowApp = (FlowApplication) app;
                DAG dag = flowApp.createDAG();
                String taskId = cli.submitApplication(dag);
                System.out.println("Successfully submitted application with task ID: " + taskId);
            } else {
                System.err.println("Error: Application class must implement FlowApplication interface");
                System.exit(1);
            }
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

/**
 * 流计算应用接口
 */
interface FlowApplication {
    DAG createDAG();
}