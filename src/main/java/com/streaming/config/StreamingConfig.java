package com.streaming.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class StreamingConfig {
    private static final Properties properties = new Properties();
    private static StreamingConfig instance;

    private StreamingConfig() {}

    public static StreamingConfig getInstance() {
        if (instance == null) {
            synchronized (StreamingConfig.class) {
                if (instance == null) {
                    instance = new StreamingConfig();
                }
            }
        }
        return instance;
    }

    public void loadConfig(String configPath) throws IOException {
        try (FileInputStream fis = new FileInputStream(configPath)) {
            properties.load(fis);
        }
    }

    public String getKafkaBootstrapServers() {
        return properties.getProperty("kafka.bootstrap.servers", "localhost:9092");
    }

    public String getKafkaGroupId() {
        return properties.getProperty("kafka.group.id", "streaming-group");
    }

    public int getBufferSize() {
        return Integer.parseInt(properties.getProperty("buffer.size", "1000"));
    }

    public int getParallelism() {
        return Integer.parseInt(properties.getProperty("parallelism", "1"));
    }

    public String getLogLevel() {
        return properties.getProperty("log.level", "INFO");
    }

    public String getOutputPath() {
        return properties.getProperty("output.path", "output");
    }
}