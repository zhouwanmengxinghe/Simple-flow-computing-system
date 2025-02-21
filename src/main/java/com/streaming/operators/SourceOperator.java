package com.streaming.operators;

import com.streaming.DataStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class SourceOperator<T> implements Operator<T> {
    private final boolean isKafkaMode;
    private final String topic;
    private final List<String> mockData;

    public SourceOperator(String topic) {
        this.isKafkaMode = true;
        this.topic = topic;
        this.mockData = null;
    }

    public SourceOperator(List<String> mockData) {
        this.isKafkaMode = false;
        this.topic = null;
        this.mockData = mockData;
    }

    @Override
    public void execute() {
        if (isKafkaMode) {
            executeKafkaMode();
        } else {
            executeMockMode();
        }
    }

    private void executeKafkaMode() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String value = record.value();
                    if (value != null) {
                        System.out.println("SourceOperator output: " + value);
                        DataStream.addDataToBuffer(value);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error reading from Kafka topic: " + topic);
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    private void executeMockMode() {
        if (mockData == null || mockData.isEmpty()) {
            System.err.println("Mock data is empty or null.");
            return;
        }

        for (String data : mockData) {
            System.out.println("SourceOperator output: " + data);
            DataStream.addDataToBuffer(data);
        }
    }

    @Override
    public Operator<T> getOperator() {
        return this;
    }
}