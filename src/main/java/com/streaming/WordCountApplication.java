package com.streaming;

import com.streaming.functions.KeySelector;
import com.streaming.functions.MapFunction;
import com.streaming.functions.ReduceFunction;
import com.streaming.operators.KeyByOperator;
import com.streaming.operators.MapOperator;
import com.streaming.operators.ReduceOperator;

import java.util.Arrays;
import java.util.List;

public class WordCountApplication {
    public static void main(String[] args) {
        List<String> mockData = Arrays.asList(
                "Apple,1",
                "Banana,1",
                "apple,1",
                "banana,1",
                "Apple,1",
                "Cherry,1"
        );

        // 创建DataStream，从Kafka主题读取数据（使用模拟数据）
        DataStream<String> source = DataStream.fromKafka("wordcount-topic", mockData);

        // 提取单词并转换为小写
        DataStream<String[]> mapStream = source.addOperator(new MapOperator<>((value) -> {
            System.out.println("MapFunction input: " + value);
            String[] parts = value.split(",");
            if (parts.length < 2) {
                System.err.println("Invalid data format: " + value);
                return new String[]{"", "0"}; // 返回默认值
            }
            String[] result = new String[]{parts[0].toLowerCase(), parts[1]};
            System.out.println("MapFunction output: " + Arrays.toString(result));
            return result;
        }));

        // 按单词分组
        DataStream<String[]> keyByStream = mapStream.addOperator(new KeyByOperator<>((value) -> {
            System.out.println("KeyByOperator input: " + Arrays.toString(value));
            if (value.length < 2) {
                System.err.println("Invalid data format: " + Arrays.toString(value));
                return ""; // 返回默认键
            }
            return value[0];
        }));

        // 累加单词计数
        DataStream<String[]> reduceStream = keyByStream.addOperator(new ReduceOperator<>((value1, value2) -> {
            System.out.println("ReduceOperator input: " + Arrays.toString(value1) + " and " + Arrays.toString(value2));
            if (value1.length < 2 || value2.length < 2) {
                System.err.println("Invalid data format: " + Arrays.toString(value1) + " or " + Arrays.toString(value2));
                return new String[]{"", "0"}; // 返回默认值
            }
            int count1 = Integer.parseInt(value1[1]);
            int count2 = Integer.parseInt(value2[1]);
            String[] result = new String[]{value1[0], String.valueOf(count1 + count2)};
            System.out.println("ReduceOperator output: " + Arrays.toString(result));
            return result;
        }));

        // 输出结果到文件
        reduceStream.writeAsText("output.csv");

        // 执行DAG
        try {
            source.execute();
        } catch (Exception e) {
            System.err.println("Error executing WordCountApplication.");
            e.printStackTrace();
        }
    }
}