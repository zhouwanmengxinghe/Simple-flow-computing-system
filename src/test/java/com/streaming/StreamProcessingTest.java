package com.streaming;

import com.streaming.functions.KeySelector;
import com.streaming.functions.MapFunction;
import com.streaming.functions.ReduceFunction;
import com.streaming.operators.KeyByOperator;
import com.streaming.operators.MapOperator;
import com.streaming.operators.ReduceOperator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StreamProcessingTest {
    private static final List<String> mockKafkaData = List.of(
            "Apple,1",
            "Banana,1",
            "apple,1",
            "banana,1",
            "Apple,1",
            "Cherry,1"
    );

    public static void main(String[] args) {
        // 创建DataStream，从Kafka主题读取数据（使用模拟数据）
        DataStream<String> source = DataStream.fromKafka("wordcount-topic", mockKafkaData);

        // 提取单词并转换为小写
        DataStream<String[]> mapStream = source.addOperator(new MapOperator<>(value -> {
            String[] parts = value.split(",");
            return new String[]{parts[0].toLowerCase(), parts[1]};
        }));

        // 按单词分组
        DataStream<String[]> keyByStream = mapStream.addOperator(new KeyByOperator<>(value -> value[0]));

        // 累加单词计数
        DataStream<String[]> reduceStream = keyByStream.addOperator(new ReduceOperator<>((value1, value2) -> {
            int count1 = Integer.parseInt(value1[1]);
            int count2 = Integer.parseInt(value2[1]);
            return new String[]{value1[0], String.valueOf(count1 + count2)};
        }));

        // 输出结果到文件
        reduceStream.writeAsText("output.csv");

        // 执行DAG
        source.execute();

        // 验证输出结果
        verifyOutput("output.csv");
    }

    private static void verifyOutput(String filePath) {
        List<String> expectedResults = List.of(
                "apple,3",
                "banana,2",
                "cherry,1"
        );

        List<String> actualResults = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                actualResults.add(line);
            }
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + filePath);
            return;
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // 检查结果是否符合预期
        if (actualResults.containsAll(expectedResults) && expectedResults.containsAll(actualResults)) {
            System.out.println("Test passed! Output matches expected results.");
        } else {
            System.out.println("Test failed! Output does not match expected results.");
            System.out.println("Expected: " + expectedResults);
            System.out.println("Actual: " + actualResults);
        }
    }
}