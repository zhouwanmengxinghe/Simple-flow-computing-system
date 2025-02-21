package com.streaming;

import com.streaming.functions.KeySelector;
import com.streaming.functions.MapFunction;
import com.streaming.functions.ReduceFunction;
import com.streaming.operators.KeyByOperator;
import com.streaming.operators.MapOperator;
import com.streaming.operators.ReduceOperator;

public class WordCountApplication {
    public static void main(String[] args) {
        // 从 Kafka 读取数据
        DataStream<String> source = DataStream.fromKafka("wordcount-topic");

        // 提取单词并转换为小写
        DataStream<String[]> mapStream = (DataStream<String[]>) source.addOperator(new MapOperator<>(value -> {
            String[] parts = value.split(",");
            return new String[]{parts[0].toLowerCase(), parts[1]};
        }));

        // 按单词分组
        DataStream<String[]> keyByStream = mapStream.addOperator(new KeyByOperator<>(value -> value[0]));

        // 累加单词计数
        DataStream<String[]> reduceStream = keyByStream.addOperator(new ReduceOperator<>((String[] value1, String[] value2) -> {
            int count1 = Integer.parseInt(value1[1]);
            int count2 = Integer.parseInt(value2[1]);
            return new String[]{value1[0], String.valueOf(count1 + count2)};
        }));

        // 输出结果到文件
        reduceStream.writeAsText("output.csv");

        // 执行DAG
        source.execute();
    }
}