package com.streaming;

import com.streaming.functions.KeySelector;
import com.streaming.functions.MapFunction;
import com.streaming.functions.ReduceFunction;
import com.streaming.operators.KeyByOperator;
import com.streaming.operators.MapOperator;
import com.streaming.operators.ReduceOperator;

import java.util.Arrays;
import java.util.List;

public class StreamProcessingTest {
    public static void main(String[] args) {
        List<String> mockData = Arrays.asList(
                "Apple,1",
                "Banana,1",
                "apple,1",
                "banana,1",
                "Apple,1",
                "Cherry,1"
        );

        DataStream<String> source = DataStream.fromKafka("wordcount-topic", mockData);

        DataStream<String[]> mapStream = source.addOperator(new MapOperator<>(value -> {
            String[] parts = ((String) value).split(",");
            if (parts.length < 2) {
                return new String[]{"", "0"};
            }
            return new String[]{parts[0].toLowerCase(), parts[1]};
        }));

        DataStream<String[]> keyByStream = mapStream.addOperator(new KeyByOperator<>(value -> {
            if (value.length < 2) {
                return "";
            }
            return value[0];
        }));

        DataStream<String[]> reduceStream = keyByStream.addOperator(new ReduceOperator<>((value1, value2) -> {
            int count1 = Integer.parseInt(value1[1]);
            int count2 = Integer.parseInt(value2[1]);
            return new String[]{value1[0], String.valueOf(count1 + count2)};
        }));

        reduceStream.writeAsText("output.csv");

        source.execute();
    }
}
