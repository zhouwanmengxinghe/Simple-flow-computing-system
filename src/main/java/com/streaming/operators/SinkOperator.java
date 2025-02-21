package com.streaming.operators;

import com.streaming.DataStream;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class SinkOperator<T> implements Operator<T> {
    private String path;

    public SinkOperator(String path) {
        this.path = path;
    }

    @Override
    public void execute() {
        System.out.println("SinkOperator starting execution...");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            while (true) {
                Object data = DataStream.getLatestData();
                if (data == null) {
                    System.out.println("SinkOperator: No more data to process.");
                    break;
                }

                if (!(data instanceof String[])) {
                    System.err.println("Expected String[] but got: " + data.getClass());
                    continue;
                }

                String[] record = (String[]) data;
                System.out.println("SinkOperator output: " + Arrays.toString(record));
                writer.write(record[0] + "," + record[1]);
                writer.newLine();
            }
            writer.flush();
            System.out.println("SinkOperator finished execution.");
        } catch (IOException e) {
            System.err.println("Error writing to file: " + path);
            e.printStackTrace();
        }
    }

    @Override
    public Operator<T> getOperator() {
        return this;
    }
}
