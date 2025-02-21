package com.streaming.operators;

import com.streaming.DataStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

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
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path));
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("word", "count"))) {
            while (true) {
                T data = DataStream.getLatestData();
                if (data == null) {
                    System.out.println("No more data to process in SinkOperator.");
                    break;
                }

                if (!(data instanceof String[])) {
                    System.err.println("Expected String[] but got: " + data.getClass());
                    continue; // Skip this data if type mismatch
                }

                String[] record = (String[]) data;
                System.out.println("SinkOperator output: " + Arrays.toString(record));
                csvPrinter.printRecord(record[0], record[1]);
            }
            csvPrinter.flush();
            System.out.println("SinkOperator completed.");
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