package com.streaming.operators;

import com.streaming.DataStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import com.streaming.DataStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

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
                if (data == null) break;
                if (data instanceof String[]) {
                    String[] record = (String[]) data;
                    csvPrinter.printRecord(record[0], record[1]);
                }
            }
            csvPrinter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Operator<T> getOperator() {
        return this;
    }
}