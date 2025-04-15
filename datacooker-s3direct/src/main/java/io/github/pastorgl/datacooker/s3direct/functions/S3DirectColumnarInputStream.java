/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.s3direct.functions;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import io.github.pastorgl.datacooker.data.Columnar;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.RecordInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class S3DirectColumnarInputStream implements RecordInputStream {
    private final int[] order;
    private final BufferedReader reader;
    private final CSVParser parser;
    private final List<String> columns;

    public S3DirectColumnarInputStream(InputStream input, char delimiter, boolean _fromFile, String[] _columns) {
        int[] columnOrder;

        String[] _schema = null;
        this.reader = new BufferedReader(new InputStreamReader(input));
        this.parser = new CSVParserBuilder().withSeparator(delimiter).build();
        if (_fromFile) {
            try {
                String line = reader.readLine();

                if (line != null) {
                    _schema = parser.parseLine(line);
                }
            } catch (Exception ignore) {
            }
        }

        if (_schema != null) {
            if (_columns == null) {
                columnOrder = IntStream.range(0, _schema.length).toArray();
                _columns = _schema;
            } else {
                Map<String, Integer> schema = new HashMap<>();
                for (int i = 0; i < _schema.length; i++) {
                    schema.put(_schema[i], i);
                }

                Map<Integer, String> columns = new HashMap<>();
                for (int i = 0; i < _columns.length; i++) {
                    columns.put(i, _columns[i]);
                }

                columnOrder = new int[_columns.length];
                for (int i = 0; i < _columns.length; i++) {
                    columnOrder[i] = schema.get(columns.get(i));
                }
            }
        } else {
            columnOrder = IntStream.range(0, _columns.length).toArray();
        }

        this.columns = Arrays.asList(_columns);
        this.order = columnOrder;
    }

    public Columnar ensureRecord() throws IOException {
        String line = reader.readLine();

        if (line == null) {
            return null;
        }

        try {
            String[] ll = parser.parseLine(line);
            String[] acc = new String[order.length];

            for (int i = 0; i < order.length; i++) {
                int l = order[i];
                acc[i] = ll[l];
            }

            return new Columnar(columns, acc);
        } catch (Exception e) {
            throw new IOException("Malformed input line: " + line, e);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
