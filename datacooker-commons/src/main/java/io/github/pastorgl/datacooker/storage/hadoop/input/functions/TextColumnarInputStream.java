/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input.functions;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import io.github.pastorgl.datacooker.Constants;
import io.github.pastorgl.datacooker.data.Columnar;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class TextColumnarInputStream implements RecordInputStream {
    protected final int[] order;
    protected final BufferedReader reader;
    protected final CSVParser parser;
    protected final List<String> columns;

    public TextColumnarInputStream(InputStream input, char delimiter, String[] _columns) {
        int[] columnOrder;

        this.reader = new BufferedReader(new InputStreamReader(input));
        this.parser = new CSVParserBuilder().withSeparator(delimiter).build();
        String[] schema = new String[0];
        try {
            String line = reader.readLine();

            if (line != null) {
                schema = parser.parseLine(line);
            }
        } catch (Exception ignore) {
        }

        String[] _schema = schema;
        if (_columns == null) {
            columnOrder = IntStream.range(0, _schema.length).filter(i -> !Constants.UNDERSCORE.equals(_schema[i])).toArray();
            _columns = _schema;
        } else {
            Map<String, Integer> sch = new HashMap<>();
            for (int i = 0; i < _schema.length; i++) {
                sch.put(_schema[i], i);
            }

            Map<Integer, String> columns = new HashMap<>();
            int j = 0;
            for (String column : _columns) {
                if (!Constants.UNDERSCORE.equals(column)) {
                    columns.put(j, column);
                    j++;
                }
            }

            columnOrder = new int[_columns.length];
            for (int i = 0; i < _columns.length; i++) {
                columnOrder[i] = sch.get(columns.get(i));
            }
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
