/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.functions;

import io.github.pastorgl.datacooker.data.Columnar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class ParquetColumnarStream implements RecordStream {
    private final int[] order;
    private final List<String> columns;
    private final ParquetReader<Group> reader;

    public ParquetColumnarStream(Configuration conf, String inputFile, String[] _columns) throws Exception {
        Path inputFilePath = new Path(inputFile);

        ParquetMetadata readFooter = ParquetFileReader.readFooter(HadoopInputFile.fromPath(inputFilePath, conf), ParquetMetadataConverter.NO_FILTER);
        MessageType schema = readFooter.getFileMetaData().getSchema();

        int[] fieldOrder;
        if (_columns != null) {
            fieldOrder = new int[_columns.length];

            for (int i = 0; i < _columns.length; i++) {
                String column = _columns[i];
                fieldOrder[i] = schema.getFieldIndex(column);
            }
        } else {
            fieldOrder = IntStream.range(0, schema.getFieldCount()).toArray();
            _columns = schema.getColumns().stream().map(cd -> String.join(".", cd.getPath())).toArray(String[]::new);
        }

        this.columns = Arrays.asList(_columns);

        GroupReadSupport readSupport = new GroupReadSupport();
        readSupport.init(conf, null, schema);
        this.reader = ParquetReader.builder(readSupport, inputFilePath).build();
        this.order = fieldOrder;
    }

    @Override
    public Columnar ensureRecord() throws IOException {
        Group g = reader.read();

        if (g == null) {
            return null;
        } else {
            String[] acc = new String[order.length];

            for (int i = 0; i < order.length; i++) {
                int l = order[i];
                acc[i] = g.getValueToString(l, 0);
            }

            return new Columnar(columns, acc);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
