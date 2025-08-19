/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input.functions;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pastorgl.datacooker.data.Structured;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ParquetStructuredInputStream implements RecordInputStream {
    protected final List<String> attrs;
    protected final ParquetReader<GenericRecord> reader;
    private final ObjectMapper om;

    public ParquetStructuredInputStream(String inputFile, String[] _attrs) throws Exception {
        Path inputFilePath = new Path(inputFile);

        if (_attrs != null) {
            this.attrs = Arrays.asList(_attrs);
        } else {
            this.attrs = null;
        }

        this.reader = AvroParquetReader.<GenericRecord>builder(inputFilePath).build();
        this.om = new ObjectMapper();
        om.enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);
    }

    @Override
    public Structured ensureRecord() throws IOException {
        GenericRecord g = reader.read();

        if (g == null) {
            return null;
        } else {
            HashMap<?, ?> v = om.readValue(g.toString(), HashMap.class);

            if (attrs != null) {
                Structured struct = new Structured(attrs);
                for (String a : attrs) {
                    struct.put(a, v.get(a));
                }

                return struct;
            } else {
                return new Structured(v);
            }
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
