/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.output;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OutputAdapterMeta;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.ColumnarParquetOutputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.OutputFunction;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;

@SuppressWarnings("unused")
public class ColumnarParquetOutput extends HadoopOutput {
    protected Codec codec;
    protected String[] columns;

    @Override
    public OutputAdapterMeta initMeta() {
        return new OutputAdapterMeta("columnarParquet", "File-based output adapter that utilizes Hadoop FileSystems." +
                " Supports Parquet files, optionally compressed",
                new String[]{"hdfs:///output/into/parquet/files/", "file:/mnt/storage/output/for/parquet/", "s3://bucket/prefix/"},

                StreamType.COLUMNAR,
                new DefinitionMetaBuilder()
                        .def(CODEC, "Codec to compress the output", Codec.class, Codec.NONE,
                                "By default, use no compression")
                        .def(COLUMNS, "Columns to write",
                                Object[].class, null, "By default, select all columns")
                        .build()
        );
    }

    protected void configure(Configuration params) throws InvalidConfigurationException {
        super.configure(params);

        Object[] cols = params.get(COLUMNS);
        if (cols != null) {
            columns = Arrays.stream(cols).map(String::valueOf).toArray(String[]::new);
        }
    }

    @Override
    protected OutputFunction getOutputFunction(String sub) {
        String confXml = "";
        try {
            StringWriter sw = new StringWriter();
            context.hadoopConfiguration().writeXml(sw);
            confXml = sw.toString();
        } catch (IOException ignored) {
        }

        return new ColumnarParquetOutputFunction(sub, path, codec, confXml, columns);
    }
}
