/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.output;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OutputAdapterMeta;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.ColumnarParquetOutputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.OutputFunction;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;

@SuppressWarnings("unused")
public class ColumnarParquetOutput extends HadoopOutput {
    protected Codec codec;
    protected String[] columns;

    @Override
    public OutputAdapterMeta meta() {
        return new OutputAdapterMeta("columnarParquet", "File-based output adapter that utilizes Hadoop FileSystems." +
                " Supports Parquet files, optionally compressed",
                new String[]{"hdfs:///output/into/parquet/files/", "file:/mnt/storage/output/for/parquet/", "s3://bucket/prefix/"},

                new StreamType[]{StreamType.Columnar},
                new DefinitionMetaBuilder()
                        .def(CODEC, "Codec to compress the output", Codec.class, Codec.NONE,
                                "By default, use no compression")
                        .def(COLUMNS, "Columns to write",
                                String[].class, null, "By default, select all columns")
                        .build()
        );
    }

    protected void configure() throws InvalidConfigurationException {
        super.configure();

        columns = resolver.get(COLUMNS);
    }

    @Override
    protected OutputFunction getOutputFunction(String sub) {
        return new ColumnarParquetOutputFunction(sub, path, codec, columns);
    }
}
