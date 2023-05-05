/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.storage.hadoop.functions.ParquetOutputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.functions.PartOutputFunction;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;

@SuppressWarnings("unused")
public class HadoopParquetOutput extends HadoopOutput {
    protected Codec codec;
    protected String[] columns;

    @Override
    public AdapterMeta meta() {
        return new AdapterMeta("hadoopParquet", "File-based output adapter that utilizes Hadoop FileSystems." +
                " Supports Parquet files, optionally compressed",
                "Path examples: hdfs://output/into/parquet/files/",

                new StreamType[]{StreamType.PlainText, StreamType.Columnar},
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
    protected PartOutputFunction getOutputFunction(String sub) {
        return new ParquetOutputFunction(sub, path, codec, columns);
    }
}
