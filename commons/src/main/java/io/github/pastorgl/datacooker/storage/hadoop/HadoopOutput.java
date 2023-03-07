/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.storage.OutputAdapter;
import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;

public class HadoopOutput extends OutputAdapter {
    protected HadoopStorage.Codec codec;
    protected String[] columns;
    protected String delimiter;

    @Override
    public AdapterMeta meta() {
        return new AdapterMeta("hadoop", "File-based output adapter that utilizes Hadoop FileSystems." +
                " Supports plain text, delimited text (CSV/TSV), and Parquet files, optionally compressed",
                "Path examples: file:/mnt/path/to/output, hdfs://output/into/parquet/files/.parquet," +
                        " s3://bucket/and/key_prefix",

                new StreamType[]{StreamType.PlainText, StreamType.Columnar},
                new DefinitionMetaBuilder()
                        .def(CODEC, "Codec to compress the output", HadoopStorage.Codec.class, HadoopStorage.Codec.NONE,
                                "By default, use no compression")
                        .def(COLUMNS, "Columns to write",
                                String[].class, null, "By default, select all columns")
                        .def(DELIMITER, "Record column delimiter",
                                String.class, "\t", "By default, tabulation character")
                        .build()
        );
    }

    protected void configure() throws InvalidConfigurationException {
        codec = resolver.get(CODEC);

        columns = resolver.get(COLUMNS);
        delimiter = resolver.get(DELIMITER);
    }

    @Override
    public void save(String sub, DataStream rdd) {
        Function2<Integer, Iterator<Record>, Iterator<Void>> outputFunction = new PartOutputFunction(sub, path, codec, columns, delimiter.charAt(0));

        rdd.get().mapPartitionsWithIndex(outputFunction, true).count();
    }
}
