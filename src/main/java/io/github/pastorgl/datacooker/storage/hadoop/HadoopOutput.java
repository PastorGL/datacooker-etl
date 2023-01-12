/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop;

import io.github.pastorgl.datacooker.data.BinRec;
import io.github.pastorgl.datacooker.dist.InvalidConfigurationException;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.DataHolder;
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
    protected AdapterMeta meta() {
        return new AdapterMeta("hadoop", "File-based output adapter that utilizes Hadoop FileSystems." +
                " Supports plain text, delimited text (CSV/TSV), and Parquet files, optionally compresse. Path examples:" +
                " file:/mnt/path/to/output, hdfs://output/into/parquet/files/.parquet, s3://bucket/and/key_prefix",

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
    public void save(String path, DataHolder rdd) {
        Function2<Integer, Iterator<BinRec>, Iterator<Void>> outputFunction = new PartOutputFunction(rdd.sub, path, codec, columns, delimiter.charAt(0));

        rdd.underlyingRdd.mapPartitionsWithIndex(outputFunction, true).count();
    }
}
