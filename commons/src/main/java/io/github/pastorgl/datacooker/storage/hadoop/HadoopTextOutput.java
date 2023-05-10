/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OutputAdapterMeta;
import io.github.pastorgl.datacooker.storage.hadoop.functions.PartOutputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.functions.TextOutputFunction;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;

@SuppressWarnings("unused")
public class HadoopTextOutput extends HadoopOutput {
    protected String[] columns;
    protected String delimiter;

    @Override
    public OutputAdapterMeta meta() {
        return new OutputAdapterMeta("hadoopText", "File-based output adapter that utilizes Hadoop FileSystems." +
                " Supports plain and delimited text (CSV/TSV), optionally compressed",
                "Path examples: file:/mnt/path/to/output," +
                        " s3://bucket/and/key_prefix",

                new StreamType[]{StreamType.PlainText, StreamType.Columnar},
                new DefinitionMetaBuilder()
                        .def(CODEC, "Codec to compress the output", Codec.class, Codec.NONE,
                                "By default, use no compression")
                        .def(COLUMNS, "Columns to write",
                                String[].class, null, "By default, select all columns")
                        .def(DELIMITER, "Record column delimiter",
                                String.class, "\t", "By default, tabulation character")
                        .build()
        );
    }

    protected void configure() throws InvalidConfigurationException {
        super.configure();

        columns = resolver.get(COLUMNS);
        delimiter = resolver.get(DELIMITER);
    }

    @Override
    protected PartOutputFunction getOutputFunction(String sub) {
        return new TextOutputFunction(sub, path, codec, columns, delimiter.charAt(0));
    }
}
