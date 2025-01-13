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
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.HadoopTextOutputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.OutputFunction;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;

@SuppressWarnings("unused")
public class HadoopTextOutput extends HadoopOutput {
    protected String[] columns;
    protected String delimiter;

    @Override
    public OutputAdapterMeta meta() {
        return new OutputAdapterMeta("hadoopText", "File-based output adapter that utilizes Hadoop FileSystems." +
                " Depending on DS type, outputs to plain or delimited text, optionally compressed",
                new String[]{"hdfs:///output/path", "file:/mnt/storage/path/to/output", "s3://bucket/and/key_prefix"},

                new StreamType[]{StreamType.PlainText, StreamType.Columnar},
                new DefinitionMetaBuilder()
                        .def(CODEC, "Codec to compress the output", Codec.class, Codec.NONE,
                                "By default, use no compression")
                        .def(COLUMNS, "Columns to write",
                                Object[].class, null, "By default, select all columns")
                        .def(DELIMITER, "Record column delimiter",
                                String.class, "\t", "By default, tabulation character")
                        .build()
        );
    }

    protected void configure(Configuration params) throws InvalidConfigurationException {
        super.configure(params);

        Object[] cols = params.get(COLUMNS);
        if (cols != null) {
            columns = Arrays.stream(cols).map(String::valueOf).toArray(String[]::new);
        }
        delimiter = params.get(DELIMITER);
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

        String _confXml = confXml;
        return new HadoopTextOutputFunction(sub, path, codec, confXml, columns, delimiter.charAt(0));
    }
}
