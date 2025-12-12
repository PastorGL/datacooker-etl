/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.output;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.HadoopTextOutputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.OutputFunction;

import java.io.IOException;
import java.io.StringWriter;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;

@SuppressWarnings("unused")
public class HadoopTextOutput extends HadoopOutput {
    static final String VERB = "hadoopText";
    protected String delimiter;

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "File-based output adapter that utilizes Hadoop FileSystems." +
                " Depending on DS type, outputs to plain or delimited text, optionally compressed")
                .outputAdapter(new String[]{"hdfs:///output/path", "file:/mnt/storage/path/to/output", "s3://bucket/and/key_prefix"})
                .objLvls(VALUE)
                .input(StreamType.of(StreamType.PlainText, StreamType.Columnar), "PlainText or Columnar DS")
                .def(CODEC, "Codec to compress the output", Codec.class, Codec.NONE,
                        "By default, use no compression")
                .def(DELIMITER, "Record column delimiter",
                        String.class, "\t", "By default, tabulation character")
                .build();
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        super.configure(params);

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

        return new HadoopTextOutputFunction(sub, path, codec, confXml, columns, delimiter.charAt(0));
    }
}
