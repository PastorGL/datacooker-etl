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
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.ColumnarParquetOutputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.OutputFunction;

import java.io.IOException;
import java.io.StringWriter;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;

@SuppressWarnings("unused")
public class ColumnarParquetOutput extends HadoopOutput {
    protected Codec codec;

    @Override
    public PluggableMeta initMeta() {
        return new PluggableMetaBuilder("columnarParquet", "File-based output adapter that utilizes Hadoop FileSystems." +
                " Supports Parquet files, optionally compressed")
                .outputAdapter(new String[]{"hdfs:///output/into/parquet/files/", "file:/mnt/storage/output/for/parquet/", "s3://bucket/prefix/"})
                .objLvls(VALUE)
                .input(StreamType.COLUMNAR, "Columnar DS")
                .def(CODEC, "Codec to compress the output", Codec.class, Codec.NONE,
                        "By default, use no compression")
                .build();
    }

    protected void configure(Configuration params) throws InvalidConfigurationException {
        super.configure(params);
    }

    @Override
    protected OutputFunction getOutputFunction(String sub, String[] columns) {
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
