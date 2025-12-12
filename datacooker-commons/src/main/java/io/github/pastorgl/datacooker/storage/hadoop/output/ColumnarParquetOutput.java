/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.output;

import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.ColumnarParquetOutputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.OutputFunction;

import java.io.IOException;
import java.io.StringWriter;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.CODEC;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.Codec;

@SuppressWarnings("unused")
public class ColumnarParquetOutput extends HadoopOutput {
    static final String VERB = "columnarParquet";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "File-based output adapter that utilizes Hadoop FileSystems." +
                " Supports Parquet files, optionally compressed")
                .outputAdapter(new String[]{"hdfs:///output/into/parquet/files/", "file:/mnt/storage/output/for/parquet/", "s3://bucket/prefix/"})
                .objLvls(VALUE)
                .input(StreamType.COLUMNAR, "Columnar DS")
                .def(CODEC, "Codec to compress the output", Codec.class, Codec.NONE,
                        "By default, use no compression")
                .build();
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
