/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.s3direct;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OutputAdapterMeta;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.OutputFunction;
import io.github.pastorgl.datacooker.s3direct.functions.S3DirectTextOutputFunction;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;
import static io.github.pastorgl.datacooker.s3direct.S3DirectStorage.*;

@SuppressWarnings("unused")
public abstract class S3DirectTextOutput extends S3DirectOutput {
    protected String[] columns;
    protected String delimiter;

    @Override
    public OutputAdapterMeta meta() {
        return new OutputAdapterMeta("s3directText", "Multipart output adapter for any S3-compatible storage," +
                " based on Hadoop Delimited Text adapter.",
                new String[]{"s3d://bucket/prefix/to/output/csv/files/"},

                StreamType.of(StreamType.PlainText, StreamType.Columnar),
                new DefinitionMetaBuilder()
                        .def(CODEC, "Codec to compress the output", HadoopStorage.Codec.class, HadoopStorage.Codec.NONE,
                                "By default, use no compression")
                        .def(S3D_ACCESS_KEY, "S3 access key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3D_SECRET_KEY, "S3 secret key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3D_ENDPOINT, "S3 endpoint", null, "By default, try to discover" +
                                " the endpoint from client's standard profile")
                        .def(S3D_REGION, "S3 region", null, "By default, try to discover" +
                                " the region from client's standard profile")
                        .def(CONTENT_TYPE, "Content type for objects", "text/csv", "By default," +
                                " content type is CSV")
                        .def(COLUMNS, "Columns to write",
                                Object[].class, null, "By default, select all columns")
                        .def(DELIMITER, "Record column delimiter",
                                String.class, "\t", "By default, tabulation character")
                        .build()
        );
    }

    @Override
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

        return new S3DirectTextOutputFunction(sub, path, codec, confXml,
                columns, delimiter.charAt(0), endpoint, region, accessKey, secretKey, contentType);
    }
}
