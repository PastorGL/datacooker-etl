/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.s3direct;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.s3direct.functions.S3DirectTextOutputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.OutputFunction;

import java.io.IOException;
import java.io.StringWriter;

import static io.github.pastorgl.datacooker.s3direct.S3DirectStorage.*;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.CODEC;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.DELIMITER;

@SuppressWarnings("unused")
public abstract class S3DirectTextOutput extends S3DirectOutput {
    protected String delimiter;

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder("s3directText", "Multipart output adapter for any S3-compatible storage," +
                " based on Hadoop Delimited Text adapter.")
                .outputAdapter(new String[]{"s3d://bucket/prefix/to/output/csv/files/"})
                .input(StreamType.of(StreamType.PlainText, StreamType.Columnar), "PlainText or Columnar DS")
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
    protected OutputFunction getOutputFunction(String sub, String[] columns) {
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
