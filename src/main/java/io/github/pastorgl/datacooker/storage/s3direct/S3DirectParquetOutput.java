/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.s3direct;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.OutputAdapterMeta;
import io.github.pastorgl.datacooker.storage.hadoop.output.functions.OutputFunction;
import io.github.pastorgl.datacooker.storage.s3direct.functions.S3DirectParquetOutputFunction;

import java.io.IOException;
import java.io.StringWriter;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;
import static io.github.pastorgl.datacooker.storage.s3direct.S3DirectStorage.*;

@SuppressWarnings("unused")
public abstract class S3DirectParquetOutput extends S3DirectOutput {
    private String[] columns;

    @Override
    public OutputAdapterMeta meta() {
        return new OutputAdapterMeta("s3directParquet", "Multipart output adapter for any S3-compatible storage," +
                " based on Hadoop Parquet output adapter.",
                new String[]{"s3d://bucket/prefix/to/output/parquet/files/"},

                new StreamType[]{StreamType.Columnar},
                new DefinitionMetaBuilder()
                        .def(CODEC, "Codec to compress the output", Codec.class, Codec.NONE,
                                "By default, use no compression")
                        .def(S3D_ACCESS_KEY, "S3 access key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3D_SECRET_KEY, "S3 secret key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3D_ENDPOINT, "S3 endpoint", null, "By default, try to discover" +
                                " the endpoint from client's standard profile")
                        .def(S3D_REGION, "S3 region", null, "By default, try to discover" +
                                " the region from client's standard profile")
                        .def(CONTENT_TYPE, "Content type for objects", "application/vnd.apache.parquet",
                                "By default, content type is application/vnd.apache.parquet")
                        .def(COLUMNS, "Columns to write",
                                String[].class, null, "By default, select all columns")
                        .build()
        );
    }

    @Override
    protected void configure(Configuration params) throws InvalidConfigurationException {
        super.configure(params);

        columns = params.get(COLUMNS);
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

        return new S3DirectParquetOutputFunction(sub, path, codec, confXml, columns,
                endpoint, region, accessKey, secretKey, tmpDir, contentType);
    }
}
