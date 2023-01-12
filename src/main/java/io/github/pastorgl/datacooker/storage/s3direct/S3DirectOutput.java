/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.s3direct;

import io.github.pastorgl.datacooker.data.BinRec;
import io.github.pastorgl.datacooker.dist.InvalidConfigurationException;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.DataHolder;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopOutput;
import io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage;
import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;

@SuppressWarnings("unused")
public class S3DirectOutput extends HadoopOutput {
    private String accessKey;
    private String secretKey;

    private String contentType;
    private String endpoint;
    private String region;
    private String tmpDir;

    @Override
    protected AdapterMeta meta() {
        return new AdapterMeta("s3direct", "Multipart output adapter for any S3-compatible storage, based on Hadoop adapter." +
                " Path example: s3d://bucket/prefix/to/output/parquet/files/.parquet",

                new DefinitionMetaBuilder()
                        .def(CODEC, "Codec to compress the output", HadoopStorage.Codec.class, HadoopStorage.Codec.NONE,
                                "By default, use no compression")
                        .def(S3DirectStorage.S3D_ACCESS_KEY, "S3 access key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3DirectStorage.S3D_SECRET_KEY, "S3 secret key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3DirectStorage.S3D_ENDPOINT, "S3 endpoint", null, "By default, try to discover" +
                                " the endpoint from client's standard profile")
                        .def(S3DirectStorage.S3D_REGION, "S3 region", null, "By default, try to discover" +
                                " the region from client's standard profile")
                        .def(S3DirectStorage.CONTENT_TYPE, "Content type for objects", "text/csv", "By default," +
                                " content type is CSV")
                        .def(COLUMNS, "Columns to write",
                                String[].class, null, "By default, select all columns")
                        .def(DELIMITER, "Record column delimiter",
                                String.class, "\t", "By default, tabulation character")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigurationException {
        super.configure();

        accessKey = resolver.get(S3DirectStorage.S3D_ACCESS_KEY);
        secretKey = resolver.get(S3DirectStorage.S3D_SECRET_KEY);
        endpoint = resolver.get(S3DirectStorage.S3D_ENDPOINT);
        region = resolver.get(S3DirectStorage.S3D_REGION);

        contentType = resolver.get(S3DirectStorage.CONTENT_TYPE);

        tmpDir = resolver.get("tmp");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void save(String path, DataHolder rdd) {
        Function2<Integer, Iterator<BinRec>, Iterator<Void>> outputFunction = new S3DirectPartOutputFunction(rdd.sub, path, codec, columns, delimiter.charAt(0),
                endpoint, region, accessKey, secretKey, tmpDir, contentType);

        rdd.underlyingRdd.mapPartitionsWithIndex(outputFunction, true).count();
    }
}
