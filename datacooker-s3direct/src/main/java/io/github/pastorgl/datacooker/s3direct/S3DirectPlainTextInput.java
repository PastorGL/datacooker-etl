/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.s3direct;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.InputFunction;
import io.github.pastorgl.datacooker.s3direct.functions.S3DirectTextInputFunction;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.s3direct.S3DirectStorage.*;

@SuppressWarnings("unused")
public class S3DirectPlainTextInput extends S3DirectInput {
    @Override
    public InputAdapterMeta meta() {
        return new InputAdapterMeta("s3directText", "Input adapter for any S3-compatible storage," +
                " based on Hadoop PlainText adapter",
                new String[]{"s3d://bucket/path/to/data/"},

                StreamType.PLAIN_TEXT,
                new DefinitionMetaBuilder()
                        .def(S3D_ACCESS_KEY, "S3 access key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3D_SECRET_KEY, "S3 secret key", null, "By default, try to discover" +
                                " the key from client's standard credentials chain")
                        .def(S3D_ENDPOINT, "S3 endpoint", null, "By default, try to discover" +
                                " the endpoint from client's standard profile")
                        .def(S3D_REGION, "S3 region", null, "By default, try to discover" +
                                " the region from client's standard profile")
                        .def(SUB_DIRS, "If set, any first-level 'subdirectories' under designated prefix will" +
                                        " be split to different streams", Boolean.class, false,
                                "By default, don't split")
                        .build()
        );
    }

    @Override
    protected DataStream callForFiles(String name, int partCount, List<List<String>> partNum, Partitioning partitioning) {
        InputFunction inputFunction = new S3DirectTextInputFunction(endpoint, region, accessKey, secretKey, bucket,
                context.hadoopConfiguration(), partitioning);
        JavaPairRDD<Object, DataRecord<?>> rdd = context.parallelize(partNum, partNum.size())
                .flatMapToPair(inputFunction.build())
                .repartition(partCount);

        return new DataStreamBuilder(name, Collections.emptyMap())
                .created(meta.verb, path, StreamType.PlainText, partitioning.name())
                .build(rdd);
    }
}
