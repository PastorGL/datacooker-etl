/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.s3direct;

import io.github.pastorgl.datacooker.data.DataRecord;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.DataStreamBuilder;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.s3direct.functions.S3DirectTextInputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.InputFunction;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.s3direct.S3DirectStorage.*;

@SuppressWarnings("unused")
public class S3DirectPlainTextInput extends S3DirectInput {
    static final String VERB = "s3directText";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Input adapter for any S3-compatible storage," +
                " based on Hadoop PlainText adapter")
                .inputAdapter(new String[]{"s3d://bucket/path/to/data/"})
                .output(StreamType.PLAIN_TEXT, "PlainText DS")
                .def(S3D_ACCESS_KEY, "S3 access key", null, "By default, try to discover" +
                        " the key from client's standard credentials chain")
                .def(S3D_SECRET_KEY, "S3 secret key", null, "By default, try to discover" +
                        " the key from client's standard credentials chain")
                .def(S3D_ENDPOINT, "S3 endpoint", null, "By default, try to discover" +
                        " the endpoint from client's standard profile")
                .def(S3D_REGION, "S3 region", null, "By default, try to discover" +
                        " the region from client's standard profile")
                .build();
    }

    @Override
    protected DataStream callForFiles(String name, List<List<String>> partNum) {
        InputFunction inputFunction = new S3DirectTextInputFunction(endpoint, region, accessKey, secretKey, bucket,
                context.hadoopConfiguration(), partitioning);
        JavaPairRDD<Object, DataRecord<?>> rdd = context.parallelize(partNum, partNum.size())
                .flatMapToPair(inputFunction.build())
                .repartition(partCount);

        return new DataStreamBuilder(name, Collections.emptyMap())
                .created(VERB, path, StreamType.PlainText, partitioning.name())
                .build(rdd);
    }
}
