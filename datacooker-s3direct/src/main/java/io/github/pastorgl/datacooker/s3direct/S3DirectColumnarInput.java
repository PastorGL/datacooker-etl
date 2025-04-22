/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.s3direct;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import io.github.pastorgl.datacooker.s3direct.functions.S3DirectColumnarInputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.InputFunction;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.data.ObjLvl.VALUE;
import static io.github.pastorgl.datacooker.s3direct.S3DirectStorage.*;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.DELIMITER;
import static io.github.pastorgl.datacooker.storage.hadoop.input.TextColumnarInput.SCHEMA_FROM_FILE;

@SuppressWarnings("unused")
public class S3DirectColumnarInput extends S3DirectInput {
    static final String VERB = "s3directColumnar";
    protected String[] schemaDefault;
    protected boolean schemaFromFile;
    protected String dsDelimiter;

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "Input adapter for any S3-compatible storage," +
                " based on Hadoop Columnar adapter. File objects are non-splittable. Supports delimited text and Parquet files")
                .inputAdapter(new String[]{"s3d://bucket/key/prefix/"})
                .output(StreamType.COLUMNAR, "Columnar DS")
                .objLvls(VALUE)
                .def(S3D_ACCESS_KEY, "S3 access key", null, "By default, try to discover" +
                        " the key from client's standard credentials chain")
                .def(S3D_SECRET_KEY, "S3 secret key", null, "By default, try to discover" +
                        " the key from client's standard credentials chain")
                .def(S3D_ENDPOINT, "S3 endpoint", null, "By default, try to discover" +
                        " the endpoint from client's standard profile")
                .def(S3D_REGION, "S3 region", null, "By default, try to discover" +
                        " the region from client's standard profile")
                .def(SCHEMA_FROM_FILE, "Read schema from 1st line of delimited text file." +
                                " Ignored for Parquet",
                        Boolean.class, true, "By default, try to get schema from file")
                .def(DELIMITER, "Column delimiter for delimited text",
                        String.class, "\t", "By default, tabulation character")
                .build();
    }

    @Override
    public void configure(Configuration params) throws InvalidConfigurationException {
        super.configure(params);

        dsDelimiter = params.get(DELIMITER);

        schemaFromFile = params.get(SCHEMA_FROM_FILE);
    }

    @Override
    protected DataStream callForFiles(String name, List<List<String>> partNum) {
        String[] dsColumns = (requestedColumns.get(VALUE) == null) ? null : requestedColumns.get(VALUE).toArray(new String[0]);

        InputFunction inputFunction = new S3DirectColumnarInputFunction(schemaFromFile, dsColumns, dsDelimiter.charAt(0),
                endpoint, region, accessKey, secretKey, bucket, tmpDir, context.hadoopConfiguration(), partitioning);
        JavaPairRDD<Object, DataRecord<?>> rdd = context.parallelize(partNum, partNum.size())
                .flatMapToPair(inputFunction.build())
                .repartition(partCount);

        return new DataStreamBuilder(name, Collections.emptyMap())
                .created(VERB, path, StreamType.Columnar, partitioning.name())
                .build(rdd);
    }
}
