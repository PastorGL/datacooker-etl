/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.s3direct;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.InputFunction;
import io.github.pastorgl.datacooker.storage.s3direct.functions.S3DirectColumnarInputFunction;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.COLUMNS;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.DELIMITER;
import static io.github.pastorgl.datacooker.storage.hadoop.input.TextColumnarInput.SCHEMA_DEFAULT;
import static io.github.pastorgl.datacooker.storage.hadoop.input.TextColumnarInput.SCHEMA_FROM_FILE;
import static io.github.pastorgl.datacooker.storage.s3direct.S3DirectStorage.*;

@SuppressWarnings("unused")
public class S3DirectColumnarInput extends S3DirectInput {
    protected String[] schemaDefault;
    protected boolean schemaFromFile;
    protected String[] dsColumns;
    protected String dsDelimiter;

    @Override
    public InputAdapterMeta meta() {
        return new InputAdapterMeta("s3directColumnar", "Input adapter for any S3-compatible storage," +
                " based on Hadoop Columnar adapter. File objects are non-splittable. Supports delimited text and Parquet files",
                new String[]{"s3d://bucket/key/prefix/"},

                StreamType.Columnar,
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
                        .def(SCHEMA_FROM_FILE, "Read schema from 1st line of delimited text file." +
                                        " Ignored for Parquet",
                                Boolean.class, true, "By default, try to get schema from file")
                        .def(SCHEMA_DEFAULT, "Loose schema for delimited text (just column names," +
                                        " optionally with placeholders to skip some, denoted by underscores _)." +
                                        " Required if " + SCHEMA_FROM_FILE + " is set to false",
                                Object[].class, null, "By default, don't set the schema")
                        .def(DELIMITER, "Column delimiter for delimited text",
                                String.class, "\t", "By default, tabulation character")
                        .def(COLUMNS, "Columns to select from the schema",
                                Object[].class, null, "By default, don't select columns from the schema")
                        .build()
        );
    }

    @Override
    protected void configure(Configuration params) throws InvalidConfigurationException {
        super.configure(params);

        dsDelimiter = params.get(DELIMITER);

        schemaFromFile = params.get(SCHEMA_FROM_FILE);
        if (!schemaFromFile) {
            Object[] schDef = params.get(SCHEMA_DEFAULT);

            if (schDef == null) {
                throw new InvalidConfigurationException("Neither '" + SCHEMA_FROM_FILE + "' is true nor '"
                        + SCHEMA_DEFAULT + "' is specified for Input Adapter '" + meta.verb + "'");
            } else {
                schemaDefault = Arrays.stream(schDef).map(String::valueOf).toArray(String[]::new);
            }
        }

        Object[] cols = params.get(COLUMNS);
        if (cols != null) {
            dsColumns = Arrays.stream(cols).map(String::valueOf).toArray(String[]::new);
        }
    }

    @Override
    protected DataStream callForFiles(String name, int partCount, List<List<String>> partNum, Partitioning partitioning) {
        InputFunction inputFunction = new S3DirectColumnarInputFunction(schemaFromFile, schemaDefault, dsColumns, dsDelimiter.charAt(0),
                endpoint, region, accessKey, secretKey, bucket, tmpDir, context.hadoopConfiguration(), partitioning);
        JavaPairRDD<Object, DataRecord<?>> rdd = context.parallelize(partNum, partNum.size())
                .flatMapToPair(inputFunction.build())
                .repartition(partCount);

        return new DataStreamBuilder(name, Collections.emptyMap())
                .created(meta.verb, path, StreamType.Columnar, partitioning.name())
                .build(rdd);
    }
}
