/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.s3direct;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.InputFunction;
import io.github.pastorgl.datacooker.storage.s3direct.functions.S3DirectColumnarInputFunction;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.*;
import static io.github.pastorgl.datacooker.storage.hadoop.input.TextColumnarInput.*;
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
                " based on Hadoop Columnar adapter. File objects are non-splittable",
                new String[]{"s3d://bucket/key/prefix/glob/pattern/{2020,2021}/{01,02}/*.tsv",
                        "s3d://bucket/key/prefix/all/Parquet/files/*.parquet"},

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
                        .def(SUB_DIRS, "If set, any first-level subdirectories under designated path will" +
                                        " be split to different streams", Boolean.class, false,
                                "By default, don't split")
                        .def(SCHEMA_FROM_FILE, "Read schema from 1st line of delimited text file." +
                                        " Ignored for Parquet",
                                Boolean.class, true, "By default, try to get schema from file")
                        .def(SCHEMA_DEFAULT, "Loose schema for delimited text (just column names," +
                                        " optionally with placeholders to skip some, denoted by underscores _)." +
                                        " Required if " + SCHEMA_FROM_FILE + " is set to false",
                                String[].class, null, "By default, don't set the schema")
                        .def(DELIMITER, "Column delimiter for delimited text",
                                String.class, "\t", "By default, tabulation character")
                        .def(COLUMNS, "Columns to select from the schema",
                                String[].class, null, "By default, don't select columns from the schema")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigurationException {
        super.configure();

        dsDelimiter = resolver.get(DELIMITER);

        schemaFromFile = resolver.get(SCHEMA_FROM_FILE);
        if (!schemaFromFile) {
            schemaDefault = resolver.get(SCHEMA_DEFAULT);

            if (schemaDefault == null) {
                throw new InvalidConfigurationException("Neither '" + SCHEMA_FROM_FILE + "' is true nor '"
                        + SCHEMA_DEFAULT + "' is specified for Input Adater '" + meta.verb + "'");
            }
        }

        dsColumns = resolver.get(COLUMNS);
    }

    @Override
    protected DataStream callForFiles(int partCount, List<List<String>> partNum, Partitioning partitioning) {
        InputFunction inputFunction = new S3DirectColumnarInputFunction(schemaFromFile, schemaDefault, dsColumns, dsDelimiter.charAt(0),
                endpoint, region, accessKey, secretKey, bucket, tmpDir, partitioning);
        JavaPairRDD<Object, Record<?>> rdd = context.parallelize(partNum, partNum.size())
                .flatMapToPair(inputFunction.build())
                .repartition(partCount);

        return new DataStream(StreamType.Columnar, rdd, Collections.emptyMap());
    }
}
