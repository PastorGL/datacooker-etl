/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop;

import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;
import io.github.pastorgl.datacooker.storage.hadoop.functions.ColumnarInputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.functions.InputFunction;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.COLUMNS;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.DELIMITER;

@SuppressWarnings("unused")
public class HadoopColumnarInput extends HadoopInput {
    public static final String SCHEMA_DEFAULT = "schema_default";
    public static final String SCHEMA_FROM_FILE = "schema_from_file";

    protected String[] schemaDefault;
    protected boolean schemaFromFile;
    protected String[] dsColumns;
    protected String dsDelimiter;

    @Override
    public InputAdapterMeta meta() {
        return new InputAdapterMeta("hadoopColumnar", "File-based input adapter that utilizes available Hadoop FileSystems." +
                " Supports delimited text (CSV/TSV) and Parquet files, optionally compressed",
                "Path examples: hdfs:///path/to/input/with/glob/**/*.tsv," +
                        " file:/mnt/data/{2020,2021,2022}/{01,02,03}/*.parquet",

                StreamType.Columnar,
                new DefinitionMetaBuilder()
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
        InputFunction inputFunction = new ColumnarInputFunction(schemaFromFile, schemaDefault, dsColumns, dsDelimiter.charAt(0), partitioning);
        JavaPairRDD<Object, Record<?>> rdd = context.parallelize(partNum, partNum.size())
                .flatMapToPair(inputFunction.build())
                .repartition(partCount);

        List<String> attrs = Collections.emptyList();
        if (dsColumns != null) {
            attrs = Arrays.asList(dsColumns);
        } else if (schemaDefault != null) {
            attrs = Arrays.asList(schemaDefault);
        }
        return new DataStream(StreamType.Columnar, rdd, Collections.singletonMap(OBJLVL_VALUE, attrs));
    }

}
