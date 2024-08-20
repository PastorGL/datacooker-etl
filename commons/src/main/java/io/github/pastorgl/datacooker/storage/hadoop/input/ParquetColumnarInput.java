/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input;

import io.github.pastorgl.datacooker.config.Configuration;
import io.github.pastorgl.datacooker.config.InvalidConfigurationException;
import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.InputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.input.functions.ParquetColumnarInputFunction;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.COLUMNS;

@SuppressWarnings("unused")
public class ParquetColumnarInput extends HadoopInput {
    protected String[] dsColumns;

    @Override
    public InputAdapterMeta meta() {
        return new InputAdapterMeta("parquetColumnar", "File-based input adapter that utilizes available Hadoop FileSystems." +
                " Supports Parquet files (non-splittable), optionally compressed",
                new String[]{"hdfs:///path/to/input/with/glob/**/*.snappy.parquet", "file:/mnt/data/{2020,2021,2022}/{01,02,03}/*.parquet"},

                StreamType.Columnar,
                new DefinitionMetaBuilder()
                        .def(SUB_DIRS, "If set, path will be treated as a prefix, and any first-level subdirectories underneath it" +
                                        " will be split to different streams", Boolean.class, false,
                                "By default, don't split")
                        .def(COLUMNS, "Columns to select from the built-in schema",
                                String[].class, null, "By default, don't select columns from the schema")
                        .build()
        );
    }

    @Override
    protected void configure(Configuration params) throws InvalidConfigurationException {
        super.configure(params);

        dsColumns = params.get(COLUMNS);
    }

    @Override
    protected DataStream callForFiles(String name, int partCount, List<List<String>> partNum, Partitioning partitioning) {
        InputFunction inputFunction = new ParquetColumnarInputFunction(dsColumns, context.hadoopConfiguration(), partitioning);
        JavaPairRDD<Object, DataRecord<?>> rdd = context.parallelize(partNum, partNum.size())
                .flatMapToPair(inputFunction.build())
                .repartition(partCount);

        List<String> attrs = Collections.emptyList();
        if (dsColumns != null) {
            attrs = Arrays.asList(dsColumns);
        }
        return new DataStreamBuilder(name, StreamType.Columnar, Collections.singletonMap(OBJLVL_VALUE, attrs))
                .created(meta.verb, path)
                .build(rdd);
    }
}
