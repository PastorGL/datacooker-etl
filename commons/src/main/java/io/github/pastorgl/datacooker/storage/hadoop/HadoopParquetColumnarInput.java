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
import io.github.pastorgl.datacooker.storage.hadoop.functions.InputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.functions.ParquetColumnarInputFunction;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.github.pastorgl.datacooker.Constants.OBJLVL_VALUE;
import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.COLUMNS;

@SuppressWarnings("unused")
public class HadoopParquetColumnarInput extends HadoopInput {
    protected String[] dsColumns;

    @Override
    public InputAdapterMeta meta() {
        return new InputAdapterMeta("hadoopParquetColumnar", "File-based input adapter that utilizes available Hadoop FileSystems." +
                " Supports Parquet files, optionally compressed",
                new String[]{"hdfs:///path/to/input/with/glob/**/*.snappy.parquet", "file:/mnt/data/{2020,2021,2022}/{01,02,03}/*.parquet"},

                StreamType.Columnar,
                new DefinitionMetaBuilder()
                        .def(SUB_DIRS, "If set, any first-level subdirectories under designated path will" +
                                        " be split to different streams", Boolean.class, false,
                                "By default, don't split")
                        .def(COLUMNS, "Columns to select from the built-in schema",
                                String[].class, null, "By default, don't select columns from the schema")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigurationException {
        super.configure();

        dsColumns = resolver.get(COLUMNS);
    }

    @Override
    protected DataStream callForFiles(int partCount, List<List<String>> partNum, Partitioning partitioning) {
        InputFunction inputFunction = new ParquetColumnarInputFunction(dsColumns, partitioning);
        JavaPairRDD<Object, Record<?>> rdd = context.parallelize(partNum, partNum.size())
                .flatMapToPair(inputFunction.build())
                .repartition(partCount);

        List<String> attrs = Collections.emptyList();
        if (dsColumns != null) {
            attrs = Arrays.asList(dsColumns);
        }
        return new DataStream(StreamType.Columnar, rdd, Collections.singletonMap(OBJLVL_VALUE, attrs));
    }
}
