/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop;

import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Partitioning;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;
import io.github.pastorgl.datacooker.storage.hadoop.functions.InputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.functions.StructuredInputFunction;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Collections;
import java.util.List;

@SuppressWarnings("unused")
public class HadoopStructuredInput extends HadoopInput {
    @Override
    public InputAdapterMeta meta() {
        return new InputAdapterMeta("hadoopStructured", "File-based input adapter that utilizes available Hadoop FileSystems." +
                " Supports JSON fragment and Parquet files, optionally compressed",
                new String[]{"hdfs:///path/to/input/with/glob/**/*.json", "file:/mnt/data/{2020,2021,2022}/{01,02,03}/*.parquet"},

                StreamType.Structured,
                new DefinitionMetaBuilder()
                        .def(SUB_DIRS, "If set, any first-level subdirectories under designated path will" +
                                        " be split to different streams", Boolean.class, false,
                                "By default, don't split")
                        .build()
        );
    }

    @Override
    protected DataStream callForFiles(int partCount, List<List<String>> partNum, Partitioning partitioning) {
        InputFunction inputFunction = new StructuredInputFunction(partitioning);
        JavaPairRDD<Object, Record<?>> rdd = context.parallelize(partNum, partNum.size())
                .flatMapToPair(inputFunction.build())
                .repartition(partCount);

        return new DataStream(StreamType.Structured, rdd, Collections.emptyMap());
    }
}
