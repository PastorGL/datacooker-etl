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
import io.github.pastorgl.datacooker.storage.hadoop.functions.PlainTextInputFunction;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.List;

@SuppressWarnings("unused")
public class HadoopPlainTextInput extends HadoopInput {
    @Override
    public InputAdapterMeta meta() {
        return new InputAdapterMeta("hadoopText", "File-based input adapter that utilizes available Hadoop FileSystems." +
                " Supports plain text files, optionally compressed",
                new String[]{"file:/mnt/data/path/to/files/*.csv", "s3://bucket/path/to/data/group-000??", "hdfs:///source/path/**/*.tsv"},

                StreamType.PlainText,
                new DefinitionMetaBuilder()
                        .def(SUB_DIRS, "If set, any first-level subdirectories under designated path will" +
                                        " be split to different streams", Boolean.class, false,
                                "By default, don't split")
                        .build()
        );
    }

    @Override
    protected DataStream callForFiles(int partCount, List<List<String>> partNum, Partitioning partitioning) {
        InputFunction inputFunction = new PlainTextInputFunction(partitioning);
        JavaPairRDD<Object, Record<?>> rdd = context.parallelize(partNum, partNum.size())
                .flatMapToPair(inputFunction.build())
                .repartition(partCount);

        return new DataStream(rdd);
    }
}
