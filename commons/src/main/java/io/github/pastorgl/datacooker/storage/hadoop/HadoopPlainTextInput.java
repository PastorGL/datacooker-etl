/**
 * Copyright (C) 2022 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop;

import io.github.pastorgl.datacooker.data.DataStream;
import io.github.pastorgl.datacooker.data.Record;
import io.github.pastorgl.datacooker.data.StreamType;
import io.github.pastorgl.datacooker.metadata.AdapterMeta;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.storage.hadoop.functions.InputFunction;
import io.github.pastorgl.datacooker.storage.hadoop.functions.PlainTextInputFunction;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.List;

import static io.github.pastorgl.datacooker.storage.hadoop.HadoopStorage.PART_COUNT;

@SuppressWarnings("unused")
public class HadoopPlainTextInput extends HadoopInput {
    @Override
    public AdapterMeta meta() {
        return new AdapterMeta("hadoopText", "File-based input adapter that utilizes available Hadoop FileSystems." +
                " Supports plain text files, optionally compressed",
                "Path examples: s3://bucket/path/to/data/group-000??",

                new StreamType[]{StreamType.PlainText},
                new DefinitionMetaBuilder()
                        .def(SUB_DIRS, "If set, any first-level subdirectories under designated path will" +
                                        " be split to different streams", Boolean.class, false,
                                "By default, don't split")
                        .def(PART_COUNT, "Desired number of parts",
                                Integer.class, 1, "By default, one part")
                        .build()
        );
    }

    @Override
    protected DataStream callForFiles(List<List<String>> partNum) {
        InputFunction inputFunction = new PlainTextInputFunction();
        JavaPairRDD<Object, Record<?>> rdd = context.parallelize(partNum, partNum.size())
                .flatMapToPair(inputFunction.build())
                .repartition(partCount);

        return new DataStream(rdd);
    }
}
