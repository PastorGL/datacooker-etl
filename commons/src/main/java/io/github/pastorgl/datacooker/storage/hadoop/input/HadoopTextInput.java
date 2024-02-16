/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.DefinitionMetaBuilder;
import io.github.pastorgl.datacooker.metadata.InputAdapterMeta;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class HadoopTextInput extends HadoopInput {
    @Override
    public InputAdapterMeta meta() {
        return new InputAdapterMeta("hadoopText", "File-based input adapter that utilizes available Hadoop FileSystems." +
                " Supports plain text files (splittable), optionally compressed",
                new String[]{"file:/mnt/data/path/to/files/*.gz", "s3://bucket/path/to/data/group-000??.jsonf", "hdfs:///source/path/**/*.tsv"},

                StreamType.PlainText,
                new DefinitionMetaBuilder()
                        .def(SUB_DIRS, "If set, path will be treated as a prefix, and any first-level subdirectories underneath it" +
                                        " will be split to different streams", Boolean.class, false,
                                "By default, don't split")
                        .build()
        );
    }

    @Override
    protected DataStream callForFiles(String name, int partCount, List<List<String>> partNum, final Partitioning partitioning) {
        JavaPairRDD<Object, Record<?>> rdd = context.textFile(partNum.stream().map(l -> String.join(",", l)).collect(Collectors.joining(",")), partCount)
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Object, Record<?>>> ret = new ArrayList<>();

                    Random random = new Random();
                    while (it.hasNext()) {
                        PlainText rec = new PlainText(it.next());
                        Object key = (partitioning == Partitioning.RANDOM) ? random.nextInt() : rec.hashCode();
                        ret.add(new Tuple2<>(key, rec));
                    }

                    return ret.iterator();
                });

        if (partitioning != Partitioning.SOURCE) {
            rdd = rdd.repartition(partCount);
        }

        return new DataStreamBuilder(name, StreamType.PlainText, null).created(meta.verb, path).build(rdd);
    }
}
