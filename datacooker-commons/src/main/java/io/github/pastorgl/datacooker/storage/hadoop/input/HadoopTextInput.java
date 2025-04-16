/**
 * Copyright (C) 2023 Data Cooker Team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package io.github.pastorgl.datacooker.storage.hadoop.input;

import io.github.pastorgl.datacooker.data.*;
import io.github.pastorgl.datacooker.metadata.PluggableMeta;
import io.github.pastorgl.datacooker.metadata.PluggableMetaBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class HadoopTextInput extends HadoopInput {
    static final String VERB = "hadoopText";

    @Override
    public PluggableMeta meta() {
        return new PluggableMetaBuilder(VERB, "File-based input adapter that utilizes available Hadoop FileSystems." +
                " Supports plain text files (splittable), optionally compressed")
                .inputAdapter(new String[]{"file:/mnt/data/path/to/files/*.gz", "s3://bucket/path/to/data/group-000??.jsonf", "hdfs:///source/path/**/*.tsv"}, true)
                .output(StreamType.PLAIN_TEXT, "PlainText DS")
                .build();
    }

    @Override
    protected DataStream callForFiles(String name, List<List<String>> partNum) {
        final String _source = partNum.stream().map(l -> String.join(",", l)).collect(Collectors.joining(","));

        final Partitioning _partitioning = partitioning;
        JavaPairRDD<Object, DataRecord<?>> rdd = context.textFile(_source, partCount)
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Object, DataRecord<?>>> ret = new ArrayList<>();

                    Random random = new Random();
                    while (it.hasNext()) {
                        PlainText rec = new PlainText(it.next());
                        Object key = switch (_partitioning) {
                            case HASHCODE -> rec.hashCode();
                            case RANDOM -> random.nextInt();
                            case SOURCE -> _source.hashCode();
                        };
                        ret.add(new Tuple2<>(key, rec));
                    }

                    return ret.iterator();
                });

        if (partitioning != Partitioning.SOURCE) {
            rdd = rdd.repartition(partCount);
        }

        return new DataStreamBuilder(name, null).created(VERB, path, StreamType.PlainText, partitioning.toString()).build(rdd);
    }
}
